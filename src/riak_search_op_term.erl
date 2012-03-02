%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_op_term).
-export([
         preplan/2,
         chain_op/4,
         chain_op/5,
         default_filter/2,
         frequency/1
        ]).

-import(riak_search_utils, [to_binary/1]).
-record(scoring_vars, {term_boost, doc_frequency, num_docs}).

%% Look up results from the index without any kind of text analyzis on
%% term. Filter and transform the results, and send them to the
%% OutputPid.

-include("riak_search.hrl").
-include_lib("lucene_parser/include/lucene_parser.hrl").

%% Need term count for node planning. Used in #intersection and
%% #union. Calculate this during preplan based on where the most
%% results come from.
preplan(Op, State) ->
    %% Get info about the term, return in props...
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,
    Term = to_binary(Op#term.s),
    Weights1 = info(IndexName, FieldName, Term),
    lager:error("term preplan Weights1: ~p", [Weights1]),

    %% TODO: Add ability for search nodes to negotiate protocol so
    %% that in future I can change info to not return Term which is
    %% just filtered out
    Weights2 = [{Node, Count} || {_, Node, Count} <- Weights1],
    lager:error("term preplan Weights2: ~p", [Weights2]),
    Length = length(Weights2),

    if Length == 0 ->
            lager:error("Couldn't perform preplan for op ~p with state ~p",
                        [Op, State]),
            throw({error, data_not_available, {IndexName, FieldName, Term}});
       true ->
            TotalCount = lists:sum([Count || {_, Count} <- Weights2]),
            DocFrequency = TotalCount / length(Weights1),
            Op#term{weights=Weights2, doc_freq=DocFrequency}
    end.

chain_op(Op, OutputPid, OutputRef, State) ->
    F = fun() ->
                erlang:link(State#search_state.parent),
                start_loop(Op, OutputPid, OutputRef, State, none)
        end,
    erlang:spawn_link(F),
    {ok, 1}.

chain_op(Op, OutputPid, OutputRef, State, CandidateSet) ->
    F = fun() ->
                erlang:link(State#search_state.parent),
                start_loop(Op, OutputPid, OutputRef, State, CandidateSet)
        end,
    erlang:spawn_link(F),
    {ok, 1}.

-spec frequency(term()) -> {Freq::integer(), Op::term()}.
frequency(Op=#term{doc_freq=Freq}) -> {Freq, Op}.

-spec start_loop(any(), pid(), reference(), #search_state{}, list()) -> any().
start_loop(Op, OutputPid, OutputRef, State, CandidateSet) ->
    %% Get the current index/field...
    IndexName = State#search_state.index,
    FieldName = State#search_state.field,
    Term = to_binary(Op#term.s),

    %% TODO: IIRC, the #search_state.filter fun is always a fun that
    %% lets everything through
    FilterFun = case CandidateSet of
                    none -> State#search_state.filter;
                    _ ->
                        State#search_state.filter
                        %% filter_candidates(CandidateSet)
                end,
    {ok, Ref} = stream(IndexName, FieldName, Term, FilterFun, CandidateSet),

    %% Collect the results...
    TransformFun = generate_transform_function(Op, State),
    riak_search_op_utils:gather_stream_results(Ref, OutputPid, OutputRef, TransformFun).

%% TODO: I need to push the CandidateSet all the way down to the index
%% since I need to do a binary search of each candidate
-spec stream(index(), field(), s_term(), fun(), term()) -> {ok, stream_ref()}.
stream(Index, Field, Term, FilterFun, CandidateSet) ->
    %% Get the primary preflist, minus any down nodes. (We don't use
    %% secondary nodes since we ultimately read results from one node
    %% anyway.)
    DocIdx = riak_search_ring_utils:calc_partition(Index, Field, Term),
    {ok, Schema} = riak_search_config:get_schema(Index),
    NVal = Schema:n_val(),
    Preflist = get_preflist(DocIdx, NVal),

    %% Try to use the local node if possible. Otherwise choose
    %% randomly.
    case lists:keyfind(node(), 2, Preflist) of
        false ->
            PreflistEntry = riak_search_utils:choose(Preflist);
        PreflistEntry ->
            PreflistEntry = PreflistEntry
    end,

    %% TODO: FIXME: Changing the arguments passed here is going to
    %% cause issues with rolling upgrade...if rolling upgrade is going
    %% to work need to negotiate versions and only use new boolean
    %% queries if possible.
    riak_search_vnode:stream([PreflistEntry], Index, Field, Term,
                             {FilterFun, CandidateSet},
                             self()).

default_filter(_, _) -> true.

info(Index, Field, Term) ->
    %% Get the primary preflist, minus any down nodes. (We don't use
    %% secondary nodes since we ultimately read results from one node
    %% anyway.)
    DocIdx = riak_search_ring_utils:calc_partition(Index, Field, Term),
    {ok, Schema} = riak_search_config:get_schema(Index),
    NVal = Schema:n_val(),
    Preflist = get_preflist(DocIdx, NVal),

    {ok, Ref} = riak_search_vnode:info(Preflist, Index, Field, Term, self()),
    %% Results :: [{Term, Node, Freq}]
    {ok, Results} = riak_search_backend:collect_info_response(length(Preflist), Ref, []),
    Results.

%% Create transform function, taking scoring values into account.
generate_transform_function(Op, State) ->
    %% Create the scoring vars record...
    ScoringVars = #scoring_vars {
        term_boost = Op#term.boost,
        doc_frequency = Op#term.doc_freq,
        num_docs = State#search_state.num_docs
    },

    %% Transform the result by adding the Index and calculating the
    %% Score.
    IndexName = State#search_state.index,
    fun({DocID, Props}) ->
        NewProps = calculate_score(ScoringVars, Props),
        {IndexName, DocID, NewProps}
    end.

calculate_score(ScoringVars, Props) ->
    %% Pull from ScoringVars...
    TermBoost = ScoringVars#scoring_vars.term_boost,
    DocFrequency = ScoringVars#scoring_vars.doc_frequency + 1,
    NumDocs = ScoringVars#scoring_vars.num_docs + 1,

    %% Pull freq from Props. (If no exist, use 1).
    Frequency = length(proplists:get_value(p, Props, [])),
    DocFieldBoost = proplists:get_value(boost, Props, 1),

    %% Calculate the score for this term, based roughly on Lucene
    %% scoring. http://lucene.apache.org/java/2_4_0/api/org/apache/lucene/search/Similarity.html
    TF = math:pow(Frequency, 0.5),
    IDF = (1 + math:log(NumDocs/DocFrequency)),
    Norm = DocFieldBoost,

    Score = TF * math:pow(IDF, 2) * TermBoost * Norm,
    ScoreList = case lists:keyfind(score, 1, Props) of
                    {score, OldScores} ->
                        [Score|OldScores];
                    false ->
                        [Score]
                end,

    %% TODO: FIXME: Removing score from prop list just to test some
    %% stuff

    %% lists:keystore(score, 1, Props, {score, ScoreList}).
    Props.

-spec get_preflist(binary(), pos_integer()) -> list().
get_preflist(DocIdx, NVal) ->
    lists:map(fun({IdxNode, _}) -> IdxNode end,
              riak_core_apl:get_primary_apl(DocIdx, NVal, riak_search)).
