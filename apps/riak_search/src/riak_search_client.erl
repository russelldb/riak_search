-module(riak_search_client, [RiakClient]).

-include("riak_search.hrl").

-define(MAX_MULTI_TERM_SZ, 250).
-define(OPTIMIZER_PROC_CT, 32).

-export([
    %% Searching...
    parse_query/2,
    search/5,
    search_doc/5,

    %% Stream Searching...
    stream_search/2,
    collect_result/2,

    %% Explain...
    explain/2,
    query_as_graph/1,

    %% Indexing...
    index_doc/2,
    index_term/5,

    %% Delete
    delete_doc/2,
    delete_term/4,

    %% utility
    dump_graph/1
]).

-import(riak_search_utils, [
    from_binary/1,
    to_binary/1
]).


%% Parse the provided query. Returns either {ok, QueryOps} or {error,
%% Error}.
parse_query(IndexOrSchema, Query) ->
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    {ok, AnalyzerPid} = qilr:new_analyzer(),
    try
        qilr_parse:string(AnalyzerPid, Query, list_to_atom(Schema:default_op()),
                          Schema:field_types(), Schema:analyzer_factory())
    after
        qilr:close_analyzer(AnalyzerPid)
    end.

%% Run the Query, return the list of keys.
%% Timeout is in milliseconds.
%% Return the {Length, Results}.
search(IndexOrSchema, QueryOps, QueryStart, QueryRows, Timeout) ->
    %% Execute the search and collect the results.
    SearchRef = stream_search(IndexOrSchema, QueryOps),
    Results = collect_results(SearchRef, Timeout, []),

    %% Dedup, and handle start and max results. Return matching
    %% documents.
    Results1 = truncate_list(QueryStart, QueryRows, Results),
    Length = length(Results),
    {Length, Results1}.

search_doc(IndexOrSchema, QueryOps, QueryStart, QueryRows, Timeout) ->
    %% Get results...
    {Length, Results} = search(IndexOrSchema, QueryOps, QueryStart, QueryRows, Timeout),
    MaxScore = case Results of
                   [] ->
                       "0.0";
                   [{_, Attrs}|_] ->
                       [MS] = io_lib:format("~g", [proplists:get_value(score, Attrs)]),
                       MS
               end,
    %% Fetch the documents in parallel.
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    Index = Schema:name(),
    F = fun({DocID, _}) ->
        riak_indexed_doc:get(RiakClient, Index, DocID)
    end,
    Documents = plists:map(F, Results, {processes, 4}),
    {Length, MaxScore, [X || X <- Documents, X /= {error, notfound}]}.

%% Run the query through preplanning, return the result.
explain(IndexOrSchema, QueryOps) ->
    %% Get schema information...
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    DefaultIndex = Schema:name(),
    DefaultField = Schema:default_field(),
    Facets = [{DefaultIndex, Schema:field_name(X)} || X <- Schema:facets()],

    %% Run the query through preplanning.
    optimize_query(riak_search_preplan:preplan(QueryOps, DefaultIndex, DefaultField, Facets)).

%% Index a specified #riak_idx_doc
index_doc(IdxDoc, AnalyzerPid) ->
    {ok, Postings} = riak_indexed_doc:analyze(IdxDoc, AnalyzerPid),
    F = fun(X) ->
        {Index, Field, Term, Value, Props} = X,
        index_term(Index, Field, Term, Value, Props)
    end,
    plists:map(F, Postings, {processes, 4}),
    riak_indexed_doc:put(RiakClient, IdxDoc).

%% Index the specified term.
index_term(Index, Field, Term, Value, Props) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    Preflist = riak_core_apl:get_apl(Partition, N),
    riak_search_vnode:index(Preflist, Index, Field, Term, Value, Props).

delete_term(Index, Field, Term, DocId) ->
    {N, Partition} = riak_search_utils:calc_n_partition(Index, Field, Term),
    Preflist = riak_core_apl:get_apl(Partition, N),
    riak_search_vnode:delete_term(Preflist, Index, Field, Term, DocId).

truncate_list(QueryStart, QueryRows, List) ->
    %% Remove the first QueryStart results...
    case QueryStart =< length(List) of
        true  -> {_, List1} = lists:split(QueryStart, List);
        false -> List1 = []
    end,

    %% Only keep QueryRows results...
    case QueryRows /= infinity andalso QueryRows =< length(List1) of
        true  -> {List2, _} = lists:split(QueryRows, List1);
        false -> List2 = List1
    end,

    %% Return.
    List2.

stream_search(IndexOrSchema, OpList) ->
    %% Get schema information...
    {ok, Schema} = riak_search_config:get_schema(IndexOrSchema),
    DefaultIndex = Schema:name(),
    DefaultField = Schema:default_field(),
    Facets = [{DefaultIndex, Schema:field_name(X)} || X <- Schema:facets()],
    %% Normalize, Optimize, and Expand Buckets.
    OpList1 = riak_search_preplan:preplan(OpList, DefaultIndex, DefaultField, Facets),

    %% Query optimization pass
    %%io:format("OpList1 = ~p~n", [OpList1]),
    OpList2 = optimize_query(OpList1),
    %%io:format("OpList2 = ~p~n", [OpList2]),

    %% Get the total number of terms and weight in query...
    {NumTerms, NumDocs, QueryNorm} = get_scoring_info(OpList1),

    %% Set up the operators. They automatically start when created...
    Ref = make_ref(),
    QueryProps = [{num_docs, NumDocs},
                  {default_field, DefaultField},
                  {index_name, IndexOrSchema}],

    %% Start the query process ...
    {ok, NumInputs} = riak_search_op:chain_op(OpList2, self(), Ref, QueryProps),
    #riak_search_ref {
        id=Ref, termcount=NumTerms,
        inputcount=NumInputs, querynorm=QueryNorm }.

%% Gather all results from the provided SearchRef, return the list of
%% results sorted in descending order by score.
collect_results(SearchRef, Timeout, Acc) ->
    M = collect_result(SearchRef, Timeout),
    case M of
        {done, _} ->
            sort_by_score(SearchRef, Acc);
        {[], Ref} ->
            collect_results(Ref, Timeout, Acc);
        {Results, Ref} ->
            collect_results(Ref, Timeout, Acc ++ Results);
        Error ->
            Error
    end.

%% Collect one or more individual results in as non-blocking of a
%% fashion as possible. Returns {Results, SearchRef}, {done,
%% SearchRef}, or {error, timeout}.
collect_result(#riak_search_ref{inputcount=0}=SearchRef, _Timeout) ->
    {done, SearchRef};
collect_result(#riak_search_ref{id=Id, inputcount=InputCount}=SearchRef, Timeout) ->
    receive
        {results, Results, Id} ->
            {Results, SearchRef};
        {disconnect, Id} ->
            {[], SearchRef#riak_search_ref{inputcount=InputCount - 1}}
        after Timeout ->
             {error, timeout}
    end.

%% Return {NumTerms, NumDocs, QueryNorm}...
%% http://lucene.apache.org/java/2_4_0/api/org/apache/lucene/search/Similarity.html
get_scoring_info(Op) ->
    %% Get a list of scoring info...
    List = lists:flatten(get_scoring_info_1(Op)),
    case List /= [] of
        true ->
            %% Calculate NumTerms and NumDocs...
            NumTerms = length(List),
            NumDocs = lists:sum([NodeWeight || {NodeWeight, _} <- List]),

            %% Calculate the QueryNorm...
            F = fun({DocFrequency, Boost}, Acc) ->
                IDF = 1 + math:log((NumDocs + 1) / (DocFrequency + 1)),
                Acc + math:pow(IDF * Boost, 2)
            end,
            SumOfSquaredWeights = lists:foldl(F, 0, List),
            QueryNorm = 1 / math:pow(SumOfSquaredWeights, 0.5),

            %% Return.
            {NumTerms, NumDocs, QueryNorm};
        false ->
            {0, 0, 0}
    end.
get_scoring_info_1(Op) when is_record(Op, term) ->
    Weights = [X || {node_weight, _, X} <- Op#term.options],
    DocFrequency = hd(Weights ++ [0]),
    Boost = proplists:get_value(boost, Op#term.options, 1),
    [{DocFrequency, Boost}];
get_scoring_info_1(Op) when is_record(Op, mockterm) ->
    [];
get_scoring_info_1(#phrase{base_query=BaseQuery}) ->
    get_scoring_info_1(BaseQuery);
get_scoring_info_1(Op) when is_tuple(Op) ->
    get_scoring_info_1(element(2, Op));
get_scoring_info_1(Ops) when is_list(Ops) ->
    [get_scoring_info_1(X) || X <- Ops].


sort_by_score(#riak_search_ref{querynorm=QNorm, termcount=TermCount}, Results) ->
    SortedResults = lists:sort(calculate_scores(QNorm, TermCount, Results)),
    [{Value, Props} || {_, Value, Props} <- SortedResults].

calculate_scores(QueryNorm, NumTerms, [{Value, Props}|Results]) ->
    ScoreList = proplists:get_value(score, Props),
    Coord = length(ScoreList) / (NumTerms + 1),
    Score = Coord * QueryNorm * lists:sum(ScoreList),
    NewProps = lists:keystore(score, 1, Props, {score, Score}),
    [{-1 * Score, Value, NewProps}|calculate_scores(QueryNorm, NumTerms, Results)];
calculate_scores(_, _, []) ->
    [].

delete_doc(Index, DocId) ->
    case riak_indexed_doc:get(RiakClient, Index, DocId) of
        {error, notfound} ->
            {error, notfound};
        IdxDoc ->
            %% Get the postings...
            {ok, Postings} = riak_indexed_doc:analyze(IdxDoc),

            %% Delete the postings...
            F = fun(X) ->
                {Index, Field, Term, Value, _Props} = X,
                delete_term(Index, Field, Term, Value)
            end,
            plists:map(F, Postings, {processes, 4}),

            %% Delete the indexed doc.
            riak_indexed_doc:delete(RiakClient, Index, DocId)
    end.

query_as_graph(OpList) ->
    G = digraph:new(),
    digraph:add_vertex(G, root, "root"),
    digraph:add_vertex(G, nodes, "nodes"),
    digraph:add_vertex(G, or_ops, "or_ops"),
    digraph:add_vertex(G, and_ops, "and_ops"),
    digraph:add_vertex(G, not_ops, "not_ops"),
    digraph:add_edge(G, root, nodes, "has-property"),
    digraph:add_edge(G, root, or_ops, "has-property"),
    digraph:add_edge(G, root, and_ops, "has-property"),
    digraph:add_edge(G, root, not_ops, "has-property"),
    query_as_graph(OpList, root, 0, G),
    %dump_graph(G),
    G.

dump_graph(G) ->
    dump_graph(G, root).

dump_graph(G, StartNode) ->
    dump_graph(G, StartNode, 1).

dump_graph(G, Parent, Tabs) ->
    lists:map(fun(Node) ->
        tab_n(Tabs),
        case length(digraph:out_neighbours(G, Node)) > 0 of
            true ->
                io:format("(~p) ~p~n", [Parent, Node]);
            false ->
                io:format("~p~n", [Node])
        end,
        case is_atom(Parent) andalso Parent /= root of
            true -> skip;
            _ -> dump_graph(G, Node, Tabs+2)
        end
    end, digraph:out_neighbours(G, Parent)),
    ok.

tab_n(Tabs) ->
    lists:map(fun(_) -> io:format("  ") end, lists:seq(0,Tabs)).

%%
%% query optimization
%%

%
% perform all query optimizations
%
% optimize_junction for "OR" and "AND" to multi_term operator
%   per term-node coincidence
%
optimize_query(OpList) ->
    G = query_as_graph(OpList),
    JunctionOps =
        digraph:out_neighbours(G, or_ops) ++
        digraph:out_neighbours(G, and_ops),
    lists:foreach(fun(JunctionNode) ->
        %io:format("optimize_query: JunctionNode = ~p~n",
        %    [JunctionNode]),
        optimize_junction(G, JunctionNode)
    end, JunctionOps),
    OpList2 = graph_as_query(G, root, []),
    [OpList2].

%
% digraph to s-expression OpList
%
graph_as_query(G, Node, Acc) ->
    Out0 = digraph:out_neighbours(G, Node),
    case Node of
        and_ops -> Out = [];
        or_ops -> Out = [];
        not_ops -> Out = [];
        nodes -> Out = [];
        root -> Out = Out0;
        _ ->
            case is_atom(Node) of
                true -> io:format("graph_as_query: unknown atom: ~p~n", [Node]),
                        Out = [];
                _ ->
                    Out = Out0
            end
    end,
    case length(Out) of
        0 ->
            case Node of
                {multi_term, _NTerms, _TNode} -> Node;
                and_ops -> [];
                or_ops -> [];
                not_ops -> [];
                nodes -> [];
                _ ->
                    case is_list(Node) of
                        true -> graph_as_query(G, hd(Node), Acc);
                        _ -> []
                    end
            end;
        _ ->
            case Node of
                {lnot, _N} ->
                    Terms = lists:reverse(lists:map(fun(OutNode) ->
                        graph_as_query(G, OutNode, Acc)
                    end, Out)),
                    {lnot, Terms};
                {land, _N} ->
                    Terms = lists:reverse(lists:map(fun(OutNode) ->
                        graph_as_query(G, OutNode, Acc)
                    end, Out)),
                    {node, {land, Terms}, node()}; %% todo: real node?
                {lor, _N} ->
                    Terms = lists:reverse(lists:map(fun(OutNode) ->
                        graph_as_query(G, OutNode, Acc)
                    end, Out)),
                    {node, {lor, Terms}, node()}; %% todo: real node?
                {term, {I, F, T}, Props} ->
                    % todo: fix counts (until then, everyone has a 1 count)
                    %  ^update: remove all count dependencies (and ultimately, the call
                    %           to get them, and maybe the metadata itself from the store)
                    % io:format("term, IFT, Out = ~p~n", [Out]),
                    NodeWeights =
                        lists:reverse(
                            lists:usort(
                                lists:map(
                                    fun({node, N1}) ->
                                            {node_weight, N1, 1}
                                    end,
                                Out)
                            )
                        ),
                    R = {term, {I, F, T},
                         proplists:delete(node_weight, Props) ++ NodeWeights},
                    R;
                root ->
                    lists:foldl(fun(N, RAcc) ->
                        case graph_as_query(G, N, Acc) of
                            [] -> RAcc;
                            V -> [V|RAcc]
                        end
                    end, [], Out);
                _ ->
                    io:format("graph_as_query: unknown_node_type: ~p: ~p~n", [Node, Out]),
                    {unknown_node_type, Node}
            end
    end.


%
% optimize_junction
%
% {lor, N} -> [ (T->N), ... ]
% to:
% {lor, N} -> [ MG({T0, T1, ...}, N), ... ]
%
optimize_junction(G, OrNode) ->
    Terms0 = digraph:out_neighbours(G, OrNode),
    Terms = lists:filter(fun(E) ->
        case E of
            [] -> false;
            _ -> true
        end
    end, lists:map(fun(T0) ->
            case T0 of
                {term, _, _} ->
                    T0;
                {term, _} ->
                    T0;
                _ -> []
            end
        end, Terms0)),
    L = lists:foldl(fun(T, Acc) ->
        lists:map(fun(Node_N) ->
            case Node_N of
                {node, N} ->
                    case proplists:is_defined(N, Acc) of
                        true ->
                            {N, TL} = proplists:lookup(N, Acc),
                            proplists:delete(N, Acc) ++ {N, lists:flatten(TL ++ [T])};
                        false -> Acc ++ {N, [T]}
                    end;
                _ -> Acc
            end
        end, digraph:out_neighbours(G, T))
    end, [], Terms),
    TCD = lists:sort(fun(A,B) ->
        {_Na, La} = A,
        {_Nb, Lb} = B,
        length(La) >= length(Lb)
    end, L),
    lists:foreach(fun(N_NTerms) ->
        {Node, NodeTerms} = N_NTerms,
        GOutNeighbors = digraph:out_neighbours(G, OrNode),
        RemTerms = lists:foldl(fun(RTerm, Acc) ->
            case lists:member(RTerm, GOutNeighbors) of
                    false -> Acc;
                    true -> Acc ++ [RTerm]
            end
        end, [], NodeTerms),
        case RemTerms of
            [] -> skip;
            _ ->
                lists:foreach(fun(Nt) ->
                        digraph:del_edges(G, digraph:edges(G, Nt)),
                        digraph:del_vertex(G, Nt)
                end, RemTerms),

                L_RemTerms = chop_list(RemTerms, ?MAX_MULTI_TERM_SZ, []),
                lists:map(fun(RemTerms2) ->
                    Vtx = {multi_term, RemTerms2, Node},
                    digraph:add_vertex(G, Vtx),
                    digraph:add_edge(G, OrNode, Vtx)
                end, L_RemTerms)
        end
    end, TCD),
    TCD.

chop_list(L, Sz, Acc) ->
    case length(L) =< Sz of
        true -> Acc ++ [L];
        false ->
            {L1, Rest} = lists:split(Sz, L),
            chop_list(Rest, Sz, Acc ++ [L1])
    end.


%
% turn OpList s-expression into digraph
%
query_as_graph(OpList, Parent, C0, G) ->
    case is_list(OpList) of
        true ->
            lists:foldl(fun(Op, C) ->
                case Op of
                    [L] ->
                        %%io:format("[L] / ~p~n", [L]),
                        query_as_graph(L, Parent, C, G),
                        C+1;
                    {node, {lor, N}, _Node} ->
                        %%io:format("[~p] lor: ~p~n", [Node, N]),
                        case N of
                            [] -> C;
                            _ ->
                                V = {lor, C},
                                digraph:add_vertex(G, V, "or"),
                                digraph:add_edge(G, Parent, V, "has-or"),
                                digraph:add_edge(G, or_ops, V, "has-member"),
                                query_as_graph(N, V, C+1, G)+1
                        end;
                    {node, {land, N}, _Node} ->
                        %%io:format("[~p] land: ~p~n", [Node, N]),
                        case N of
                            [] -> C;
                            _ ->
                                V = {land, C},
                                digraph:add_vertex(G, V, "and"),
                                digraph:add_edge(G, Parent, V, "has-and"),
                                digraph:add_edge(G, and_ops, V, "has-member"),
                                query_as_graph(N, V, C+1, G)+1
                        end;
                    {lnot, N} ->
                        %%io:format("lnot: ~p~n", [N]),
                        case N of
                            [] -> C;
                            _ ->
                                V = {lnot, C},
                                digraph:add_vertex(G, V, "not"),
                                digraph:add_edge(G, Parent, V, "has-not"),
                                digraph:add_edge(G, not_ops, V, "has-member"),
                                query_as_graph(N, V, C+1, G)+1
                        end;
                    {term, IFT, Props} ->
                        %%io:format("term, IFT = ~p, Props = ~p~n",
                        %%    [IFT, Props]),
                        V = {term, IFT, Props},
                        digraph:add_vertex(G, V, "term"),
                        digraph:add_edge(G, Parent, V, "has-term"),
                        query_as_graph(Props, V, C+1, G)+1;
                    {facets, _Facets} ->
                        %% ignore facets at this level (already accounted for)
                        C;
                    {fuzzy, _StrDistance} ->
                        %% ignore fuzzy prop (already accounted for)
                        C;
                    {node_weight, N, _NodeCount} ->
                        %%io:format("~p: ~p (~p)~n", [Parent, N, NodeCount]),
                        V = {node, N},
                        case lists:keysearch(N, 2, digraph:vertices(G)) of
                            false ->
                                digraph:add_vertex(G, V, "node"),
                                digraph:add_edge(G, nodes, V, "has-member");
                            _ -> skip
                        end,
                        digraph:add_edge(G, Parent, V, "has-location"),
                        C+1;
                    X ->
                        io:format("query_as_graph: Unknown node type: ~p~n", [X])
                end end, C0, OpList);
        false ->
            query_as_graph([OpList], Parent, C0, G)
    end.
