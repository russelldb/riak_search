%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_search_sup).

-include("riak_search.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Config = {riak_search_config,
              {riak_search_config, start_link, []},
              permanent, 5000, worker, [riak_search_config]},
    VMaster = {riak_search_vnode_master,
               {riak_core_vnode_master, start_link, [riak_search_vnode]},
               permanent, 5000, worker, [riak_core_vnode_master]},
    ExtractJSPool = {?JSPOOL_SEARCH_EXTRACT,
                     {riak_kv_js_manager, start_link,
                      [?JSPOOL_SEARCH_EXTRACT, extract_js_vm_count()]},
                 permanent, 30000, worker, [riak_kv_js_manager]},
    Processes = [Config,
                 VMaster,
                 ExtractJSPool],
    {ok, { {one_for_one, 5, 10}, Processes} }.

extract_js_vm_count() ->
    case app_helper:get_env(riak_search, extract_js_vm_count, undefined) of
        Size when is_integer(Size),
                  Size > 0 ->
            Size;
        _ ->
            error_logger:info_msg("The riak_search application parameter "
                                  "`extract_js_vm_count` was either not defined "
                                  "or given an invalid value. "
                                  "JavaScript extract functions will not work "
                                  "unless this parameter is set to a positive "
                                  "integer.~n"),
            0
    end.
