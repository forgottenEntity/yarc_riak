%%%-------------------------------------------------------------------
%%% @author chris
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Jan 2019 00:33
%%%-------------------------------------------------------------------
-module(arc_riak_connection_pool).
-author("chris").

-behaviour(supervisor).

%% Supervisor exports

-export([
         init/1
        ]).

-define(pool_name, riak_pool).
-define(pool_size_args, [{size, 400}, {max_overflow, 400}]).
-define(pool_worker_args, []).



%% API
-export([get_connection/1,
         return_connection/2
        ]).



init(_Args) ->
  {ok, Config} = application:get_env(arc_riak),

  io:format("Config: ~n~p", [Config]),

  PoolSpecs = lists:map(fun(PoolConfigPropList) ->
    Name = proplists:get_value(riak_pool_name, PoolConfigPropList),
    Size = proplists:get_value(riak_pool_base_size, PoolConfigPropList),
    MaxOverFlow = proplists:get_value(riak_pool_max_growth, PoolConfigPropList),
    PB_IP = proplists:get_value(riak_pb_ip, PoolConfigPropList),
    PB_Port = proplists:get_value(riak_pb_port, PoolConfigPropList),
    PB_Options = proplists:get_value(riak_pb_options, PoolConfigPropList),
    SizeArgs = [{size, Size}, {max_overflow, MaxOverFlow}],
    WorkerArgs = [{riak_pb_ip, PB_IP}, {riak_pb_port, PB_Port}, {riak_pb_options, PB_Options}],

    PoolArgs = [{name, {local, Name}},
      {worker_module, arc_riak_connection}] ++ SizeArgs,
    poolboy:child_spec(Name, PoolArgs, WorkerArgs)
                        end, Config),

  io:format("Poolboy Spec: ~p~n", PoolSpecs),

  {ok, {{one_for_one, 10, 10}, PoolSpecs}}.


%% API Functions %%

-spec get_connection(RiakPoolName :: atom()) -> pid().
get_connection(RiakPoolName) ->
  poolboy:checkout(RiakPoolName).

-spec return_connection(RiakPoolName :: atom(), Connection :: pid()) -> ok.
return_connection(RiakPoolName, Connection) ->
  poolboy:checkin(RiakPoolName, Connection).


