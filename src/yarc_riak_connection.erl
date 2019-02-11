%%%-------------------------------------------------------------------
%%% @author forgottenEntity
%%% @doc
%%%
%%% @end
%%% Created : 29. Jan 2019 00:32
%%%-------------------------------------------------------------------
-module(yarc_riak_connection).
-author("forgottenEntity").

-behaviour(gen_server).
-behaviour(poolboy_worker).

%% API

-record(state, {riak_process}).

-export([get/4,
         get_meta/4,
         put/3,
         put/4,
         delete/4
        ]).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

%% **************************************************************************** %%
%% API Functions %%
%% **************************************************************************** %%

get(Connection, Bucket, Key, Timeout) ->
  gen_server:call(Connection, {get, {Bucket, Key, Timeout}}).

get_meta(Connection, Bucket, Key, Timeout) ->
  gen_server:call(Connection, {get_meta, {Bucket, Key, Timeout}}).

put(Connection, Timeout, RiakObj) ->
  gen_server:call(Connection, {put, {Timeout, RiakObj}}).

put(Connection, Timeout, RiakObj, return_body) ->
  gen_server:call(Connection, {put, {Timeout, RiakObj, return_body}});
put(Connection, Timeout, RiakObj, return_head) ->
  gen_server:call(Connection, {put, {Timeout, RiakObj, return_head}}).

delete(Connection, Bucket, Key, Timeout) ->
  gen_server:call(Connection, {delete, {Bucket, Key, Timeout}}).



%% **************************************************************************** %%
%% poolboy_worker functions %%
%% **************************************************************************** %%


start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).


%% **************************************************************************** %%
%% gen_server functions %%
%% **************************************************************************** %%

init(Args) ->
  Riak_PB_IP = proplists:get_value(riak_pb_ip, Args),
  Riak_PB_Port = proplists:get_value(riak_pb_port, Args),
  Riak_PB_Options= proplists:get_value(riak_pb_options, Args),

  {ok, Pid} = riakc_pb_socket:start_link(Riak_PB_IP, Riak_PB_Port, Riak_PB_Options),
  {ok, #state{riak_process = Pid}}.

handle_call({get, {Bucket, Key, Timeout}}, _From, State = #state{riak_process=RiakProcess}) ->
  Result = riakc_pb_socket:get(RiakProcess, Bucket, Key, Timeout),
  {reply, Result, State};

handle_call({get_meta, {Bucket, Key, Timeout}}, _From, State = #state{riak_process=RiakProcess}) ->
  Result = riakc_pb_socket:get(RiakProcess, Bucket, Key, [head], Timeout),
  {reply, Result, State};

handle_call({put, {Timeout, RiakObj}}, _From, State = #state{riak_process=RiakProcess}) ->
  Result = riakc_pb_socket:put(RiakProcess, RiakObj, Timeout),
  {reply, Result, State};

handle_call({put, {Timeout, RiakObj, return_body}}, _From, State = #state{riak_process=RiakProcess}) ->
  Result = riakc_pb_socket:put(RiakProcess, RiakObj, [return_body], Timeout),
  {reply, Result, State};

handle_call({put, {Timeout, RiakObj, return_head}}, _From, State = #state{riak_process=RiakProcess}) ->
  Result = riakc_pb_socket:put(RiakProcess, RiakObj, [return_head], Timeout),
  {reply, Result, State};

handle_call({delete, {Bucket, Key, Timeout}}, _From, State = #state{riak_process=RiakProcess}) ->
  Result = riakc_pb_socket:delete(RiakProcess, Bucket, Key, Timeout),
  {reply, Result, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

