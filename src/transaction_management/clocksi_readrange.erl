%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(clocksi_readrange).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("kernel/include/logger.hrl").
-define(LASTTIMEOUT, 5).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([
    read_range/7,
    perform_read_range/7
]).

%% Spawn
-type read_property_list() :: [].
-export_type([read_property_list/0]).
%%%===================================================================
%%% API
%%%===================================================================

-spec read_range(index_node(), key(), key(), non_neg_integer(),  type(), tx(), read_property_list()) -> {ok, snapshot()}.
read_range({Partition, Node}, Min, Max, Timeout, Type, Transaction, PropertyList) ->
    rpc:call(Node, ?MODULE, perform_read_range, [Min, Max, Timeout, Type, Transaction, PropertyList, Partition]).

%%%===================================================================
%%% Internal
%%%===================================================================

-spec perform_read_range(key(), key(), non_neg_integer(), type(), tx(), read_property_list(), partition_id()) ->
    {error, term()} | {ok, snapshot()}.
perform_read_range(Min, Max, Timeout, Type, Transaction, _PropertyList, _Partition) ->
    run_quroum(Min, Max, Timeout, Type, Transaction).

%% @doc return:
%%  - Reads and returns range of values from all active vnodes.
-spec run_quroum(key(), key(), non_neg_integer(), type(), tx()) -> {ok, snapshot()}.
run_quroum(Min, Max, Timeout, _Type, Transaction) ->
    VecSnapshotTime = Transaction#transaction.vec_snapshot_time,
    ReqId = make_ref(),
    {ok, Pid}= range_fsm_sup:start_fsm([ReqId, self(), ?BUCKET, {Min, Max, VecSnapshotTime}, get_range]),
    range_fsm:prepare(ReqId),
    range_fsm:execute(ReqId),
    wait_for_reqid(ReqId, Timeout, Pid).

wait_for_reqid(ReqId, Timeout, Pid) ->
    receive
        {ReqId, Val} ->
            Val
    after
        Timeout ->
            Pid ! get_responses,
            receive
                {ReqId, Val} ->
                    Val
            after
                ?LASTTIMEOUT -> {error, timeout}
            end
    end.
