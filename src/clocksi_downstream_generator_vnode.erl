-module(clocksi_downstream_generator_vnode).

-behaviour(riak_core_vnode).

-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(dstate, {partition, last_commit_time, pending_operations}).

-export([start_vnode/1,
         trigger/2]).

-export([init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

%% @doc Notify downstream_generator_vnode that an update has been logged.
%%      Downstream_generator_vnode then reads the updates from log, generate
%%      downstream op and write to persistent log
%%      input: Key to identify the partition,
%%             Writeset -> set of updates
-spec trigger(Key :: term(),
              Writeset :: {TxId :: term(),
                           Updates :: [{term(), {Key :: term(), Op :: term()}}],
                           Vec_snapshot_time :: vectorclock:vectorclock(),
                           Commit_time :: non_neg_integer()})
             -> ok | {error, timeout}.
trigger(Key, Writeset) ->
    DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, 1,
                                             clocksi_downstream_generator),
    [{NewPref,_}] = Preflist,
    riak_core_vnode_master:command([NewPref], {trigger, Writeset, self()},
                                   clocksi_downstream_generator_vnode_master),
    receive
        {ok, trigger_received} ->
            ok
    after 100 ->
            {error, timeout}
    end.

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #dstate{partition = Partition,
                 last_commit_time = 0, pending_operations = []}}.

%% @doc Read client update operations,
%%      generate downstream operations and store it to persistent log.
handle_command({trigger, Write_set, From}, _Sender,
               State=#dstate{partition = Partition,
                             last_commit_time = Last_commit_time,
                             pending_operations = Pending}) ->
    Pending_operations = add_to_pending_operations(Pending, Write_set),
    lager:info("Pending Operations ~p",[Pending_operations]),
    From ! {ok, trigger_received},
    Node = {Partition, node()}, %% Send ack to caller and continue processing

    Stable_time = get_stable_time(Node),
    lager:info("Take updates before time : ~p",[Stable_time]),
    Sorted_ops = filter_operations(Pending_operations,
                                   Stable_time, Last_commit_time),
    lager:info("Generate downstream for ~p",[Sorted_ops]),
    {Remaining_operations, Last_processed_time} =
        lists:foldl( fun(X, {Ops, LCTS}) ->
                             case process_update(X) of
                                 {ok, Commit_time} ->
                                     %% Remove this op from pending_operations
                                     New_pending = lists:delete(X, Ops),
                                     {New_pending, Commit_time};
                                 {error, _Reason} ->
                                     {Ops, LCTS}
                             end
                     end, {Pending_operations, Last_commit_time}, Sorted_ops),
    Dc_id = dc_utilities:get_my_dc_id(),
    vectorclock:update_clock(Partition, Dc_id, Stable_time),
    inter_dc_repl_vnode:trigger({Partition, node()}),
    {reply, ok, State#dstate{last_commit_time = Last_processed_time,
                             pending_operations = Remaining_operations}};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command_logging, Message}),
    {noreply, State}.

handle_handoff_command( _Message , _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% @doc Generate downstream for one update and write to log
process_update(Update) ->
    case clocksi_downstream:generate_downstream_op(Update) of
        {ok, New_op} ->
            Key = New_op#clocksi_payload.key,
            case materializer_vnode:update(Key, New_op) of
                ok ->
                    {_Dcid, Commit_time} = New_op#clocksi_payload.commit_time,
                    {ok, Commit_time};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Return smallest snapshot time of active transactions.
%%      No new updates with smaller timestamp will occur in future.
get_stable_time(Node) ->
    lager:info("In get_stable_time"),
    {ok, Active_txns} = riak_core_vnode_master:sync_command(
                         Node, {get_active_txns}, ?CLOCKSIMASTER),
    lager:info("Active txns before filtering"),
    lists:foldl(fun({_TxId, Snapshot_time}, Min_time) ->
                        case Min_time > Snapshot_time of
                            true ->
                                Snapshot_time;
                            false ->
                                Min_time
                        end
                end,
                now_milisec(erlang:now()),
                Active_txns).

now_milisec({MegaSecs,Secs,MicroSecs}) ->
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

%% @doc Select updates committed before Time and sort them in timestamp order.
filter_operations(Ops, Before, After) ->
    %% Remove operations which are already processed
    Unprocessed_ops =
        lists:filtermap(
          fun(Payload) ->
                  {_Dcid, Commit_time} = Payload#clocksi_payload.commit_time,
                  Commit_time > After
          end,
          Ops),
    lager:info("Unprocessed Ops ~p after ~p",[Unprocessed_ops, After]),

    %% remove operations which are not safer to process now,
    %% because there could be other operations with lesser timestamps
    Filtered_ops =
        lists:filtermap(
          fun(Payload) ->
                  {_Dcid, Commit_time} = Payload#clocksi_payload.commit_time,
                  Commit_time < Before
          end,
          Unprocessed_ops),

    lager:info("Filter operations: ~p",[Filtered_ops]),
    %% Sort operations in timestamp order
    Sorted_ops =
        lists:sort(
          fun( Payload1, Payload2) ->
                  {_Dcid1, Time1} = Payload1#clocksi_payload.commit_time,
                  {_Dcid1, Time2} = Payload2#clocksi_payload.commit_time,
                  Time1 < Time2
          end,
          Filtered_ops),
    Sorted_ops.

%%@doc Add updates in writeset ot Pending operations to process downstream
add_to_pending_operations(Pending, Write_set) ->
    lager:info("Writeset : ~p",[Write_set]),
    case Write_set of
        {TxId, Updates, Vec_snapshot_time, Commit_time} ->
            lists:foldl( fun(Update, Operations) ->
                                 {_,{Key,{Op,Actor}}} = Update,
                                 Dc_id = dc_utilities:get_my_dc_id(),
                                 New_op = #clocksi_payload{
                                            key = Key, type = riak_dt_pncounter,
                                            op_param = {Op, Actor},
                                            snapshot_time = Vec_snapshot_time,
                                            commit_time = {Dc_id,Commit_time},
                                            txid = TxId},
                                 lists:append(Operations, [New_op])
                         end,
                         Pending, Updates);
        _  -> Pending
    end.
