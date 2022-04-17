-module(range_fsm).

-behavior(gen_statem).


%% API
-export([start_link/5, run/5]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3, callback_mode/0]).

-ignore_xref([start_link/7, init/1, code_change/4, handle_event/3,
              handle_info/3, handle_sync_event/4, terminate/3, run/7]).


-export([prepare/1, prepare/3, execute/1, execute/3, waiting/3]).

-ignore_xref([prepare/3, execute/3, waiting/2]).


-record(state, {req_id :: pos_integer(),
                from :: pid(),
                fsm :: pid(),
                n :: pos_integer(),
                w :: pos_integer(),
                range :: {number(), number(), boolean(), boolean()},
                bucket :: term(),
                accum = [],
                action :: atom(),
                preflist :: riak_core_apl:preflist2(),
                num_w = 0 :: non_neg_integer()}).


%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqId, From, Bucket, Range, Action) ->
    gen_statem:start_link( {local, list_to_atom(ref_to_list(ReqId))}, ?MODULE, [ReqId, From, Bucket, Range, Action],[]).

run(Action, Bucket, Range, Pid, ReqId) ->
    range_fsm_sup:start_fsm([ReqId, Pid, Bucket, Range, Action]),
    {ok, ReqId}.

prepare(ReqId)->
    gen_statem:call(list_to_atom(ref_to_list(ReqId)), do).

execute(ReqId)->
    gen_statem:call(list_to_atom(ref_to_list(ReqId)), do).

%%%===================================================================
%%% States
%%%===================================================================
terminate(_Reason, _State, _Data) ->
    void.
code_change(_Vsn, State, Data, _Extra) ->
    {ok,State,Data}.
%% @doc Initialize the state data.
init([ReqId, From, Bucket, Range, Action]) ->
    SD = #state{req_id=ReqId, from=From, action=Action, bucket = Bucket, range=Range, fsm = self()},
    {ok, prepare, SD}.
callback_mode() -> state_functions.

%% @doc Prepare by calculating the _preference list_.
prepare({call, From}, do, SD0=#state{bucket=_Bucket}) ->
    {ok, {_,_, _, {_, Ring}, _, _, _,_, _, _, _}} = riak_core_ring_manager:get_my_ring(),
    SD = SD0#state{preflist=Ring, num_w = 0, w = length(Ring)},
    {next_state, execute, SD, [{reply,From,execute}]}. % at least for now we dont have much to put here.

%% @doc Execute the request and then go into waiting state to
%% verify it has meets consistency requirements.
execute({call, From}, do, SD0=#state{req_id=ReqId, action=Action, range=Range,
                            preflist=Preflist}) ->
    Command = {Action, ReqId, Range},
    riak_core_vnode_master:command(Preflist, Command, {fsm, undefined, self()},
                                    gingko_vnode_master),
    {next_state, waiting, SD0, [{reply,From,waiting}]}.

%% @doc Wait for W reqs to respond.
waiting(info, Response, SD0=#state{req_id = ReqID, from=From, num_w=Responses, w = W, accum=Accum, bucket = _Bucket}) ->
    case Response of
        {_, {_ReqId, {{_Partition, _Node}, Res}}} ->
            NewResponses = Responses+1,
            Accum1 = [Res|Accum],
            SD = SD0#state{num_w=NewResponses, accum=Accum1},
            case NewResponses < W of
                true ->
                    {next_state, waiting, SD};
                false ->
                    Sorted = minheap:merge(Accum1),
                    From ! {ReqID, {ok, Sorted}},
                    {stop, normal, SD}
            end;
        get_responses ->
            Sorted = minheap:merge(Accum),
            From ! {ReqID, {ok, Sorted}},
            {stop, normal, SD0};
        _ ->
            {next_state, waiting, SD0}
    end.


handle_info(_Info, _StateName, StateData) ->
    % lager:warning("got unexpected info ~p", [Info]),
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    % lager:warning("got unexpected event ~p", [Event]),
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    % lager:warning("got unexpected sync event ~p", [Event]),
    {stop, badmsg, StateData}.

