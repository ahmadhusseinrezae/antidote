%% @doc Supervise the huskv_write FSM.
-module(range_fsm_sup).
-author("ahr").
-behavior(supervisor).

-export([start_fsm/1, start_link/0]).
-export([init/1]).

-ignore_xref([init/1, start_link/0]).

start_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    RangeFsm = {undefined,
                {range_fsm, start_link, []},
                temporary, 5000, worker, [range_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [RangeFsm]}}.

