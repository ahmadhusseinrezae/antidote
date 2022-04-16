-module(range_tree_sup).
-behavior(supervisor).

-export([start_sup/1, start_link/0]).
-export([init/1]).

-ignore_xref([init/1, start_link/0]).

start_sup(Args) ->
    supervisor:start_child(?MODULE, Args).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one, 10, 10}, [
        {tree,
        {range_tree, start_link, []},
        permanent, 1000, worker, [range_tree]}
    ]}}.
