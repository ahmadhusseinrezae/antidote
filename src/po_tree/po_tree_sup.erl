-module(po_tree_sup).
-author("ahr").
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
        {po_tree_server, start_link, []},
        permanent, 1000, worker, [po_tree_server]}
    ]}}.
