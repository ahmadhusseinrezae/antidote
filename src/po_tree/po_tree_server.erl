-module(po_tree_server).
-author("ahr").
-include("potree.hrl").
-behaviour(gen_server).

-import(lists,[max/1]).

-export[start_link/1, stop/0, init_tree/1].
-export([init/1, handle_call/3, handle_cast/2]).
-export([insert/2, get_range/6, clean_store/0, remove/3]).

-record(state, {index, table, dic_table}).
-define(INDEX, index).
-define(REVINDEX, reverse_index_table).

start_link(Partition) ->
  gen_server:start_link({local, list_to_atom("tree" ++ integer_to_list(Partition))}, ?MODULE, [], []).

init_tree(Partition) ->
    po_tree_sup:start_sup([Partition]),
  ok.

stop() ->
  gen_server:cast(?MODULE, stop).

insert(Record, Partition) ->
  gen_server:call(list_to_atom("tree" ++ integer_to_list(Partition)), {insert_record, Record}).

remove(RowId, Val, Ver) ->
  gen_server:call(?MODULE, {remove, RowId, Val, Ver}).

get_range(
    Lower_bound,
    Upper_bound,
    Lower_bound_included,
    Upper_bound_included,
    Version,
    Partition) ->
  Result = gen_server:call(list_to_atom("tree" ++ integer_to_list(Partition)), {get_range, Lower_bound,Upper_bound,Lower_bound_included, Upper_bound_included, Version}),
  Result.

clean_store() ->
  gen_server:call(?MODULE, clean_store).

init(_Args) ->
  IndexTable = ets:new(?INDEX, []),
  ReverseIndexTable = ets:new(?REVINDEX, []),
  {ok, #state{index =  {nil, b}, table =  IndexTable, dic_table = ReverseIndexTable}}.


handle_call(clean_store, _From, #state{index = _Index, table = Table, dic_table = DicTable}) ->
  ets:delete_all_objects(Table),
  ets:delete_all_objects(DicTable),
  {reply, ok, #state{index = {nil,b}, table = Table, dic_table = DicTable}};

handle_call({insert_record, {RowId, Val, Ver}}, _From, #state{index = Index, table = Table, dic_table = DicTable} = State) ->
  try delete_item(RowId, Val, Ver, Index, Table, DicTable) of
    CleanIndex ->
      try po_tree:insert(Val, RowId, Ver, CleanIndex, Table) of
        NewIndex ->
          {reply, ok, State#state{index = NewIndex, table = Table}}
      catch
        throw:Throw -> {reply, Throw, State};
        error:Error -> {reply, Error, State};
        _:Exception -> {reply, Exception, State}
      end
  catch
    throw:Throw -> {reply, Throw, State};
    error:Error -> {reply, Error, State};
    _:Exception -> {reply, Exception, State}
  end;

handle_call({remove, RowId, Val, Ver}, _From, #state{index = Index, table = Table, dic_table = DicTable} = State) ->
  try po_tree:remove(Val, RowId, Ver, Index, Table) of
    NewIndex ->
      delete_record(RowId, Val, DicTable),
      {reply, ok, State#state{index = NewIndex, table = Table}}
  catch
    throw:Throw -> {reply, Throw, State};
    error:Error -> {reply, Error, State};
    _:Exception -> {reply, Exception, State}
  end;

handle_call(tree, _From, #state{index = Index} = State) ->
  {reply, Index, State};

handle_call({get_range, Min, Max, _, _, Version}, _From, #state{index = Index, table = Table} = State) ->
  try po_tree:getRange(Min, Max, Index, Table, Version) of
    Res ->
      {reply, Res, State}
  catch
    throw:Throw -> {reply, Throw, State};
    error:Error -> {reply, Error, State};
    _:Exception -> {reply, Exception, State}
  end.

handle_cast(stop, State) ->
  io:format("Stopping the server ~n"),
  {stop, normal, State};

handle_cast(Message, State) ->
  io:format("server recieved the message ~p and ignored it ~n", [Message]),
  {noreply, State}.

delete_item(RowId, NewVal, Ver, Index, Table, DicTable) ->
  case ets:lookup(DicTable, RowId) of
    [] ->
      ets:insert(DicTable, {RowId, NewVal}),
      Index;
    [{RowId, Val} | _] ->
      case Val == NewVal of
        true ->
          Index;
        false ->
          try po_tree:remove(Val, RowId, Ver, Index, Table) of
            NewIndex ->
              ets:insert(DicTable, {RowId, NewVal}),
              NewIndex
          catch
            throw:_Throw -> Index;
            error:_Error -> Index;
            _:_Exception -> Index
          end
      end
  end.

delete_record(RowId, ToRemoveVal, DicTable) ->
  case ets:lookup(DicTable, RowId) of
    [] ->
      ok;
    [{RowId, Val} | _] ->
      case Val == ToRemoveVal of
        true ->
          ets:delete(DicTable, RowId),
          ok;
        false ->
          ok
      end
  end.











