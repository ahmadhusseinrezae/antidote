-module(minheap).

-export([merge/1, trim_list/1, initialize_heap/3, merge_lists/3]).

merge([]) -> [];
merge(T) ->
%%    make a list of non empty results
    RealT = trim_list(T),
    N = length(RealT),
    Ordset = ordsets:new(),
    {NewOrdset, NewList} = initialize_heap(Ordset, RealT, N),
    merge_lists(NewOrdset, NewList, ordsets:size(NewOrdset)).

merge_lists(_, _, 0) -> [];
merge_lists(Set, List, _SetSize) ->
%%    get the first element of set of all first elements of results which has been sorted by ordsets
%%    this first element is minimum element
%%    Item is the val
%%    Data is {Val, RowID or any other data saved by index}
%%    Index is the index of result that Item comes from, it will be used to find the next item from that list
     {Item, Data, Index} = lists:nth(1, ordsets:to_list(Set)),
%%    remove the minimum element from set
     NewSet = ordsets:del_element({Item, Data, Index}, Set),
%%    get the next item from that list that minimum item comes from, this next item will be added to ordset to be sorted
%%    and be used for the next iteration
     ItemRes = get_item(List, Index),
     case ItemRes of 
        {none, NewList} ->
%%            if there is not any more item in that sub list then simply add the minimum item(which was the last item in that sublist) and
%%            continue to next iteration which one sublist less than previous iteration
            [Data] ++ merge_lists(NewSet, NewList, ordsets:size(NewSet));
        {{Head, NewData}, NewList} ->
%%            if there is an element, add it to ordset to have it sorted in set
            NewSet2 = ordsets:add_element({Head, {Head, NewData}, Index}, NewSet),
%%            add minimum item to result list and continue
            [Data] ++ merge_lists(NewSet2, NewList, ordsets:size(NewSet2))
    end.

initialize_heap(Set, List, 0) ->
    {Set, List};
initialize_heap(Set, List, N) -> 
    Item = get_item(List, N),
    case Item of 
        {none, List} ->
            initialize_heap(Set, List, N-1);
        {{Head, Data}, NewList} ->
%%            add element to orderset which it will then order it based on Head which is value of key-val store
            NewSet = ordsets:add_element({Head, {Head, Data}, N}, Set),
            initialize_heap(NewSet, NewList, N-1)
    end.

get_item(List, 1) -> 
    [SubList | Tail] = List,
    case get_head(SubList) of
        none ->
            {none , List};
        {H, T} ->
            {H, [T | Tail]}
    end;
get_item(List, Index) -> 
    {HeadList, [SubList | Tail]} = lists:split(Index -1, List),
    case get_head(SubList) of
        none ->
            {none , List};
        {H, T} ->
            {H, HeadList ++ [T | Tail]}
    end.


get_head([]) -> none;
get_head([H | T]) -> {H, T}.

%% this will removed the empty results returned
trim_list([]) -> [];
trim_list([[]| T]) -> trim_list(T);
trim_list([H | T]) -> [H] ++ trim_list(T).




