-module(redblackt).

-export([insert/5, insertToListB/3, lookup/4, findLeaf/2, getGreaterThan/2, getLessThan/3, getRange/5, findMostLeftLeafKey/1,
    findSplitPoint/2, splitData/3, get_greatest_lower_index/2, get_greatest_lower_version/2, merge/4, appendData/3, removeFromLeaf/6, removeFromTree/5, remove/5]).
-define(Order, 4).

insert(Key, Val, Ver, Tree, EtsTable) ->
    makeRootBlack(insertTo(Key, Val, Ver, Tree, EtsTable)).

insertTo(Key, Val, Ver, {nil, b}, EtsTable) ->
    B = {[{Ver, [{Key, [Val]}]}], nil, nil, [Ver]},
    ets:insert(EtsTable, {Key, B}),
    {Key, b, leaf};

insertTo(Key, Val, Ver, {KeyO, C, leaf}, EtsTable) ->
    [{_,{Data, Left, Right, Versions}}]  = ets:lookup(EtsTable, KeyO),
    [LastVer |_] = Versions,
    case versions_eq(LastVer, Ver) of
        true ->
            [{_, VerData} | T] = Data,
            case length(VerData) >= ?Order of
                true ->
                    splitLeaf(Key, Val, Ver, {Data, Left, Right, Versions, KeyO, b, leaf}, EtsTable);
                false ->
                    NewVerData = insertToListB(Key, Val, VerData),
                    case NewVerData == VerData of
                        true ->
                            {KeyO, C, leaf};
                        false ->
                            ets:insert(EtsTable, {KeyO, { [{Ver, NewVerData}] ++ T, Left, Right, Versions}}),
                            {KeyO, C, leaf}
                    end
            end;
        false ->
            case versions_gt(LastVer, Ver) of %% It is now allowed to update old versions (Partial persistent)
                true ->
                    {KeyO, C, leaf};
                false ->
                    [{_, LastVerData} | _T] = Data,  %% Add new element to payload of last version
                    NewVerData = insertToListB(Key, Val, LastVerData),
                    case NewVerData == LastVerData of
                        true ->
                            {KeyO, C, leaf};
                        false ->
                            ets:insert(EtsTable, {KeyO, { [{Ver, NewVerData}]++ Data, Left, Right, [Ver] ++ Versions}}),
                            {KeyO, C, leaf}
                    end

            end
    end;


insertTo(Key, Val, Ver, {L, R, Key2, C}, EtsTable) when Key < Key2 ->
    balance({ insertTo(Key, Val, Ver, L, EtsTable), R, Key2, C });
insertTo(Key,Val, Ver, {L, R, Key2, C}, EtsTable) when Key == Key2 ->
    { insertTo(Key, Val, Ver, L, EtsTable), R, Key2, C };
insertTo(Key, Val, Ver, {L, R, Key2, C}, EtsTable) when Key > Key2 ->
    balance({ L, insertTo(Key, Val, Ver, R, EtsTable), Key2, C }).

balance({{{L3 , R3, Key3, r}, R2, Key2, r}, R, Key, b}) ->
    {{L3 , R3, Key3, b}, {R2, R, Key, b}, Key2, r};
balance({{L2, {L3 , R3, Key3, r}, Key2, r}, R, Key, b}) ->
    {{L2 , L3, Key2, b}, {R3, R, Key, b}, Key3, r};
balance({L, {L2, {L3 , R3, Key3, r}, Key2, r}, Key, b}) ->
    {{L , L2, Key, b}, {L3, R3, Key3, b}, Key2, r};
balance({L, {{L3 , R3, Key3, r}, R2, Key2, r}, Key, b}) ->
    {{L , L3, Key, b}, {R3, R2, Key2, b}, Key3, r};
balance({L, R, Key, C}) ->
    {L, R, Key, C}.

splitLeaf(Key, Val, Ver, {T, OriginalLeftKey, OriginalRightKey, _Versions, KeyO, b, leaf}, EtsTable) ->
    [{_, VerData} | RestT] = T,
    NewVerT = insertToListB(Key, Val, VerData),
    {NewKey, _} = lists:nth(?Order div 2, NewVerT),
    {LeftVers, LeftData, RightVers, RightData} = splitData({[], [], [], []}, [{Ver, NewVerT}] ++ RestT, NewKey),
    {LeftT, RightT} = lists:split(?Order div 2, NewVerT),
    {RightKey, _} = lists:nth(1, RightT),
    {LeftKey, _} = lists:nth(1, LeftT),
    LeftB = {LeftData, OriginalLeftKey, RightKey, LeftVers},
    RightB = {RightData, LeftKey, OriginalRightKey, RightVers},
    ets:delete(EtsTable, KeyO),
    ets:insert(EtsTable, {LeftKey, LeftB}),
    ets:insert(EtsTable, {RightKey, RightB}),
    updateOriginalLeftLeaf(EtsTable, LeftKey, OriginalLeftKey),
    updateOriginalRightLeaf(EtsTable, RightKey, OriginalRightKey),
    {{LeftKey, b, leaf}, {RightKey, b, leaf}, NewKey, r}.

splitData({LeftVers, LeftData, RightVers, RightData}, [], _SplitKey) ->
    {lists:reverse(LeftVers), lists:reverse(LeftData), lists:reverse(RightVers), lists:reverse(RightData)};
splitData({LeftVers, LeftData, RightVers, RightData}, [{Ver, Data} | T], SplitKey) ->
    SplitPoint = findSplitPoint(SplitKey, Data),
    case SplitPoint == 0 of
        true ->
            case compareList(Data, RightData) of
                false ->
                    splitData({LeftVers, LeftData, [Ver] ++ RightVers, [{Ver, Data}] ++ RightData}, T, SplitKey);
                true ->
                    splitData({LeftVers, LeftData, RightVers, RightData}, T, SplitKey)
            end;
        false ->
            case lists:split(SplitPoint, Data) of
                {[], []} ->
                    splitData({LeftVers, LeftData, RightVers, RightData}, T, SplitKey);
                {Left, []} ->
                    case compareList(Left, LeftData) of
                        false ->
                            splitData({[Ver] ++ LeftVers, [{Ver, Left}] ++ LeftData, RightVers, RightData}, T, SplitKey);
                        true ->
                            splitData({LeftVers, LeftData, RightVers, RightData}, T, SplitKey)
                    end;
                {[], Right} ->
                    case compareList(Right, RightData) of
                        false ->
                            splitData({LeftVers, LeftData, [Ver] ++ RightVers, [{Ver,Right}] ++ RightData}, T, SplitKey);
                        true ->
                            splitData({LeftVers, LeftData, RightVers, RightData}, T, SplitKey)
                    end;
                {Left, Right} ->
                    case compareList(Left, LeftData) of
                        false ->
                            NewLeftVers = [Ver] ++ LeftVers,
                            NewLeftData = [{Ver, Left}] ++ LeftData;
                        true ->
                            NewLeftVers = LeftVers,
                            NewLeftData = LeftData
                    end,
                    case compareList(Right, RightData) of
                        false ->
                            NewRightVers = [Ver] ++ RightVers,
                            NewRightData = [{Ver,Right}] ++ RightData;
                        true ->
                            NewRightVers = RightVers,
                            NewRightData = RightData
                    end,

                    splitData({NewLeftVers, NewLeftData, NewRightVers, NewRightData}, T, SplitKey)
            end
    end.
compareList(_List, []) ->
    false;
compareList(List, [{_Ver2, Data2} | _T]) ->
    List =:= Data2.

updateOriginalLeftLeaf(_EtsTable, _NewRightKey, nil) ->
    ok;
updateOriginalLeftLeaf(EtsTable, NewRightKey, OriginalLeftKey) ->
    [{_,{OLT, OLL, _OLR, OLV}}] = ets:lookup(EtsTable, OriginalLeftKey),
    ets:insert(EtsTable, {OriginalLeftKey, {OLT, OLL, NewRightKey, OLV}}).

updateOriginalRightLeaf(_EtsTable, _NewLeftKey, nil) ->
    ok;
updateOriginalRightLeaf(EtsTable, NewLeftKey, OriginalRightKey) ->
    [{_,{ORT, _ORL, ORR, ORV}}] = ets:lookup(EtsTable, OriginalRightKey),
    ets:insert(EtsTable, {OriginalRightKey, {ORT, NewLeftKey, ORR, ORV}}).


splitLeafInRemove({ _DeadL, DeadR, DeadKey},{T, OriginalLeftKey, _OriginalRightKey, _Versions, Key}, NewKey, EtsTable, to_right) ->
    {LeftVers, LeftData, RightVers, RightData} = splitData({[], [], [], []}, T, NewKey),
    [{_, [{RightKey, _} | _]} | _] = RightData,
    [{_, [{LeftKey, _} | _]} | _] = LeftData,
    LeftB = {LeftData, OriginalLeftKey, RightKey, LeftVers},
    RightB = {RightData, LeftKey, DeadR, RightVers},
    ets:delete(EtsTable, DeadKey),
    ets:delete(EtsTable, Key),
    ets:insert(EtsTable, {LeftKey, LeftB}),
    ets:insert(EtsTable, {RightKey, RightB}),

    case OriginalLeftKey of
        nil ->
            ok;
        _ ->
            [{_,{OLT, OLL, _OLR, OLV}}] = ets:lookup(EtsTable, OriginalLeftKey),
            ets:insert(EtsTable, {OriginalLeftKey, {OLT, OLL, LeftKey, OLV}})
    end,

    case DeadR of
        nil ->
            ok;
        _ ->
            [{_,{ORT, _ORL, ORR, ORV}}] = ets:lookup(EtsTable, DeadR),
            ets:insert(EtsTable, {DeadR, {ORT, RightKey, ORR, ORV}})
    end,

    {{LeftKey, b, leaf}, {RightKey, b, leaf}, NewKey, r};

splitLeafInRemove({DeadL, _DeadR, DeadKey},{T, _OriginalLeftKey, OriginalRightKey, _Versions, Key}, NewKey, EtsTable, to_left) ->
    {LeftVers, LeftData, RightVers, RightData} = splitData({[], [], [], []}, T, NewKey),
    [{_, [{RightKey, _} | _]} | _] = RightData,
    [{_, [{LeftKey, _} | _]} | _] = LeftData,
    LeftB = {LeftData, DeadL, RightKey, LeftVers},
    RightB = {RightData, LeftKey, OriginalRightKey, RightVers},
    ets:delete(EtsTable, DeadKey),
    ets:delete(EtsTable, Key),
    ets:insert(EtsTable, {LeftKey, LeftB}),
    ets:insert(EtsTable, {RightKey, RightB}),

    case DeadL of
        nil ->
            ok;
        _ ->
            [{_,{OLT, OLL, _OLR, OLV}}] = ets:lookup(EtsTable, DeadL),
            ets:insert(EtsTable, {DeadL, {OLT, OLL, LeftKey, OLV}})
    end,

    case OriginalRightKey of
        nil ->
            ok;
        _ ->
            [{_,{ORT, _ORL, ORR, ORV}}] = ets:lookup(EtsTable, OriginalRightKey),
            ets:insert(EtsTable, {OriginalRightKey, {ORT, RightKey, ORR, ORV}})
    end,

    {{LeftKey, b, leaf}, {RightKey, b, leaf}, NewKey, r}.

lookup(_Key, _Ver, {nil, b}, _EtsTable) ->
    {};
lookup(Key, Ver, {KeyLf, b, leaf}, EtsTable) ->
    [{_,{T, _LL, _RL, Vers}}] = ets:lookup(EtsTable, KeyLf),
    VerIndex= get_greatest_lower_version(Ver, Vers),
    case VerIndex == 0 of
        true ->
            {};
        false ->
            {_, Data} = lists:nth(VerIndex, T),
            binarySearch(Key, Data)
    end;
lookup(Key, Ver, {L, _R, Key2, _C}, EtsTable) when Key < Key2 ->
    lookup(Key, Ver, L, EtsTable);
lookup(Key, Ver, {L, _R, Key2, _C}, EtsTable) when Key == Key2 ->
    lookup(Key, Ver, L, EtsTable);
lookup(Key, Ver, {_L, R, Key2, _C}, EtsTable) when Key > Key2 ->
    lookup(Key, Ver, R, EtsTable).


getRange(_Min, _Max, Tree, _EtsTable, _MaxVersion) when Tree =:= {nil, b} ->
    [];
getRange(Min, Max, _Tree, _EtsTable, _MaxVersion) when Min > Max ->
    [];
getRange(Min, Max, Tree, EtsTable, MaxVersion) when Min == Max ->
    lookup(Min, MaxVersion, Tree, EtsTable);
getRange(Min, Max, Tree, EtsTable, MaxVersion) ->
    {MinKey, b, leaf} = findLeaf(Min, Tree),
    {MaxKey, b, leaf} = findLeaf(Max, Tree),
    [{_,{MiT, _, MinR, MiVers}}] = ets:lookup(EtsTable, MinKey),
    [{_,{MaT, _, _, MaVers}}] = ets:lookup(EtsTable, MaxKey),
    MinVerIndex= get_greatest_lower_version(MaxVersion, MiVers),
    case MinKey == MaxKey of
        true ->
            case MinVerIndex == 0 of
                true ->
                    GreaterThan = [];
                false ->
                    {_, MiData} = lists:nth(MinVerIndex, MiT),
                    GreaterThan = getGreaterThan(Min, MiData)
            end,
            GreaterThan;
        false ->
            case MinVerIndex == 0 of
                true ->
                    GreaterThan = [];
                false ->
                    {_, MiData} = lists:nth(MinVerIndex, MiT),
                    GreaterThan = getGreaterThan(Min, MiData)
            end,

            MaxVerIndex= get_greatest_lower_version(MaxVersion, MaVers),
            case MaxVerIndex == 0 of
                true ->
                    LessThan = [];
                false ->
                    {_, MaData} = lists:nth(MaxVerIndex, MaT),
                    LessThan = getLessThan(Min, Max, MaData)
            end,
            GreaterThan ++ getNextUntil(MaxKey, MinR, EtsTable, MaxVersion) ++ LessThan
    end.

getNextUntil(Until, Key, _EtsTable, _MaxVersion) when Key == Until ->
    [];
getNextUntil(Until, Key, _EtsTable, _MaxVersion) when Key > Until ->
    [];
getNextUntil(Until, Key, EtsTable, MaxVersion) when Key < Until ->
    [{_,{T, _, NextKey, Vers}}] = ets:lookup(EtsTable, Key),
    VerIndex= get_greatest_lower_version(MaxVersion, Vers),
    case VerIndex == 0 of
        true ->
            getNextUntil(Until, NextKey, EtsTable, MaxVersion);
        false ->
            {_, Data} = lists:nth(VerIndex, T),
            lists:append(Data, getNextUntil(Until, NextKey, EtsTable, MaxVersion))
    end.

findLeaf(_Key, {nil, b}) ->
    nil;
findLeaf(_Key, {_KeyLf, b, leaf} = Leaf) ->
    Leaf;
findLeaf(Key, {L, _R, Key2, _C}) when Key < Key2 ->
    findLeaf(Key, L);
findLeaf(Key, {L, _R, Key2, _C}) when Key == Key2 ->
    findLeaf(Key, L);
findLeaf(Key, {_L, R, Key2, _C}) when Key > Key2 ->
    findLeaf(Key, R).


removeFromTree(_Key, _Val, _Ver, {nil, b}, _Table) ->
    {nil, b};
removeFromTree(Key, Val, Ver,  {_KeyLf, b, leaf} = Leaf, Table) ->
    removeFromLeaf(Key, Val, Ver, Leaf, Table, self);
removeFromTree(Key, Val, Ver,  {{_KeyLf, b, leaf}, _R, Key2, _C} = Node, Table) when Key < Key2 ->
    removeFromLeaf(Key, Val, Ver, Node, Table, left);
removeFromTree(Key, Val, Ver, {L, R, Key2, C}, Table) when Key < Key2 ->
    balance({removeFromTree(Key, Val, Ver, L, Table), R, Key2, C});
removeFromTree(Key, Val, Ver, {{_KeyLf, b, leaf}, _R, Key2, _C} = Node, Table) when Key == Key2 ->
    removeFromLeaf(Key, Val, Ver, Node, Table, left);
removeFromTree(Key, Val, Ver, {L, R, Key2, C}, Table) when Key == Key2 ->
    balance({removeFromTree(Key, Val, Ver, L, Table), R, Key2, C});
removeFromTree(Key, Val, Ver, {_L, {_KeyLf, b, leaf}, Key2, _C} = Node, Table) when Key > Key2 ->
    removeFromLeaf(Key, Val, Ver, Node, Table, right);
removeFromTree(Key, Val, Ver, {L, R, Key2, C}, Table) when Key > Key2 ->
    balance({L, removeFromTree(Key, Val, Ver, R, Table), Key2, C}).


removeFromLeaf(Key, Val, Ver, {LeafKey, b, leaf} = Tree, Table , self) ->
    [{_,{Data, LeafL, LeafR, V}}] = ets:lookup(Table, LeafKey),
    [{LastVer, VerData} | T] = Data,
    case versions_eq(LastVer, Ver) of
        false ->
            case versions_gt(Ver, LastVer) of
                true ->
                    NewData = binaryFindDelete(Key, Val, VerData),
                    case NewData == VerData of
                        true ->
                            Tree;
                        false ->
                            B = {[{Ver, NewData}] ++ [{LastVer, VerData}] ++ T, LeafL, LeafR, [Ver] ++ V},
                            ets:insert(Table, {LeafKey, B}),
                            Tree
                    end;
                false ->
                    Tree
            end;
        true ->
            NewData = binaryFindDelete(Key, Val, VerData),
            B = {[{LastVer, NewData}] ++ T, LeafL, LeafR, V},
            ets:insert(Table, {LeafKey, B}),
            Tree
    end;
%% left -> means left leaf node is the target leaf
removeFromLeaf(Key, Val, Ver, {{LeafKey, b, leaf}, R, _ParentKey, _C} = Tree, Table , left) ->
    [{_,{Data, LeafL, LeafR, V}}] = ets:lookup(Table, LeafKey),
    [{LastVer, VerData} | T] = Data,
    case versions_eq(LastVer, Ver) of
        false ->
            case versions_gt(Ver, LastVer) of
                true ->
                    NewData = binaryFindDelete(Key, Val, VerData),
                    case NewData == VerData of
                        true ->
                            Tree;
                        false ->
                            B = {[{Ver, NewData}] ++ [{LastVer, VerData}] ++ T, LeafL, LeafR, [Ver] ++ V},
                            ets:insert(Table, {LeafKey, B}),
                            case isItUnderflow([{Ver, NewData}] ++ [{LastVer, VerData}] ++ T) of
                                true ->
                                    %% Merge it
                                    %% 1- remove the ParentKey, (It will be removed just by ignoring it)
                                    %% 2- merge the new B (Which is L) to R
                                    merge(Table, LeafKey, {[{Ver, NewData}] ++ [{LastVer, VerData}] ++ T, LeafL, LeafR, V}, R);
                                false ->
                                    Tree
                            end
                    end;
                false ->
                    Tree
            end;
        true ->
            NewData = binaryFindDelete(Key, Val, VerData),
            B = {[{LastVer, NewData}] ++ T, LeafL, LeafR, V},
            ets:insert(Table, {LeafKey, B}),
            case isItUnderflow([{LastVer, NewData}] ++ T) of
                true ->
                    %% Merge it
                    %% 1- remove the ParentKey, (It will be removed just by ignoring it)
                    %% 2- merge the new B (Which is L) to R
                    merge(Table, LeafKey, {[{LastVer, NewData}] ++ T, LeafL, LeafR, V}, R);
                false ->
                    Tree
            end
    end;
%% right -> means right leaf node is the target leaf
removeFromLeaf(Key, Val, Ver, {L, {LeafKey, b, leaf}, _ParentKey, _C} = Tree, Table , right) ->
    [{_,{Data, LeafL, LeafR, V}}] = ets:lookup(Table, LeafKey),
    [{LastVer, VerData} | T] = Data,
    case versions_eq(LastVer, Ver) of
        false ->
            case versions_gt(Ver, LastVer) of
                true ->
                    NewData = binaryFindDelete(Key, Val, VerData),
                    case NewData == VerData of
                        true ->
                            Tree;
                        false ->
                            B = {[{Ver, NewData}] ++ [{LastVer, VerData}] ++ T, LeafL, LeafR, [Ver] ++ V},
                            ets:insert(Table, {LeafKey, B}),
                            case isItUnderflow([{Ver, NewData}] ++ [{LastVer, VerData}] ++ T) of
                                true ->
                                    %% Merge it
                                    %% 1- remove the ParentKey, (It will be removed just by ignoring it)
                                    %% 2- merge the new B (Which is L) to R
                                    merge(Table, LeafKey, {[{Ver, NewData}] ++ [{LastVer, VerData}] ++ T, LeafL, LeafR, V}, L);
                                false ->
                                    Tree
                            end
                    end;
                false ->
                    Tree
            end;
        true ->
            NewData = binaryFindDelete(Key, Val, VerData),
            B = {[{LastVer, NewData}] ++ T, LeafL, LeafR, V},
            ets:insert(Table, {LeafKey, B}),
            case isItUnderflow([{LastVer, NewData}] ++ T) of
                true ->
                    %% Merge it
                    %% 1- remove the ParentKey, (It will be removed just by ignoring it)
                    %% 2- merge the new B (Which is L) to R
                    merge(Table, LeafKey, {[{LastVer, NewData}] ++ T, LeafL, LeafR, V}, L);
                false ->
                    Tree
            end
    end.

merge(Table, Key, {DeadData, DeadL, DeadR, _DeadVers}, {ToLeafKey, b, leaf}) when Key > ToLeafKey ->
    [{_,{ToData, L, R, ToVers}}] = ets:lookup(Table, ToLeafKey),
    {NewToData, NewToVers} = appendData(DeadData, ToData, ToVers),
    case isItOverflow(NewToData) of
        {true, NewParentKey} ->
            splitLeafInRemove({ DeadL, DeadR, Key},{NewToData, L, R, NewToVers, ToLeafKey}, NewParentKey, Table, to_right);
        false ->
            ets:delete(Table, Key),
            case DeadR of
                nil ->
                    ok;
                _ ->
                    [{_,{RData, _RL, RR, RVers}}] = ets:lookup(Table, DeadR),
                    ets:insert(Table, {DeadR, {RData, ToLeafKey, RR, RVers}})
            end,
            ets:insert(Table, {ToLeafKey, {NewToData, L, DeadR, NewToVers}}),
            {ToLeafKey, b, leaf}
    end;
merge(Table, Key, {DeadData, DeadL, DeadR, _DeadVers}, {ToLeafKey, b, leaf}) when Key =< ToLeafKey ->
    [{_,{ToData, L, R, ToVers}}] = ets:lookup(Table, ToLeafKey),
    {NewToData, NewToVers} = prependData(DeadData, ToData, ToVers),
    case isItOverflow(NewToData) of
        {true, NewParentKey} ->
            splitLeafInRemove({ DeadL, DeadR, Key},{NewToData, L, R, NewToVers, ToLeafKey}, NewParentKey, Table, to_left);
        false ->
            ets:delete(Table, Key),
            case DeadL of
                nil ->
                    ok;
                _ ->
                    [{_,{LData, LL, _LR, LVers}}] = ets:lookup(Table, DeadL),
                    ets:insert(Table, {DeadL, {LData, LL, ToLeafKey, LVers}})
            end,
            ets:insert(Table, {ToLeafKey, {NewToData, DeadL, R, NewToVers}}),
            {ToLeafKey, b, leaf}
    end;
merge(Table, Key, DeadLeaf, {L, R, Key2, C}) when Key =< Key2 ->
    balance({merge(Table, Key, DeadLeaf, L), R, Key2, C});
merge(Table, Key, DeadLeaf, {L, R, Key2, C}) when Key > Key2 ->
    balance({L, merge(Table, Key, DeadLeaf, R), Key2, C}).


appendData([], ToData, ToVers) ->
    {ToData, ToVers};
appendData([{FromVer, FromData} | FromT], [], []) ->
    {NewData, NewVers} = appendData(FromT, [], []),
    {[{FromVer, FromData}] ++ NewData, [FromVer]++NewVers};
appendData([{FromVer, FromData} | FromT] = From, [{ToVer, ToData} | ToT] = To, [Ver | T] = ToVers) ->
    case versions_gt(FromVer, Ver) of
        true ->
            {NewData, NewVers} = appendData(FromT, To, ToVers),
            {[{FromVer, ToData ++ FromData}] ++ NewData, [FromVer]++NewVers};
        false ->
            case versions_eq(FromVer, Ver) of
                true ->
                    {NewData, NewVers} = appendData(FromT, ToT, T),
                    {[{ToVer, ToData ++ FromData}] ++ NewData, [Ver]++NewVers};
                false ->
                    {NewData, NewVers} = appendData(From, ToT, T),
                    {[{ToVer, ToData ++ FromData}] ++ NewData, [Ver]++NewVers}
            end
    end.

prependData([], ToData, ToVers) ->
    {ToData, ToVers};
prependData([{FromVer, FromData} | FromT], [], []) ->
    {NewData, NewVers} = prependData(FromT, [], []),
    {[{FromVer, FromData}] ++ NewData, [FromVer]++NewVers};
prependData([{FromVer, FromData} | FromT] = From, [{ToVer, ToData} | ToT] = To, [Ver | T] = ToVers) ->
    case versions_gt(FromVer, Ver) of
        true ->
            {NewData, NewVers} = prependData(FromT, To, ToVers),
            {[{FromVer, FromData ++ ToData}] ++ NewData, [FromVer]++NewVers};
        false ->
            case versions_eq(FromVer, Ver) of
                true ->
                    {NewData, NewVers} = prependData(FromT, ToT, T),
                    {[{ToVer,  FromData ++ ToData}] ++ NewData, [Ver]++NewVers};
                false ->
                    {NewData, NewVers} = prependData(From, ToT, T),
                    {[{ToVer, FromData ++ ToData}] ++ NewData, [Ver]++NewVers}
            end
    end.

isItUnderflow([]) ->
    true;
isItUnderflow([{_, Data} | T]) ->
    case length(Data) >= (?Order div 2) of
        true ->
            false;
        false ->
            isItUnderflow(T)
    end.

isItOverflow([]) ->
    false;
isItOverflow([{_, Data} | T]) ->
    case length(Data) > ?Order of
        true ->
            {NewKey, _} = lists:nth(?Order div 2, Data),
            {true, NewKey};
        false ->
            isItOverflow(T)
    end.

findMostLeftLeafKey({nil, b}) ->
    nil;
findMostLeftLeafKey({KeyLf, b, leaf}) ->
    KeyLf;
findMostLeftLeafKey({L, _R, _Key2, _C}) ->
    findMostLeftLeafKey(L).

getGreaterThan(_Key, []) ->
    [];
getGreaterThan(Key, [{Key2, Val2}]) when Key < Key2 ->
    [{Key2, Val2}];
getGreaterThan(Key, [{Key2, Val2}]) when Key2 == Key ->
    [{Key2, Val2}];
getGreaterThan(Key, [{Key2, _Val2}]) when Key > Key2 ->
    [];
getGreaterThan(Key, L) ->
    N = length(L) div 2,
    {Left, Right} = lists:split(N, L),
    {Nth,_} = lists:nth(N, L),
    case Nth >= Key of
        true ->
            getGreaterThan(Key, Left) ++ Right;
        false ->
            getGreaterThan(Key, Right)
    end.

getLessThan(_Min, _Key, []) ->
    [];
getLessThan(_Min, Key, [{Key2, _Val2}]) when Key < Key2 ->
    [];
getLessThan(_Min, Key, [{Key2, Val2}]) when Key2 == Key ->
    [{Key2, Val2}];
getLessThan(Min, Key, [{Key2, Val2}]) when (Key > Key2) and (Key2 >= Min) ->
    [{Key2, Val2}];
getLessThan(Min, Key, [{Key2, _Val2}]) when (Key > Key2) and (Key2 < Min) ->
    [];
getLessThan(Min, Key, L) ->
    N = length(L) div 2,
    {Left, Right} = lists:split(N, L),
    {Nth,_} = lists:nth(N, L),
    case {Nth >= Key, Nth >= Min} of
        {true, true} ->
            getLessThan(Min, Key, Left);
        {false, true} ->
            Left ++ getLessThan(Min, Key, Right);
        _ -> getLessThan(Min, Key, Right)
    end.


get_greatest_lower_version(_Key, []) ->
    0;
get_greatest_lower_version(Key, [Nth]) ->
    get_greatest_lower_index(Key, [Nth]);
get_greatest_lower_version(Key, L) ->
    case versions_gt(lists:last(L), Key) of
        true ->
            0;
        false ->
            get_greatest_lower_index(Key, L)
    end.

get_greatest_lower_index(_Key, []) ->
    0;
get_greatest_lower_index(Key, [Nth]) ->
    case versions_lt(Nth, Key) or versions_eq(Nth, Key) of
        true ->
            1;
        false ->
            0
    end;
get_greatest_lower_index(Key, L) ->
    N = length(L) div 2,
    {Left, Right} = lists:split(N, L),
    Nth = lists:nth(N, L),
    case Nth < Key of
        true ->
            get_greatest_lower_index(Key, Left);
        false ->
            case Nth > Key of
                false ->
                    N;
                true ->
                    N + get_greatest_lower_index(Key, Right)
            end
    end.

findSplitPoint(_Key, []) ->
    0;
findSplitPoint(Key, [{Nth, _}]) ->
    case Nth =< Key of
        true ->
            1;
        false ->
            0
    end;
findSplitPoint(Key, L) ->
    N = length(L) div 2,
    {Left, Right} = lists:split(N, L),
    {Nth,_} = lists:nth(N, L),
    case Nth < Key of
        true ->
            N + findSplitPoint(Key, Right);
        false ->
            case Nth > Key of
                false ->
                    N;
                true ->
                    findSplitPoint(Key, Left)
            end
    end.

binarySearch(_Key, []) ->
    {};
binarySearch(Key, [{Key2, _Val2}]) when Key2 < Key ->
    {};
binarySearch(Key, [{Key2, Val2}]) when Key2 == Key ->
    {Key2, Val2};
binarySearch(Key, [{Key2, _Val2}]) when Key2 > Key ->
    {};
binarySearch(Key, L) ->
    N = length(L) div 2,
    {Left, Right} = lists:split(N, L),
    {Nth,_} = lists:nth(N, L),
    case Nth >= Key of
        true ->
            binarySearch(Key, Left);
        false ->
            binarySearch(Key, Right)
    end.

insertToListB(Key, Val, []) ->
    [{Key, [Val]}];
insertToListB(Key, Val, [{Key2, Val2}]) when Key2 < Key ->
    [{Key2, Val2}, {Key, [Val]}];
insertToListB(Key, RowId, [{Key2, Val2}]) when Key2 == Key ->
    [{Key2, insertToListSameValue(RowId, Val2)}];
insertToListB(Key, Val, [{Key2, Val2}]) when Key2 > Key ->
    [{Key, [Val]}, {Key2, Val2}];
insertToListB(Key, Val, L) ->
    N = length(L) div 2,
    {Left, Right} = lists:split(N, L),
    {Nth,_} = lists:nth(N, L),
    case Nth >= Key of
        true ->
            insertToListB(Key, Val, Left) ++ Right;
        false ->
            Left ++ insertToListB(Key, Val, Right)
    end.


insertToListSameValue(RowId, []) ->
    [RowId];
insertToListSameValue(RowId, [RowId2 | T]) when RowId2 == RowId ->
    [RowId] ++ T;
insertToListSameValue(RowId, [RowId2 | T]) when RowId2 =/= RowId ->
    [RowId2] ++ insertToListSameValue(RowId, T).


remove(Key, Val, Ver, Tree, Table) ->
    makeRootBlack(removeFromTree(Key, Val, Ver, Tree, Table)).

binaryFindDelete(_Key, _RowId, []) ->
    [];
binaryFindDelete(Key, _RowId, [{Key2, _Val2}]) when Key2 < Key ->
    [{Key2, _Val2}];
binaryFindDelete(Key, RowId, [{Key2, Val2}]) when Key2 == Key ->
    case removeRow(RowId, Val2) of
        [] ->
            [];
        Rows ->
            [{Key2, Rows}]
    end;
binaryFindDelete(Key, _RowId, [{Key2, Val2}]) when Key2 > Key ->
    [{Key2, Val2}];
binaryFindDelete(Key, RowId, L) ->
    N = length(L) div 2,
    {Left, Right} = lists:split(N, L),
    {Nth,_} = lists:nth(N, L),
    case Nth >= Key of
        true ->
            binaryFindDelete(Key, RowId,  Left) ++ Right;
        false ->
            Left ++ binaryFindDelete(Key, RowId, Right)
    end.

removeRow(_RowId, []) ->
    [];
removeRow(RowId, [RowId2 | T]) when RowId2 == RowId ->
    T;
removeRow(RowId, [RowId2 | T])  ->
    [RowId2] ++ removeRow(RowId, T).

makeRootBlack({nil,b}) ->
    {nil,b};
makeRootBlack({Key,_C,leaf}) ->
    {Key,b,leaf};
makeRootBlack({L, R, Key, _C}) ->
    {L, R, Key, b}.


versions_eq(Ver1, Ver2) ->
    vectorclock:eq(Ver1, Ver2).
versions_gt(Ver1, Ver2) ->
    vectorclock:all_dots_greater(Ver1, Ver2).
versions_lt(Ver1, Ver2) ->
    vectorclock:all_dots_smaller(Ver1, Ver2).