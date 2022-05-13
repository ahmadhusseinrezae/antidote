-module(redblackt).

-export([insert/5, insertToListB/3, lookup/4, findLeaf/2, getInThan/3, getRange/5, findMostLeftLeafKey/1,
    findSplitPoint/2, splitMe/3, splitData/3, merge/4, appendData/4, removeFromLeaf/6, removeFromTree/5, remove/5]).
-define(Order, 4).

insert(Key, Val, Ver, Tree, EtsTable) ->
    makeRootBlack(insertTo(Key, Val, Ver, Tree, EtsTable)).

isEqual(A, B) ->
    A == B.

getPayload(EtsTable, Key) ->
    [{_,{ToData, L, R, ToVers}}] = ets:lookup(EtsTable, Key),
    {ToData, L, R, ToVers}.

getHighestItem(Tree) ->
    version_tree:highest_version(Tree).

replaceItemData(_OldData, NewData) ->
    NewData.

insertToItemData(OldList, {NewK, NewV}) ->
    {List, _} = insertToListB(NewK, NewV, OldList),
    List.

insertPayload(EtsTable, Key, Data) ->
    ets:insert(EtsTable, {Key, Data}).

removePayload(EtsTable, Key) ->
    ets:delete(EtsTable, Key).

insertOrReplaceInTree(Item, Data, Tree, NullItem) ->
    version_tree:insert(Item, Data, Tree, fun replaceItemData/2, NullItem).

insertOrReplaceInTreeFun(Item, Data, Tree, Fun, NullItem) ->
    version_tree:insert(Item, Data, Tree, Fun, NullItem).

checkIfLengthHigherThanHalf(Ver, NewVerDataLength, {LeafKey, C}, {NewVersionIndex, LeftLeaf, RightLeaf, VersionLengthIndex}, EtsTable) ->
    case NewVerDataLength >= (?Order div 2) of
        true ->
            NewVersionLengthIndex = insertOrReplaceInTree(Ver, NewVerDataLength, VersionLengthIndex, 0),
            insertPayload(EtsTable, LeafKey, { NewVersionIndex, LeftLeaf, RightLeaf, NewVersionLengthIndex}),
            {LeafKey, C, leaf};
        false ->
            insertPayload(EtsTable, LeafKey, { NewVersionIndex, LeftLeaf, RightLeaf, VersionLengthIndex}),
            {LeafKey, C, leaf}
    end.

insertToRoot(Key, Val, Ver, EtsTable) ->
    {NewVerData, _Updated} = insertToListB(Key, Val, []),
    NewVersionIndex = insertOrReplaceInTree(Ver, NewVerData, nil, []),
    insertPayload(EtsTable, Key, {NewVersionIndex, nil, nil, nil}),
    {Key, b, leaf}.

insertToNewVersion(Key, Val, Ver, {LeafKey, C}, {_LastVer, LastVerData}, {VersionIndex, LeftLeaf, RightLeaf, VersionLengthIndex}, EtsTable) ->
    NewVersionIndex = insertOrReplaceInTreeFun(Ver, {Key, Val}, VersionIndex, fun insertToItemData/2, LastVerData),
    {_, NewVerData} = getHighestItem(NewVersionIndex),

    case length(LastVerData) >= ?Order of
        true ->
            splitLeaf(Key, Val, Ver, LastVerData, VersionIndex, LeftLeaf, RightLeaf, LeafKey, EtsTable);
        false ->
            case isEqual(NewVerData, LastVerData) of
                true ->
                    {LeafKey, C, leaf};
                false ->
                    checkIfLengthHigherThanHalf(Ver, length(NewVerData), {LeafKey, C},
                        {NewVersionIndex, LeftLeaf, RightLeaf, VersionLengthIndex}, EtsTable)
            end
    end.

insertToSameVersion(Key, Val, Ver, {LeafKey, C}, {_LastVer, LastVerData}, {VersionIndex, LeftLeaf, RightLeaf, VersionLengthIndex}, EtsTable) ->
    case length(LastVerData) >= ?Order of
        true ->
            splitLeaf(Key, Val, Ver, LastVerData, VersionIndex, LeftLeaf, RightLeaf, LeafKey, EtsTable);
        false ->
            {NewVerData, Updated} = insertToListB(Key, Val, LastVerData),
            case Updated of
                false ->
                    {LeafKey, C, leaf};
                true ->
                    checkIfLengthHigherThanHalf(Ver, length(NewVerData), {LeafKey, C},
                        {insertOrReplaceInTree(Ver, NewVerData, VersionIndex, []), LeftLeaf, RightLeaf, VersionLengthIndex}, EtsTable)
            end
    end.

insertTo(Key, Val, Ver, {nil, b}, EtsTable) ->
    insertToRoot(Key, Val, Ver, EtsTable);
insertTo(Key, Val, Ver, {LeafKey, C, leaf}, EtsTable) ->
    {VersionIndex, LeftLeaf, RightLeaf, VersionLengthIndex}  = getPayload(EtsTable, LeafKey),
    {LastVer, LastVerData} = getHighestItem(VersionIndex),
    case versions_eq(LastVer, Ver) of
        true ->
            insertToSameVersion(Key, Val, Ver, {LeafKey, C}, {LastVer, LastVerData}, {VersionIndex, LeftLeaf, RightLeaf, VersionLengthIndex}, EtsTable);
        false ->
            case versions_gt(LastVer, Ver) of %% It is now allowed to update old versions (Partial persistent)
                true ->
                    {LeafKey, C, leaf};
                false ->
                    insertToNewVersion(Key, Val, Ver, {LeafKey, C}, {LastVer, LastVerData}, {VersionIndex, LeftLeaf, RightLeaf, VersionLengthIndex}, EtsTable)
            end
    end;
insertTo(Key, Val, Ver, {L, R, Key2, C}, EtsTable) when Key =< Key2 ->
    balance({ insertTo(Key, Val, Ver, L, EtsTable), R, Key2, C });
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

updateVersionLengthIndex(Ver, Data, Index) ->
    NewVerDataLength = length(Data),
    case NewVerDataLength >= (?Order div 2) of
        true ->
            insertOrReplaceInTree(Ver, NewVerDataLength, Index, 0);
        false ->
            Index
    end.

splitVersionData(Ver, VersionData, VersionLenghts, VerstionDataIndex) ->
    case compareList(VersionData, VerstionDataIndex) of
        false ->
            {updateVersionLengthIndex(Ver, VersionData, VersionLenghts),
                insertOrReplaceInTree(Ver, VersionData, VerstionDataIndex, [])};
        true ->
            {VersionLenghts, VerstionDataIndex}
    end.

splitLeaf(Key, Val, Ver, LastVerData, VersionIndex, LeftLeaf, RightLeaf, LeafKey, EtsTable) ->
    {NewVerData, _Updated} = insertToListB(Key, Val, LastVerData),
    NewVersionIndex = insertOrReplaceInTree(Ver, NewVerData, VersionIndex, []),
    {NewInternalNodeKey, _} = lists:nth(?Order div 2, NewVerData),
    {[{LeftKey, _} | _], [{RightKey, _} | _]} = lists:split(?Order div 2, NewVerData),
    {LeftVers, LeftData, RightVers, RightData} = splitData({nil, nil, nil, nil}, version_tree:to_list(NewVersionIndex), NewInternalNodeKey),
    removePayload(EtsTable, LeafKey),
    insertPayload(EtsTable, LeftKey, {LeftData, LeftLeaf, RightKey, LeftVers}),
    insertPayload(EtsTable, RightKey, {RightData, LeftKey, RightLeaf, RightVers}),
    updatePointerToRight(EtsTable, LeftKey, LeftLeaf),
    updatePointerToLeft(EtsTable, RightKey, RightLeaf),
    {{LeftKey, b, leaf}, {RightKey, b, leaf}, NewInternalNodeKey, r}.

splitData({LeftVers, LeftData, RightVers, RightData}, [], _SplitKey) ->
    {LeftVers, LeftData, RightVers, RightData};
splitData({LeftVers, LeftData, RightVers, RightData}, [{Ver, Data} | T], SplitKey) ->
    SplitPoint = findSplitPoint(SplitKey, Data),
    case SplitPoint == 0 of
        true ->
            {NewRightVers, NewRightData} = splitVersionData(Ver, Data, RightVers, RightData),
            splitData({LeftVers, LeftData, NewRightVers, NewRightData}, T, SplitKey);
        false ->
            case lists:split(SplitPoint, Data) of
                {[], []} ->
                    splitData({LeftVers, LeftData, RightVers, RightData}, T, SplitKey);
                {Left, []} ->
                    {NewLeftVers, NewLeftData} = splitVersionData(Ver, Left, LeftVers, LeftData),
                    splitData({NewLeftVers, NewLeftData, RightVers, RightData}, T, SplitKey);
                {[], Right} ->
                    {NewRightVers, NewRightData} = splitVersionData(Ver, Right, RightVers, RightData),
                    splitData({LeftVers, LeftData, NewRightVers, NewRightData}, T, SplitKey);
                {Left, Right} ->
                    {NewLeftVers, NewLeftData} = splitVersionData(Ver, Left, LeftVers, LeftData),
                    {NewRightVers, NewRightData} = splitVersionData(Ver, Right, RightVers, RightData),
                    splitData({NewLeftVers, NewLeftData, NewRightVers, NewRightData}, T, SplitKey)
            end
    end.

compareList(List, Index) ->
    case getHighestItem(Index) of
        nil ->
            false;
        {_, Data} -> List =:= Data
    end.

updatePointerToRight(_EtsTable, _NewRightKey, nil) ->
    ok;
updatePointerToRight(EtsTable, NewRightKey, LeafKey) ->
    {VersionIndex, LeftLeaf, _RightLeaf, VersionLengthIndex} = getPayload(EtsTable, LeafKey),
    insertPayload(EtsTable, LeafKey, {VersionIndex, LeftLeaf, NewRightKey, VersionLengthIndex}).

updatePointerToLeft(_EtsTable, _NewLeftKey, nil) ->
    ok;
updatePointerToLeft(EtsTable, NewLeftKey, LeafKey) ->
    {VersionIndex, _LeftLeaf, RightLeaf, VersionLengthIndex} = getPayload(EtsTable, LeafKey),
    ets:insert(EtsTable, {LeafKey, {VersionIndex, NewLeftKey, RightLeaf, VersionLengthIndex}}).

splitLeafInRemoval(DeadL, DeadR, DeadKey,T, OriginalLeftKey, OriginalRightKey, _Versions, Key, NewKey, EtsTable, LeftOrRight) ->
    {LeftVers, LeftData, RightVers, RightData} = splitData({nil, nil, nil, nil}, version_tree:to_list(T), NewKey),
    {_, [{RightKey, _} | _]} = version_tree:lowest_version(RightData),
    {_, [{LeftKey, _} | _]} = version_tree:lowest_version(LeftData),
    {LeftB, RightB, LeftLeafToUpdate, RightLeafToUpdate} =
        getLeavesPayload({DeadL, DeadR, LeftVers, LeftData, LeftKey, RightVers, RightData, RightKey},
            {OriginalLeftKey, OriginalRightKey}, LeftOrRight),
    removePayload(EtsTable, DeadKey),
    removePayload(EtsTable, Key),
    insertPayload(EtsTable, LeftKey, LeftB),
    insertPayload(EtsTable, RightKey, RightB),
    updatePointerToRight(EtsTable, LeftKey, LeftLeafToUpdate),
    updatePointerToLeft(EtsTable, RightKey, RightLeafToUpdate),
    {{LeftKey, b, leaf}, {RightKey, b, leaf}, NewKey, r}.

getLeavesPayload({_DeadLeafLeft, DeadLeafRight, LeftVers, LeftData, LeftKey, RightVers, RightData, RightKey},{OriginalLeftKey, _OriginalRightKey}, to_right) ->
    LeftPayload = {LeftData, OriginalLeftKey, RightKey, LeftVers},
    RightPayload = {RightData, LeftKey, DeadLeafRight, RightVers},
    {LeftPayload, RightPayload, OriginalLeftKey, DeadLeafRight};

getLeavesPayload({DeadLeafLeft, _DeadLeafRight, LeftVers, LeftData, LeftKey, RightVers, RightData, RightKey},{_OriginalLeftKey, OriginalRightKey}, to_left) ->
    LeftPayload = {LeftData, DeadLeafLeft, RightKey, LeftVers},
    RightPayload = {RightData, LeftKey, OriginalRightKey, RightVers},
    {LeftPayload, RightPayload, DeadLeafLeft, OriginalRightKey}.

lookup(_Key, _Ver, {nil, b}, _EtsTable) ->
    {};
lookup(Key, Ver, {KeyLf, b, leaf}, EtsTable) ->
    {Data, _LL, _RL, _Vers} = getPayload(EtsTable, KeyLf),
    case version_tree:get_glv_data(Ver, Data) of
        nil ->
            {};
        {_, Res} ->
            binarySearch(Key, Res)
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
    [lookup(Min, MaxVersion, Tree, EtsTable)];
getRange(Min, Max, Tree, EtsTable, MaxVersion) ->
    {MinKey, b, leaf} = findLeaf(Min, Tree),
    {MaxKey, b, leaf} = findLeaf(Max, Tree),
    {MiT, _, MinR, _MiVers} = getPayload(EtsTable, MinKey),
    {MaT, _, _, _MaVers} = getPayload(EtsTable, MaxKey),
    case MinKey == MaxKey of
        true ->
            getGreaterThanFromVersion(MaxVersion, Min, Max, MiT);
        false ->
            GreaterThan = getGreaterThanFromVersion(MaxVersion, Min, Max, MiT),
            LessThan = getLessThanFromVersion(MaxVersion, Min, Max, MaT),
            GreaterThan ++ getNextUntil(MaxKey, MinR, EtsTable, MaxVersion) ++ LessThan
    end.
getGreaterThanFromVersion(Version,  MinimumKey, MaximumKey, VersionIndex) ->
    case version_tree:get_glv_data(Version, VersionIndex) of
        nil ->
            [];
        {_, GLVVersionData} ->
            getInThan( MinimumKey, MaximumKey, GLVVersionData)
    end.

getLessThanFromVersion(Version, MinimumKey, MaximumKey, VersionIndex) ->
    case version_tree:get_glv_data(Version, VersionIndex) of
        nil ->
            [];
        {_, GLVVersionData} ->
            getInThan(MinimumKey, MaximumKey, GLVVersionData)
    end.

getNextUntil(Until, Key, _EtsTable, _MaxVersion) when Key == Until ->
    [];
getNextUntil(Until, Key, _EtsTable, _MaxVersion) when Key > Until ->
    [];
getNextUntil(Until, Key, EtsTable, MaxVersion) when Key < Until ->
    {T, _, NextKey, _Vers} = getPayload(EtsTable, Key),
    case version_tree:get_glv_data(MaxVersion, T) of
        nil ->
            [];
        {_, Data} ->
            Data ++ getNextUntil(Until, NextKey, EtsTable, MaxVersion)
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

removeFromSameVersion(Key, Val, Ver, VerData, LeafL, LeafKey, LeafR, Data,  VersionLengthIndex, Table) ->
    NewData = binaryFindDelete(Key, Val, VerData),
    NewVerDataLength = length(NewData),
    case length(VerData) >= (?Order div 2) of
        true ->
            case NewVerDataLength < (?Order div 2) of
                true ->
                    NewVersionLengthIndex = version_tree:remove(Ver, VersionLengthIndex);
                false ->
                    NewVersionLengthIndex = insertOrReplaceInTree(Ver, NewVerDataLength, VersionLengthIndex, 0)
            end;
        false ->
            NewVersionLengthIndex = VersionLengthIndex
    end,
    NewRightVersionIndex = insertOrReplaceInTree(Ver, NewData, Data, []),
    B = {NewRightVersionIndex, LeafL, LeafR, NewVersionLengthIndex},
    ets:insert(Table, {LeafKey, B}),
    {NewVersionLengthIndex, NewRightVersionIndex}.

removeAndInsertToNewVersion(NewData, Ver, VerData, LeafL, LeafKey, LeafR, Data,  VersionLengthIndex, Table) ->
    NewVerDataLength = length(NewData),
    case length(VerData) >= (?Order div 2) of
        true ->
            case NewVerDataLength < (?Order div 2) of
                true ->
                    NewVersionLengthIndex = version_tree:remove(Ver, VersionLengthIndex);
                false ->
                    NewVersionLengthIndex = insertOrReplaceInTree(Ver, NewVerDataLength, VersionLengthIndex, 0)
            end;
        false ->
            NewVersionLengthIndex = VersionLengthIndex
    end,
    NewRightVersionIndex = insertOrReplaceInTree(Ver, NewData, Data, []),
    Payload = {NewRightVersionIndex, LeafL, LeafR, NewVersionLengthIndex},
    insertPayload(Table, LeafKey, Payload),
    {NewVersionLengthIndex, NewRightVersionIndex}.

removeAndMergeToNewVersion(Key, Val, Ver, VerData, LeafL, LeafKey, LeafR, Data,  VersionLengthIndex, SubTreeToMerge, Tree, Table) ->
    NewData = binaryFindDelete(Key, Val, VerData),
    case isEqual(NewData, VerData) of
        true ->
            Tree;
        false ->
            {NewVersionLengthIndex, NewRightVersionIndex} =
                removeAndInsertToNewVersion(NewData, Ver, VerData, LeafL, LeafKey, LeafR, Data,  VersionLengthIndex, Table),
            checkIfIsUnderflow(NewRightVersionIndex, NewVersionLengthIndex, LeafKey, LeafL, LeafR, SubTreeToMerge,  Tree, Table)
    end.

checkIfIsUnderflow(VersionIndex, NewVersionLengthIndex, LeafKey, LeftLeafKey, RightLeafKey, ToSubTree,  Tree, Table) ->
    case isItUnderflow(NewVersionLengthIndex) of
        true ->
            merge(Table, LeafKey, {VersionIndex, LeftLeafKey, RightLeafKey, nil}, ToSubTree);
        false ->
            Tree
    end.
removeIt(Key, Val, Ver, LeafKey, SubTree, Tree, Table) ->
    {Data, LeafL, LeafR, V} = getPayload(Table, LeafKey),
    {LastVer, VerData} = getHighestItem(Data),
    case versions_eq(LastVer, Ver) of
        false ->
            case versions_gt(Ver, LastVer) of
                true ->
                    removeAndMergeToNewVersion(Key, Val, Ver, VerData, LeafL, LeafKey, LeafR, Data,  V, SubTree, Tree, Table);
                false ->
                    Tree
            end;
        true ->
            {NewVersionLengthIndex, NewRightVersionIndex} =
                removeFromSameVersion(Key, Val, Ver, VerData, LeafL, LeafKey, LeafR, Data,  V, Table),
            checkIfIsUnderflow(NewRightVersionIndex, NewVersionLengthIndex, LeafKey, LeafL, LeafR, SubTree,  Tree, Table)
    end.

removeFromTree(_Key, _Val, _Ver, {nil, b}, _Table) ->
    {nil, b};
removeFromTree(Key, Val, Ver,  {_KeyLf, b, leaf} = Leaf, Table) ->
    removeFromLeaf(Key, Val, Ver, Leaf, Table, self);
removeFromTree(Key, Val, Ver,  {{_KeyLf, b, leaf}, _R, Key2, _C} = Node, Table) when Key =< Key2 ->
    removeFromLeaf(Key, Val, Ver, Node, Table, left);
removeFromTree(Key, Val, Ver, {_L, {_KeyLf, b, leaf}, Key2, _C} = Node, Table) when Key > Key2 ->
    removeFromLeaf(Key, Val, Ver, Node, Table, right);
removeFromTree(Key, Val, Ver, {L, R, Key2, C}, Table) when Key =< Key2 ->
    balance({removeFromTree(Key, Val, Ver, L, Table), R, Key2, C});
removeFromTree(Key, Val, Ver, {L, R, Key2, C}, Table) when Key > Key2 ->
    balance({L, removeFromTree(Key, Val, Ver, R, Table), Key2, C}).

removeFromLeaf(Key, Val, Ver, {LeafKey, b, leaf} = Tree, Table , self) ->
    {Data, LeafL, LeafR, V} = getPayload(Table, LeafKey),
    {LastVer, VerData} = getHighestItem(Data),
    case versions_eq(LastVer, Ver) of
        false ->
            case versions_gt(Ver, LastVer) of
                true ->
                    NewData = binaryFindDelete(Key, Val, VerData),
                    case isEqual(NewData, VerData) of
                        true ->
                            Tree;
                        false ->
                            removeAndInsertToNewVersion(NewData, Ver, VerData, LeafL, LeafKey, LeafR, Data,  V, Table),
                            Tree
                    end;
                false ->
                    Tree
            end;
        true ->
            removeFromSameVersion(Key, Val, Ver, VerData, LeafL, LeafKey, LeafR, Data,  V, Table),
            Tree
    end;
%% left -> means left leaf node is the target leaf
removeFromLeaf(Key, Val, Ver, {{LeafKey, b, leaf}, R, _ParentKey, _C} = Tree, Table , left) ->
    removeIt(Key, Val, Ver, LeafKey, R, Tree, Table);
%% right -> means right leaf node is the target leaf
removeFromLeaf(Key, Val, Ver, {L, {LeafKey, b, leaf}, _ParentKey, _C} = Tree, Table , right) ->
    removeIt(Key, Val, Ver, LeafKey, L, Tree, Table).

merge(Table, Key, {DeadData, DeadL, DeadR, _DeadVers}, {ToLeafKey, b, leaf}) when Key > ToLeafKey ->
    {ToData, L, R, ToVers} = getPayload(Table, ToLeafKey),
    {NewToD, NewToV, IsOverflow, NewParentKey} = append(DeadData, ToData, ToVers),
    {NewToData, NewToVers} = constructTree(NewToD, NewToV),
    case IsOverflow of
        true ->
            splitLeafInRemoval(DeadL, DeadR, Key, NewToData, L, R, NewToVers, ToLeafKey, NewParentKey, Table, to_right);
        false ->
            removePayload(Table, Key),
            updatePointerToLeft(Table, ToLeafKey, DeadR),
            insertPayload(Table, ToLeafKey, {NewToData, L, DeadR, NewToVers}),
            {ToLeafKey, b, leaf}
    end;
merge(Table, Key, {DeadData, DeadL, DeadR, _DeadVers}, {ToLeafKey, b, leaf}) when Key =< ToLeafKey ->
    {ToData, L, R, ToVers} = getPayload(Table, ToLeafKey),
    {NewToD, NewToV, IsOverflow, NewParentKey} = prepend(DeadData, ToData, ToVers),
    {NewToData, NewToVers} = constructTree(NewToD, NewToV),
    case IsOverflow of
        true ->
            splitLeafInRemoval(DeadL, DeadR, Key, NewToData, L, R, NewToVers, ToLeafKey, NewParentKey, Table, to_left);
        false ->
            removePayload(Table, Key),
            updatePointerToRight(Table, ToLeafKey, DeadL),
            insertPayload(Table, ToLeafKey, {NewToData, DeadL, R, NewToVers}),
            {ToLeafKey, b, leaf}
    end;
merge(Table, Key, DeadLeaf, {L, R, Key2, C}) when Key =< Key2 ->
    balance({merge(Table, Key, DeadLeaf, L), R, Key2, C});
merge(Table, Key, DeadLeaf, {L, R, Key2, C}) when Key > Key2 ->
    balance({L, merge(Table, Key, DeadLeaf, R), Key2, C}).

constructTree(DataIndex, VersionLength) ->
    {version_tree:from_list(DataIndex, fun replaceItemData/2, []),
        version_tree:from_list(VersionLength, fun replaceItemData/2, 0)}.

append(DeadData, ToData, ToVers) ->
    appendData(
        lists:reverse(version_tree:to_list(DeadData)),
        lists:reverse(version_tree:to_list(ToData)),
        lists:reverse(version_tree:to_list(ToVers)), false).
prepend(DeadData, ToData, ToVers) ->
    prependData(
        lists:reverse(version_tree:to_list(DeadData)),
        lists:reverse(version_tree:to_list(ToData)),
        lists:reverse( version_tree:to_list(ToVers)), false).

updateVersionLengthList(Version, VersionLength, VersionList) ->
    case VersionLength >= (?Order div 2) of
        true ->
            [{Version, VersionLength}] ++ VersionList;
        false ->
            VersionList
    end.

getSplitKey(VersionData, VersionLength, OldSplitKey) ->
    case VersionLength > ?Order of
        true ->
            {NewKey, _} = lists:nth(?Order div 2, VersionData),
            NewKey;
        false ->
            OldSplitKey
    end.

updateLists(Version, VersionData, SplitKey, VersionDataList, VersionLengthList, IsOverflow) ->
    NewVerLength = length(VersionData),
    NewVersionLength = updateVersionLengthList(Version, NewVerLength, VersionLengthList),
    NewSplitKey = getSplitKey(VersionData, NewVerLength, SplitKey),
    {[{Version, VersionData}] ++ VersionDataList, NewVersionLength, ((NewVerLength > ?Order) or IsOverflow), NewSplitKey}.


appendData(From, To, ToVers, IsOverflow) ->
    joinData(From, To, ToVers, IsOverflow, fun joinAppend/2).

prependData(From, To, ToVers, IsOverflow) ->
    joinData(From, To, ToVers, IsOverflow, fun joinPrepend/2).

joinData([], ToData, ToVers, IsOverflow, _JoinFun) ->
    {ToData, ToVers, IsOverflow, 0};
joinData([{FromVer, FromData} | FromT], [], [], IsOverflow, JoinFun) ->
    {NewData, NewVers, NewIsOverflow, SplitKey} = joinData(FromT, [], [], IsOverflow, JoinFun),
    updateLists(FromVer, FromData, SplitKey, NewData, NewVers, NewIsOverflow);
joinData([{FromVer, FromData} | FromT] = From, [{ToVer, ToData} | ToT] = To, ToVers, IsOverflow, JoinFun) ->
    case versions_gt(FromVer, ToVer) of
        true ->
            {NewData, NewVers, NewIsOverflow, SplitKey} = joinData(FromT, To, ToVers, IsOverflow, JoinFun),
            updateLists(FromVer, JoinFun(FromData, ToData), SplitKey, NewData, NewVers, NewIsOverflow);
        false ->
            case versions_eq(FromVer, ToVer) of
                true ->
                    {NewData, NewVers, NewIsOverflow, SplitKey} = joinData(FromT, ToT, getVersionTail(ToVers), IsOverflow, JoinFun),
                    updateLists(ToVer, JoinFun(FromData, ToData), SplitKey, NewData, NewVers, NewIsOverflow);
                false ->
                    {NewData, NewVers, NewIsOverflow, SplitKey} = joinData(From, ToT, getVersionTail(ToVers), IsOverflow, JoinFun),
                    updateLists(ToVer, JoinFun(FromData, ToData), SplitKey, NewData, NewVers, NewIsOverflow)
            end
    end.

joinAppend(A , B) ->
    B ++ A.
joinPrepend(A , B) ->
    A ++ B.

getVersionTail([]) ->
    [];
getVersionTail([{_Ver, _} | T]) ->
    T.

isItUnderflow(VersionLengthIndex) ->
    version_tree:is_empty(VersionLengthIndex).

findMostLeftLeafKey({nil, b}) ->
    nil;
findMostLeftLeafKey({KeyLf, b, leaf}) ->
    KeyLf;
findMostLeftLeafKey({L, _R, _Key2, _C}) ->
    findMostLeftLeafKey(L).

%%getGreaterThan(_Key, []) ->
%%    [];
%%getGreaterThan(Key, [{Key2, Val2}]) when Key < Key2 ->
%%    [{Key2, Val2}];
%%getGreaterThan(Key, [{Key2, Val2}]) when Key2 == Key ->
%%    [{Key2, Val2}];
%%getGreaterThan(Key, [{Key2, _Val2}]) when Key > Key2 ->
%%    [];
%%getGreaterThan(Key, [{Key2, _Val2} | T] = List) ->
%%    case Key =< Key2 of
%%        true ->
%%            List;
%%        false ->
%%            getGreaterThan(Key, T)
%%    end.

%%getGreaterThan(_Key, _Max, []) ->
%%    [];
%%getGreaterThan(Key, _Max, [{Key2, _Val2}]) when Key > Key2 ->
%%    [];
%%getGreaterThan(Key, _Max, [{Key2, Val2}]) when Key2 == Key ->
%%    [{Key2, Val2}];
%%getGreaterThan(Key, Max, [{Key2, Val2}]) when (Key =< Key2) and (Key2 =< Max) ->
%%    [{Key2, Val2}];
%%getGreaterThan(Key, Max, [{Key2, _Val2}]) when (Key < Key2) and (Max < Key2) ->
%%    [];
%%getGreaterThan(Key, Max, [{Key2, _Val2} | T]) ->
%%    case (Key =< Key2) and (Key2 =< Max) of
%%        true ->
%%            [{Key2, _Val2}] ++ getGreaterThan(Key, Max, T);
%%        false ->
%%            []
%%    end.

getInThan(_Min, _Max, []) ->
    [];
getInThan(Min, Max, [{Key2, Val2}]) when (Min =< Key2) and (Key2 =< Max) ->
    [{Key2, Val2}];
getInThan(_Min, _Max, [{_Key2, _Val2}]) ->
    [];
getInThan(Min, Max, [{Key2, Val2} | T]) ->
    case Key2 =< Max of
        true ->
            case Min =< Key2 of
                true ->
                    [{Key2, Val2}] ++ getInThan(Min, Max, T);
                false ->
                    getInThan(Min, Max, T)
            end;
        false ->
            []
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


splitMe({Left, Right}, _Key, []) ->
    {Left, Right};
splitMe({Left, Right}, Key, [{Nth, Data} | T]) ->
    case Nth =< Key of
        true ->
            splitMe({[{Nth, Data}] ++ Left, Right}, Key, T);
        false ->
            splitMe({Left, [{Nth, Data}] ++ Right}, Key, T)
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
    {[{Key, [Val]}], true};
insertToListB(Key, Val, [{Key2, Val2}]) when Key2 < Key ->
    {[{Key2, Val2}, {Key, [Val]}], true};
insertToListB(Key, RowId, [{Key2, Val2}]) when Key2 == Key ->
    {NewVal, Updated} = insertToListSameValue(RowId, Val2),
    {[{Key2, NewVal}], Updated};
insertToListB(Key, Val, [{Key2, Val2}]) when Key2 > Key ->
    {[{Key, [Val]}, {Key2, Val2}], true};
insertToListB(Key, Val, L) ->
    N = length(L) div 2,
    {Left, Right} = lists:split(N, L),
    {Nth,_} = lists:nth(N, L),
    case Nth >= Key of
        true ->
            {NewLeft, Updated} = insertToListB(Key, Val, Left),
            {NewLeft ++ Right, Updated};
        false ->
            {NewRight, Updated} = insertToListB(Key, Val, Right),
            {Left ++ NewRight, Updated}
    end.


insertToListSameValue(RowId, []) ->
    {[RowId], true};
insertToListSameValue(RowId, [RowId2 | T]) when RowId2 == RowId ->
    {[RowId] ++ T, false};
insertToListSameValue(RowId, [RowId2 | T]) when RowId2 =/= RowId ->
    {NewTail, Updated} = insertToListSameValue(RowId, T),
    {[RowId2] ++ NewTail, Updated}.


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
    vectorclock:gt(Ver1, Ver2).
%%versions_lt(Ver1, Ver2) ->
%%    vectorclock:all_dots_smaller(Ver1, Ver2).



%%/** **/
