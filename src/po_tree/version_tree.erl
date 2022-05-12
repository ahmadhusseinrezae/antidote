-module(version_tree).

-export([insert/5, lookup/2, get_glv_data/2, to_list/1, from_list/3, remove/2, is_empty/1, highest_version/1, lowest_version/1, versions_lt/2, versions_eq/2, versions_gt/2]).
-define(Order, 4).

insert(Ver, Data, Tree, DataConst, NilElm) ->
    makeRootBlack(insertTo(Ver, Data, Tree, DataConst, NilElm)).

insertTo(Ver, Data, nil, DataConst, NilElm) ->
    {Ver, DataConst(NilElm, Data), nil, nil, r};

insertTo(Ver, Data, {Ver1, Data1, Left, Right, C}, DataConst, NilElm) ->
    case versions_eq(Ver, Ver1) of
        true ->
            {Ver1, DataConst(Data1, Data), Left, Right, C};
        false ->
            case versions_lt(Ver, Ver1) of
                true ->
                    balance({Ver1, Data1, insertTo(Ver, Data, Left, DataConst, NilElm), Right, C});
                false ->
                    balance({Ver1, Data1, Left, insertTo(Ver, Data, Right, DataConst, NilElm), C})
            end
    end.

balance({V, D, {V2, D2, {V3, D3, L3 , R3, r}, R2, r}, R, b}) ->
    {V2, D2, {V3, D3, L3 , R3, b}, {V, D, R2, R, b}, r};
balance({V, D, {V2, D2, L2, {V3, D3, L3 , R3, r}, r}, R, b}) ->
    {V3, D3, {V2, D2, L2 , L3, b}, {V, D, R3, R, b}, r};
balance({V, D, L, {V2, D2, L2, {V3, D3, L3 , R3, r}, r}, b}) ->
    {V2, D2, {V, D, L , L2, b}, {V3, D3, L3, R3, b}, r};
balance({V, D, L, {V2, D2, {V3, D3, L3 , R3, r}, R2, r}, b}) ->
    {V3, D3, {V, D, L , L3, b}, {V2, D2, R3, R2, b}, r};
balance({V, D, L, {V2, D2, {V3, D3, L3 , R3, r}, R2, r}, bb}) ->
    {V3, D3, {V, D, L , L3, b}, {V2, D2, R3, R2, b}, b};
balance({V, D, {V2, D2, L2, {V3, D3, L3 , R3, r}, r}, R, bb}) ->
    {V3, D3, {V2, D2, L2, L3, b}, {V, D, R3, R, b}, b};
balance({V, D, L, R, C}) ->
    {V, D, L, R, C}.

lookup(_Ver, nil) ->
    nil;
lookup(Ver, {V, D, L, R, _C}) ->
    case versions_eq(Ver, V) of
        true ->
            D;
        false ->
            case versions_lt(Ver, V) of
                true ->
                    lookup(Ver, L);
                false ->
                    lookup(Ver, R)
            end
    end.

get_glv_data(_Ver, nil) ->
    nil;
get_glv_data(Ver, {V, D, L, R, _C}) ->
    case versions_eq(Ver, V) of
        true ->
            {V, D};
        false ->
            case versions_lt(Ver, V) of
                true ->
                    get_glv_data(Ver, L);
                false ->
                    case R of
                        nil ->
                            {V, D};
                        _ ->
                            get_glv_data(Ver, R)
                    end
            end
    end.

is_empty(nil_nil) ->
    true;
is_empty(nil) ->
    true;
is_empty({_, _, _, _, _}) ->
    false.

highest_version(nil) ->
    nil;
highest_version({V, D, _L, nil, _C}) ->
    {V, D};
highest_version({_V, _D, _L, R, _C}) ->
    highest_version(R).

lowest_version(nil) ->
    nil;
lowest_version({V, D, nil, _R, _C}) ->
    {V, D};
lowest_version({_V, _D, L, _R, _C}) ->
    lowest_version(L).


to_list(nil) ->
    [];
to_list({V, D, L, R, _C}) ->
    to_list(L) ++ [{V, D}] ++ to_list(R).


from_list(List, DataConst, NilElm) ->
    insert_from_list(List, nil, DataConst , NilElm).

insert_from_list([], Tree, _DataConst, _NilElm)->
    Tree;
insert_from_list([{V, D} | T], Tree, DataConst, NilElm) ->
    insert_from_list(T, insert(V, D, Tree, DataConst, NilElm), DataConst, NilElm).



remove(Ver, T) ->
    case delete(Ver, make_red(T)) of
        nil_nil ->
            nil;
        Res -> Res
    end.


delete(_Ver, nil) ->
    nil;
delete(Ver, {V, D, nil, nil, r}) ->
    case versions_eq(Ver, V) of
        true ->
            nil;
        false ->
            {V, D, nil, nil, r}
    end;
delete(Ver, {V, D, nil, nil, b}) ->
    case versions_eq(Ver, V) of
        true ->
            nil_nil;
        false ->
            {V, D, nil, nil, r}
    end;

delete(Ver, {V, D, {Vl, Dl, nil, nil, r}, nil, b}) ->
    case versions_eq(Ver, V) of
        true ->
            {Vl, Dl, nil, nil, b};
        false ->
            case versions_lt(Ver, V) of
                true ->
                    {V, D, delete(Ver, {Vl, Dl, nil, nil, r}), nil, b};
                false ->
                    {V, D, {Vl, Dl, nil, nil, r}, nil, b}
            end
    end;

delete(Ver, {V, D, L, R, C}) ->
    case versions_eq(Ver, V) of
        true ->
            {{NewVer, NewData}, NewRightSubTree} = min_del(R),
            rotate({NewVer, NewData, L, NewRightSubTree, C});
        false ->
            case versions_lt(Ver, V) of
                true ->
                    rotate({V, D, delete(Ver, L), R, C});
                false ->
                    rotate({V, D, L, delete(Ver, R), C})
            end
    end.


rotate({V, D, {Vl, Dl, Ll, Rl, bb}, {Vr, Dr, Lr, Rr, b}, r}) ->
    balance({Vr, Dr, {V, D,{Vl, Dl, Ll, Rl, b}, Lr, r}, Rr, b});
rotate({V, D, nil_nil, {Vr, Dr, Lr, Rr, b}, r}) ->
    balance({Vr, Dr, {V, D, nil, Lr, r}, Rr, b});
rotate({V, D, {Vl, Dl, Ll, Rl, b}, {Vr, Dr, Lr, Rr, bb}, r}) ->
    balance({Vl, Dl, Ll, {V, D, Rl, {Vr, Dr, Lr, Rr, b},r}, b});
rotate({V, D, {Vl, Dl, Ll, Rl, b}, nil_nil, r}) ->
    balance({Vl, Dl, Ll, {V, D, Rl, nil, r}, b});
rotate({V, D, {Vl, Dl, Ll, Rl, bb}, {Vr, Dr, Lr, Rr, b}, b}) ->
    balance({Vr, Dr, {V, D,{Vl, Dl, Ll, Rl, b}, Lr, r}, Rr, bb});
rotate({V, D, nil_nil, {Vr, Dr, Lr, Rr, b}, b}) ->
    balance({Vr, Dr, {V, D, nil, Lr, r}, Rr, bb});
rotate({V, D, {Vl, Dl, Ll, Rl, b}, {Vr, Dr, Lr, Rr, bb}, b}) ->
    balance({Vl, Dl, Ll, {V, D, Rl, {Vr, Dr, Lr, Rr, b},r}, bb});
rotate({V, D, {Vl, Dl, Ll, Rl, b}, nil_nil, b}) ->
    balance({Vl, Dl, Ll, {V, D, Rl, nil, r}, bb});
rotate({V, D, {Vl, Dl, Ll, Rl, bb}, {Vr, Dr, {Vrl, Drl, Lrl, Rrl, b}, Rr, r}, b}) ->
    {Vr, Dr, balance({Vrl, Drl, {V, D, {Vl, Dl, Ll, Rl, b}, Lrl, r}, Rrl, b}), Rr, b};
rotate({V, D, nil_nil, {Vr, Dr, {Vrl, Drl, Lrl, Rrl, b}, Rr, r}, b}) ->
    {Vr, Dr, balance({Vrl, Drl, {V, D, nil, Lrl, r}, Rrl, b}), Rr, b};
rotate({V, D, {Vl, Dl, Ll, {Vlr, Dlr, Llr, Rlr, b}, r}, {Vr, Dr, Lr, Rr, bb}, b}) ->
    {Vl, Dl, Ll, balance({Vlr, Dlr, Llr, {V, D, Rlr, {Vr, Dr, Lr, Rr, b}, r}, b}), b};
rotate({V, D, {Vl, Dl, Ll, {Vlr, Dlr, Llr, Rlr, b}, r}, nil_nil, b}) ->
    {Vl, Dl, Ll, balance({Vlr, Dlr, Llr, {V, D, Rlr, nil, r}, b}), b};
rotate({V, D, L, R, C}) ->
    {V, D, L, R, C}.



min_del({V, D, nil, nil, r}) -> {{V, D}, nil};
min_del({V, D, nil, nil, b}) -> {{V, D}, nil_nil};
min_del({V, D, nil, {Vr, Dr, nil, nil, r}, b}) -> {{V, D}, {Vr, Dr, nil, nil, b}};
min_del({V, D, L, R, C}) ->
    {{NewVer, NewData}, NewSubTree} = min_del(L),
    {{NewVer, NewData}, rotate({V, D, NewSubTree, R, C})}.


make_red({V, D, {Vl, Dl, Ll, Rl, b}, {Vr, Dr, Lr, Rr, b}, b}) ->
    {V, D, {Vl, Dl, Ll, Rl, b}, {Vr, Dr, Lr, Rr, b}, r};
make_red(T) ->
    T.



makeRootBlack(nil) ->
    nil;
makeRootBlack({Ver, Data, Left, Right, _C}) ->
    {Ver, Data, Left, Right, b}.


versions_eq(Ver1, Ver2) ->
    vectorclock:eq(Ver1, Ver2).
versions_gt(Ver1, Ver2) ->
    vectorclock:gt(Ver1, Ver2).
versions_lt(Ver1, Ver2) ->
    vectorclock:lt(Ver1, Ver2).
%%versions_eq(Ver1, Ver2) ->
%%    Ver1 =:= Ver2.
%%versions_gt(Ver1, Ver2) ->
%%    Ver1 > Ver2.
%%versions_lt(Ver1, Ver2) ->
%%    Ver1 < Ver2.
