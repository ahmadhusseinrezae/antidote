-type key_values_pair() :: {Key :: pos_integer(), Values :: [term()]}.
-type key_values_pairs() :: [key_values_pair()].
-type table() :: ets:tab().
-type left_right() :: to_left | to_right.
-type which_child() :: self | left | right.
-type version() :: term().
-type version_data() :: key_values_pairs() | pos_integer().
-type version_index() :: version_data_index() | version_length_index().
-type generic_rb_index() :: nil | {Key :: term(),
                                    Data :: term(),
                                    LeftSubTree :: generic_rb_index(),
                                    RightSubTree :: generic_rb_index(),
                                    Color :: rb_tree_color()}.
-type version_data_list() :: [ {version(), key_values_pairs()} ].
-type version_data_index() :: nil | {VersionKey :: version(),
                                VersionData :: key_values_pairs(),
                                LeftVersionSubTree :: version_data_index(),
                                RightVersionSubTree :: version_data_index(),
                                Color :: rb_tree_color()}.
-type version_length_list() :: [ {version(), pos_integer()} ].
-type version_length_index() :: nil | {VersionKey :: version(),
                                      VersionData :: pos_integer(),
                                      LeftVersionSubTree :: version_length_index(),
                                      RightVersionSubTree :: version_length_index(),
                                      Color :: rb_tree_color()}.
-type generic_data_cons_fun() :: fun((term(), term()) -> term()).
-type version_id_data() :: nil | {version(), key_values_pairs()}.
-type rb_tree_color() :: r | b | bb.
-type po_tree_color() :: r | b.
-type po_tree_leaf() :: {Key:: pos_integer(), Color :: po_tree_color(), leaf}.
-type po_tree() :: {nil,b} |
                    {Key:: pos_integer(), Color :: po_tree_color(), leaf} |
                    {LeftSubTree :: po_tree(), RightSubTree :: po_tree(), Key :: pos_integer(),Color:: po_tree_color()}.
-type po_tree_payload() :: {VersionIndex :: version_data_index(),
                            LeftLeafKey :: pos_integer(),
                            RightLeafKey :: pos_integer(),
                            VersionLengthIndex :: version_length_index()}.
