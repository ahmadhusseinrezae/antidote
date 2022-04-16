% type and record definitions for range tree

-type range_record() :: {Column_val :: number(), Row_id :: number()}.
-type num_vals() :: [range_record()].
-type interval() :: {number(), number()}.
-type range_tree() :: range_record() | {S_l :: num_vals(), 
                                        S_r :: num_vals(), 
                                        I_l :: interval(),
                                        I_l :: interval(),
                                        R :: range_tree(), 
                                        L :: range_tree(),
                                        Val :: range_record() }.