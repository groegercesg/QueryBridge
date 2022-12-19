df_filter_1 = customer[['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment']]
df_filter_2 = orders[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
df_filter_3 = lineitem[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
df_group_1 = df_filter_3 \
    .groupby(['l_orderkey'], sort=False) \
    .agg(
        sum_l_quantity=("l_quantity", "sum"),
    )
df_group_1['suml_quantity'] = df_group_1.sum_l_quantity
df_group_1 = df_group_1[df_group_1.suml_quantity > 300]
df_merge_1 = df_filter_2.merge(df_group_1, left_on=['o_orderkey'], right_on=['l_orderkey'])
df_merge_1 = df_merge_1[['o_orderkey', 'o_orderdate', 'o_totalprice', 'o_custkey']]
df_sort_1 = df_merge_1.sort_values(by=['o_custkey'], ascending=[True])
df_sort_1 = df_sort_1[['o_orderkey', 'o_orderdate', 'o_totalprice', 'o_custkey']]
df_merge_2 = df_filter_1.merge(df_sort_1, left_on=['c_custkey'], right_on=['o_custkey'])
df_merge_2 = df_merge_2[['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice']]
df_filter_4 = lineitem[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
df_merge_3 = df_merge_2.merge(df_filter_4, left_on=['o_orderkey'], right_on=['l_orderkey'])
df_merge_3 = df_merge_3[['c_custkey', 'o_orderkey', 'c_name', 'o_orderdate', 'o_totalprice', 'l_quantity']]
df_sort_2 = df_merge_3.sort_values(by=['c_custkey', 'o_orderkey'], ascending=[True, True])
df_sort_2 = df_sort_2[['c_custkey', 'o_orderkey', 'c_name', 'o_orderdate', 'o_totalprice', 'l_quantity']]
df_group_2 = df_sort_2 \
    .groupby(['c_custkey', 'o_orderkey', 'c_name', 'o_orderdate', 'o_totalprice'], sort=False) \
    .agg(
        sum_l_quantity=("l_quantity", "sum"),
    )
df_group_2['suml_quantity'] = df_group_2.sum_l_quantity
df_group_2 = df_group_2[['suml_quantity']]
df_sort_3 = df_group_2.sort_values(by=['o_totalprice', 'o_orderdate'], ascending=[False, True])
df_sort_3 = df_sort_3[['suml_quantity']]
df_limit_1 = df_sort_3[['suml_quantity']]
df_limit_1 = df_limit_1.head(100)
return df_limit_1
