df_filter_1 = customer[['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment']]
df_filter_2 = lineitem[lineitem.l_returnflag == 'R']
df_filter_2 = df_filter_2[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
df_filter_3 = orders[(orders.o_orderdate >= pd.Timestamp('1993-10-01 00:00:00')) & (orders.o_orderdate < pd.Timestamp('1994-01-01 00:00:00'))]
df_filter_3 = df_filter_3[['o_custkey', 'o_orderkey']]
df_merge_1 = df_filter_2.merge(df_filter_3, left_on="l_orderkey", right_on="o_orderkey")
df_merge_1 = df_merge_1[['o_custkey', 'l_extendedprice', 'l_discount']]
df_merge_2 = df_filter_1.merge(df_merge_1, left_on="c_custkey", right_on="o_custkey")
df_merge_2 = df_merge_2[['c_custkey', 'c_name', 'c_acctbal', 'c_address', 'c_phone', 'c_comment', 'c_nationkey', 'l_extendedprice', 'l_discount']]
df_filter_4 = nation[['n_name', 'n_nationkey']]
df_merge_3 = df_merge_2.merge(df_filter_4, left_on="c_nationkey", right_on="n_nationkey")
df_merge_3 = df_merge_3[['c_custkey', 'n_name', 'c_name', 'l_extendedprice', 'l_discount', 'c_acctbal', 'c_address', 'c_phone', 'c_comment']]
df_sort_1 = df_merge_3.sort_values(by=['c_custkey', 'n_name'], ascending=[True, True])
df_sort_1 = df_sort_1[['c_custkey', 'n_name', 'c_name', 'l_extendedprice', 'l_discount', 'c_acctbal', 'c_address', 'c_phone', 'c_comment']]
df_sort_1['revenue'] = df_sort_1.l_extendedprice * ( 1 - df_sort_1.l_discount )
df_group_1 = df_sort_1 \
    .groupby(['c_custkey', 'n_name', 'c_name', 'c_acctbal', 'c_address', 'c_phone', 'c_comment']) \
    .agg(
        revenue=("revenue", "sum"),
    )
df_group_1 = df_group_1[['revenue']]
df_sort_2 = df_group_1.sort_values(by=['revenue'], ascending=[False])
df_sort_2 = df_sort_2[['revenue']]
df_limit_1 = df_sort_2[['revenue']]
result = df_limit_1.head(20)
return result
