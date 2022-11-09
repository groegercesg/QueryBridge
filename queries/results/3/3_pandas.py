df_filter_1 = orders[orders.o_orderdate < pd.Timestamp('1995-03-15 00:00:00')]
df_filter_1 = df_filter_1[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
df_filter_2 = customer[customer.c_mktsegment == 'BUILDING']
df_filter_2 = df_filter_2[['c_custkey']]
df_merge_1 = df_filter_1.merge(df_filter_2, left_on="o_custkey", right_on="c_custkey")
df_merge_1 = df_merge_1[['o_orderdate', 'o_shippriority', 'o_orderkey']]
df_filter_3 = lineitem[lineitem.l_shipdate > pd.Timestamp('1995-03-15 00:00:00')]
df_filter_3 = df_filter_3[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
df_merge_2 = df_merge_1.merge(df_filter_3, left_on="o_orderkey", right_on="l_orderkey")
df_merge_2 = df_merge_2[['l_orderkey', 'o_orderdate', 'o_shippriority', 'l_extendedprice', 'l_discount']]
df_merge_2['revenue'] = df_merge_2.l_extendedprice * ( 1 - df_merge_2.l_discount )
df_group_1 = df_merge_2 \
    .groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']) \
    .agg(
        revenue=("revenue", "sum"),
    )
df_group_1 = df_group_1[['revenue']]
df_sort_1 = df_group_1.sort_values(by=['revenue', 'o_orderdate'], ascending=[False, True])
df_sort_1 = df_sort_1[['revenue']]
df_limit_1 = df_sort_1[['revenue']]
result = df_limit_1.head(10)
return result
