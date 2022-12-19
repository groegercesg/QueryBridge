df_filter_1 = orders[(orders.o_orderdate >= pd.Timestamp('1993-07-01 00:00:00')) & (orders.o_orderdate < pd.Timestamp('1993-10-01 00:00:00'))]
df_filter_1 = df_filter_1[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
df_filter_2 = lineitem[lineitem.l_commitdate < lineitem.l_receiptdate]
df_filter_2 = df_filter_2[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
df_merge_1 = df_filter_1[df_filter_1.o_orderkey.isin(df_filter_2["l_orderkey"])]
df_merge_1 = df_merge_1[['o_orderpriority']]
df_sort_1 = df_merge_1.sort_values(by=['o_orderpriority'], ascending=[True])
df_sort_1 = df_sort_1[['o_orderpriority']]
df_group_1 = df_sort_1 \
    .groupby(['o_orderpriority'], sort=False) \
    .agg(
        count_o_orderpriority=("o_orderpriority", "count"),
    )
df_group_1['order_count'] = df_group_1.count_o_orderpriority
df_group_1 = df_group_1[['order_count']]
df_limit_1 = df_group_1[['order_count']]
df_limit_1 = df_limit_1.head(1)
return df_limit_1
