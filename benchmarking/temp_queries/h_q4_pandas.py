import pandas as pd
def qhesam-q4(orders, lineitem):
    df_filter_1 = orders[(orders.o_orderdate >= '1993-07-01 00:00:00') & (orders.o_orderdate < '1993-10-01 00:00:00')]
    df_filter_1 = df_filter_1[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
    df_filter_2 = lineitem[lineitem.l_commitdate < lineitem.l_receiptdate]
    df_filter_2 = df_filter_2[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
    df_group_1 = df_filter_2 \
        .groupby(['l_orderkey'], sort=False) \
        .last()
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_1 = df_filter_1.merge(df_group_1, left_on=['o_orderkey'], right_on=['l_orderkey'], how="inner", sort=False)
    df_merge_1 = df_merge_1[['o_orderpriority', 'o_orderdate']]
    df_sort_1 = df_merge_1.sort_values(by=['o_orderpriority'], ascending=[True])
    df_sort_1 = df_sort_1[['o_orderpriority', 'o_orderdate']]
    df_group_2 = df_sort_1 \
        .groupby(['o_orderpriority'], sort=False) \
        .agg(
            count_o_orderdate=("o_orderdate", "count"),
        )
    df_group_2['counto_orderdate'] = df_group_2.count_o_orderdate
    df_group_2 = df_group_2[['counto_orderdate']]
    return df_group_2
