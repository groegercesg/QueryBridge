import pandas as pd
def q21(orders, nation, supplier, lineitem):
    df_filter_1 = orders[(orders.o_orderstatus == 'F')]
    df_filter_1 = df_filter_1[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
    df_filter_2 = lineitem[(lineitem.l_receiptdate > lineitem.l_commitdate)]
    df_filter_2 = df_filter_2[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
    df_filter_3 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
    df_filter_4 = nation[(nation.n_name == 'SAUDI ARABIA')]
    df_filter_4 = df_filter_4[['n_nationkey']]
    df_merge_1 = df_filter_3.merge(df_filter_4, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_1 = df_merge_1[['s_name', 's_suppkey']]
    df_merge_2 = df_filter_2.merge(df_merge_1, left_on=['l_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_merge_2 = df_merge_2[['s_name', 'l_suppkey', 'l_orderkey']]
    df_filter_5 = lineitem[(lineitem.l_receiptdate > lineitem.l_commitdate)]
    df_filter_5 = df_filter_5[['l_orderkey', 'l_suppkey']]
    inner_cond = df_merge_2.merge(df_filter_5, left_on='l_orderkey', right_on='l_orderkey', how='inner', sort=False)
    inner_cond = inner_cond[inner_cond.l_suppkey_x != inner_cond.l_suppkey_y]['l_orderkey']
    df_merge_3 = df_merge_2.merge(inner_cond, left_on=['l_orderkey'], right_on=['l_orderkey'], how="outer", indicator=True, sort=False)
    df_merge_3 = df_merge_3[df_merge_3._merge == "left_only"]
    df_merge_3 = df_merge_3[['s_name', 'l_suppkey', 'l_orderkey']]
    df_sort_1 = df_merge_3.sort_values(by=['l_orderkey'], ascending=[True])
    df_sort_1 = df_sort_1[['s_name', 'l_suppkey', 'l_orderkey']]
    df_merge_4 = df_filter_1.merge(df_sort_1, left_on=['o_orderkey'], right_on=['l_orderkey'], how="inner", sort=False)
    df_merge_4 = df_merge_4[['s_name', 'l_suppkey', 'l_orderkey', 'o_orderkey']]
    df_filter_6 = lineitem[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
    inner_cond = df_merge_4.merge(df_filter_6, left_on='o_orderkey', right_on='l_orderkey', how='inner', sort=False)
    inner_cond = inner_cond[inner_cond.l_suppkey_x != inner_cond.l_suppkey_y]['o_orderkey']
    df_merge_5 = df_merge_4[df_merge_4.o_orderkey.isin(inner_cond)]
    df_merge_5 = df_merge_5[['s_name']]
    df_sort_2 = df_merge_5.sort_values(by=['s_name'], ascending=[True])
    df_sort_2 = df_sort_2[['s_name']]
    df_group_1 = df_sort_2 \
        .groupby(['s_name'], sort=False) \
        .agg(
            numwait=("s_name", "count"),
        )
    df_group_1 = df_group_1[['numwait']]
    df_sort_3 = df_group_1.sort_values(by=['numwait', 's_name'], ascending=[False, True])
    df_sort_3 = df_sort_3[['numwait']]
    df_limit_1 = df_sort_3.head(100)
    return df_limit_1
