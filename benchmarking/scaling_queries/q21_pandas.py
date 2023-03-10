import pandas as pd
def q21(lineitem, orders, nation, supplier):
    df_filter_1 = lineitem[(lineitem.l_receiptdate > lineitem.l_commitdate)]
    df_filter_1 = df_filter_1[['l_suppkey', 'l_orderkey', 'l_receiptdate', 'l_commitdate']]
    df_filter_2 = orders[(orders.o_orderstatus == 'F') & (~orders.o_orderstatus.isnull())]
    df_filter_2 = df_filter_2[['o_orderkey', 'o_orderstatus']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_3 = supplier[['s_suppkey', 's_nationkey', 's_name']]
    df_filter_4 = nation[(nation.n_name == 'SAUDI ARABIA') & (~nation.n_name.isnull())]
    df_filter_4 = df_filter_4[['n_nationkey', 'n_name']]
    df_merge_2 = df_filter_3.merge(df_filter_4, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_3 = df_merge_1.merge(df_merge_2, left_on=['l_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_filter_5 = lineitem[['l_orderkey', 'l_suppkey']]
    inner_cond = df_merge_3.merge(df_filter_5, left_on='l_orderkey', right_on='l_orderkey', how='inner', sort=False)
    inner_cond = inner_cond[inner_cond.l_suppkey_x != inner_cond.l_suppkey_y]['l_orderkey']
    df_merge_4 = df_merge_3[df_merge_3.l_orderkey.isin(inner_cond)]
    df_filter_6 = lineitem[(lineitem.l_receiptdate > lineitem.l_commitdate)]
    df_filter_6 = df_filter_6[['l_orderkey', 'l_suppkey', 'l_receiptdate', 'l_commitdate']]
    inner_cond = df_merge_4.merge(df_filter_6, left_on='l_orderkey', right_on='l_orderkey', how='inner', sort=False)
    inner_cond = inner_cond[inner_cond.l_suppkey_x != inner_cond.l_suppkey_y]['l_orderkey']
    df_merge_5 = df_merge_4.merge(inner_cond, left_on=['l_orderkey'], right_on=['l_orderkey'], how="outer", indicator=True, sort=False)
    df_merge_5 = df_merge_5[df_merge_5._merge == "left_only"]
    df_group_1 = df_merge_5 \
        .groupby(['s_name'], sort=False) \
        .agg(
            numwait=("s_name", "count"),
        )
    df_group_1 = df_group_1[['numwait']]
    df_sort_1 = df_group_1.sort_values(by=['numwait', 's_name'], ascending=[False, True])
    df_limit_1 = df_sort_1.head(100)
    return df_limit_1
