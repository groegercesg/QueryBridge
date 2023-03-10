import pandas as pd
def q9(nation, supplier, part, lineitem, orders, partsupp):
    df_filter_1 = lineitem[['l_suppkey', 'l_partkey', 'l_orderkey', 'l_extendedprice', 'l_discount', 'l_quantity']]
    df_filter_2 = partsupp[['ps_suppkey', 'ps_partkey', 'ps_supplycost']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_suppkey', 'l_partkey'], right_on=['ps_suppkey', 'ps_partkey'], how="inner", sort=False)
    df_filter_3 = orders[['o_orderkey', 'o_orderdate']]
    df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_4 = supplier[['s_suppkey', 's_nationkey']]
    df_filter_5 = nation[['n_nationkey', 'n_name']]
    df_merge_3 = df_filter_4.merge(df_filter_5, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_4 = df_merge_2.merge(df_merge_3, left_on=['ps_suppkey', 'l_suppkey'], right_on=['s_suppkey', 's_suppkey'], how="inner", sort=False)
    df_filter_6 = part[(part.p_name.str.contains("^.*?green.*?$",regex=True))]
    df_filter_6 = df_filter_6[['p_partkey', 'p_name']]
    df_merge_5 = df_merge_4.merge(df_filter_6, left_on=['ps_partkey', 'l_partkey'], right_on=['p_partkey', 'p_partkey'], how="inner", sort=False)
    df_merge_5['nation'] = df_merge_5.n_name
    df_merge_5['o_year'] = df_merge_5.o_orderdate.dt.year
    df_merge_5['amount'] = (((df_merge_5.l_extendedprice) * (1 - (df_merge_5.l_discount))) - ((df_merge_5.ps_supplycost) * (df_merge_5.l_quantity)))
    df_group_1 = df_merge_5 \
        .groupby(['nation', 'o_year'], sort=False) \
        .agg(
            sum_profit=("amount", "sum"),
        )
    df_group_1 = df_group_1[['sum_profit']]
    df_sort_1 = df_group_1.sort_values(by=['nation', 'o_year'], ascending=[True, False])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1
