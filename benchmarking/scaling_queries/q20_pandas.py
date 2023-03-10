import pandas as pd
def q20(supplier, part, partsupp, nation, lineitem):
    df_filter_1 = supplier[['s_suppkey', 's_nationkey', 's_name', 's_address']]
    df_filter_2 = nation[(nation.n_name == 'CANADA') & (~nation.n_name.isnull())]
    df_filter_2 = df_filter_2[['n_nationkey', 'n_name']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_filter_3 = partsupp[['ps_partkey', 'ps_availqty', 'ps_suppkey']]
    df_filter_4 = part[(~part.p_name.isnull()) & (part.p_name.str.contains("^forest.*?$",regex=True))]
    df_filter_4 = df_filter_4[['p_name', 'p_partkey']]
    df_merge_2 = df_filter_3[df_filter_3.ps_partkey.isin(df_filter_4["p_partkey"])]
    df_filter_5 = lineitem[(lineitem.l_shipdate>='1994-01-01') & (lineitem.l_shipdate<'1995-01-01') & (~lineitem.l_shipdate.isnull())]
    df_filter_5 = df_filter_5[['l_partkey', 'l_suppkey', 'l_shipdate', 'l_quantity']]
    df_group_1 = df_filter_5 \
        .groupby(['l_partkey', 'l_suppkey'], sort=False) \
        .agg(
            sum_l_quantity=("l_quantity", "sum"),
        )
    df_group_1['suml_quantity'] = df_group_1.sum_l_quantity
    df_group_1 = df_group_1[['suml_quantity']]
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_3 = df_merge_2.merge(df_group_1, left_on=['ps_partkey', 'ps_suppkey'], right_on=['l_partkey', 'l_suppkey'], how="inner", sort=False)
    df_merge_3 = df_merge_3[(df_merge_3.ps_availqty > 0.5 * df_merge_3.suml_quantity)]
    df_merge_4 = df_merge_1[df_merge_1.s_suppkey.isin(df_merge_3["ps_suppkey"])]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['s_name'] = (df_merge_4.s_name)
    df_aggr_1['s_address'] = (df_merge_4.s_address)
    df_aggr_1 = df_aggr_1[['s_name', 's_address']]
    df_sort_1 = df_aggr_1.sort_values(by=['s_name'], ascending=[True])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1
