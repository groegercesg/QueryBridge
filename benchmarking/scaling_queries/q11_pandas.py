import pandas as pd
def q11(partsupp, supplier, nation):
    df_filter_1 = partsupp[['ps_suppkey', 'ps_partkey', 'ps_supplycost', 'ps_availqty']]
    df_filter_2 = supplier[['s_suppkey', 's_nationkey']]
    df_filter_3 = nation[(nation.n_name == 'GERMANY') & (~nation.n_name.isnull())]
    df_filter_3 = df_filter_3[['n_nationkey', 'n_name']]
    df_merge_1 = df_filter_2.merge(df_filter_3, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_2 = df_filter_1.merge(df_merge_1, left_on=['ps_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_merge_2['before_1'] = ((df_merge_2.ps_supplycost) * (df_merge_2.ps_availqty))
    df_group_1 = df_merge_2 \
        .groupby(['ps_partkey'], sort=False) \
        .agg(
            value=("before_1", "sum"),
        )
    df_group_1 = df_group_1[['value']]
    df_filter_4 = partsupp[['ps_suppkey', 'ps_supplycost', 'ps_availqty']]
    df_filter_5 = supplier[['s_suppkey', 's_nationkey']]
    df_filter_6 = nation[(nation.n_name == 'GERMANY') & (~nation.n_name.isnull())]
    df_filter_6 = df_filter_6[['n_nationkey', 'n_name']]
    df_merge_3 = df_filter_5.merge(df_filter_6, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_4 = df_filter_4.merge(df_merge_3, left_on=['ps_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['sumps_supplycostps_availqty00001'] = [(((df_merge_4.ps_supplycost) * (df_merge_4.ps_availqty)).sum() * 0.0001)]
    df_aggr_1 = df_aggr_1[['sumps_supplycostps_availqty00001']]
    df_limit_1 = df_aggr_1.head(1)
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_5 = df_group_1.merge(df_limit_1, how="cross", sort=False)
    df_merge_5 = df_merge_5[(df_merge_5.value > df_merge_5.sumps_supplycostps_availqty00001)]
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['ps_partkey'] = (df_merge_5.ps_partkey)
    df_aggr_2['value'] = (df_merge_5.value)
    df_aggr_2 = df_aggr_2[['ps_partkey', 'value']]
    df_sort_1 = df_aggr_2.sort_values(by=['value'], ascending=[False])
    df_limit_2 = df_sort_1.head(1)
    return df_limit_2
