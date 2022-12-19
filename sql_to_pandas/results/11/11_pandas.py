df_filter_1 = nation[nation.n_name == 'GERMANY']
df_filter_1 = df_filter_1[['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']]
df_filter_2 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['n_nationkey'], right_on=['s_nationkey'])
df_merge_1 = df_merge_1[['s_suppkey']]
df_filter_3 = partsupp[['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']]
df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['s_suppkey'], right_on=['ps_suppkey'])
df_merge_2 = df_merge_2[['ps_supplycost', 'ps_availqty']]
df_aggr_1 = pd.DataFrame()
df_aggr_1['sumps_supplycostps_availqty00001'] = [(((df_merge_2.ps_supplycost) * (df_merge_2.ps_availqty)).sum() * 0.0001)]
df_aggr_1 = df_aggr_1[['sumps_supplycostps_availqty00001']]
dollar_1 = df_aggr_1['sumps_supplycostps_availqty00001'][0]

df_filter_4 = nation[nation.n_name == 'GERMANY']
df_filter_4 = df_filter_4[['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']]
df_filter_5 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
df_merge_3 = df_filter_4.merge(df_filter_5, left_on=['n_nationkey'], right_on=['s_nationkey'])
df_merge_3 = df_merge_3[['s_suppkey']]
df_filter_6 = partsupp[['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']]
df_merge_4 = df_merge_3.merge(df_filter_6, left_on=['s_suppkey'], right_on=['ps_suppkey'])
df_merge_4 = df_merge_4[['ps_partkey', 'ps_supplycost', 'ps_availqty']]
df_merge_4['before_1'] = ((df_merge_4.ps_supplycost) * (df_merge_4.ps_availqty))
df_group_1 = df_merge_4 \
    .groupby(['ps_partkey'], sort=False) \
    .agg(
        sum_before_1=("before_1", "sum"),
    )
df_group_1['value'] = df_group_1.sum_before_1
df_group_1 = df_group_1[df_group_1.value > dollar_1]
df_group_1 = df_group_1[['value']]
df_sort_1 = df_group_1.sort_values(by=['value'], ascending=[False])
df_sort_1 = df_sort_1[['value']]
df_limit_1 = df_sort_1[['value']]
df_limit_1 = df_limit_1.head(1)
return df_limit_1
