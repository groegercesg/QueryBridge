df_filter_1 = region[region.r_name == 'EUROPE']
df_filter_1 = df_filter_1[['r_regionkey', 'r_name', 'r_comment']]
df_filter_2 = partsupp[['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']]
df_filter_2 = df_filter_2['partsupp.ps_partkey = part.p_partkey']
df_filter_3 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
df_merge_1 = df_filter_2.merge(df_filter_3, left_on="ps_suppkey", right_on="s_suppkey")
df_merge_1 = df_merge_1[['ps_supplycost', 's_nationkey']]
df_filter_4 = nation[['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']]
df_merge_2 = df_merge_1.merge(df_filter_4, left_on="s_nationkey", right_on="n_nationkey")
df_merge_2 = df_merge_2[['ps_supplycost', 'n_regionkey']]
df_merge_3 = df_filter_1.merge(df_merge_2, left_on="r_regionkey", right_on="n_regionkey")
df_merge_3 = df_merge_3[['ps_supplycost']]
df_aggr_1 = pd.DataFrame()
df_aggr_1['minps_supplycost'] = [(df_merge_3.ps_supplycost).min()]
df_aggr_1 = df_aggr_1[['minps_supplycost']]
SubPlan 1 = df_aggr_1['minps_supplycost'][0]

df_filter_5 = part[(part.p_type.str.contains("^'.*?BRASS'$", regex=True)) & (part.p_size = 15)]
df_filter_5 = df_filter_5[['p_partkey', 'p_mfgr']]
df_filter_6 = region[region.r_name == 'EUROPE']
df_filter_6 = df_filter_6[['r_regionkey', 'r_name', 'r_comment']]
df_filter_7 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
df_filter_8 = nation[['n_name', 'n_nationkey', 'n_regionkey']]
df_merge_4 = df_filter_7.merge(df_filter_8, left_on="s_nationkey", right_on="n_nationkey")
df_merge_4 = df_merge_4[['s_acctbal', 's_name', 's_address', 's_phone', 's_comment', 's_suppkey', 'n_name', 'n_regionkey']]
df_merge_5 = df_filter_6.merge(df_merge_4, left_on="r_regionkey", right_on="n_regionkey")
df_merge_5 = df_merge_5[['s_acctbal', 's_name', 's_address', 's_phone', 's_comment', 's_suppkey', 'n_name']]
df_filter_9 = partsupp[['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']]
df_merge_6 = df_merge_5.merge(df_filter_9, left_on="s_suppkey", right_on="ps_suppkey")
df_merge_6 = df_merge_6[['s_acctbal', 's_name', 's_address', 's_phone', 's_comment', 'ps_partkey', 'ps_supplycost', 'n_name']]
df_merge_7 = df_filter_5.merge(df_merge_6, left_on="p_partkey", right_on="ps_partkey) AND ((SubPlan 1)")
df_merge_7 = df_merge_7[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]
df_sort_1 = df_merge_7.sort_values(by=['s_acctbal', 'n_name', 's_name', 'p_partkey'], ascending=[False, True, True, True])
df_sort_1 = df_sort_1[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]
df_limit_1 = df_sort_1[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]
df_limit_1 = df_limit_1.head(100)
return df_limit_1
