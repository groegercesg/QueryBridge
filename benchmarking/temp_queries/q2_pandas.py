import numpy as np
import pandas as pd
def query(nation_1, region_1, supplier, region, partsupp_1, supplier_1, partsupp, part, nation):
    df_filter_1 = part[(part.p_type.str.contains("^.*?BRASS$",regex=True)) & (part.p_size == 15)]
    df_filter_1 = df_filter_1[['p_partkey', 'p_mfgr']]
    df_filter_2 = partsupp[['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']]
    df_filter_3 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
    df_filter_4 = nation[['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']]
    df_filter_5 = region[region.r_name == 'EUROPE']
    df_filter_5 = df_filter_5[['r_regionkey']]
    df_merge_1 = df_filter_4.merge(df_filter_5, left_on=['n_regionkey'], right_on=['r_regionkey'])
    df_merge_1 = df_merge_1[['n_name', 'n_nationkey']]
    df_merge_2 = df_filter_3.merge(df_merge_1, left_on=['s_nationkey'], right_on=['n_nationkey'])
    df_merge_2 = df_merge_2[['s_acctbal', 's_name', 's_address', 's_phone', 's_comment', 's_suppkey', 'n_name']]
    df_merge_3 = df_filter_2.merge(df_merge_2, left_on=['ps_suppkey'], right_on=['s_suppkey'])
    df_merge_3 = df_merge_3[['s_acctbal', 's_name', 's_address', 's_phone', 's_comment', 'ps_partkey', 'ps_supplycost', 'n_name']]
    df_merge_4 = df_filter_1.merge(df_merge_3, left_on=['p_partkey'], right_on=['ps_partkey'])
    df_merge_4 = df_merge_4[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment', 'ps_supplycost']]
    df_filter_6 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
    df_filter_7 = partsupp[['ps_supplycost', 'ps_suppkey', 'ps_partkey']]
    df_filter_8 = part[['p_partkey']]
    df_merge_5 = df_filter_7.merge(df_filter_8, left_on=['ps_partkey'], right_on=['p_partkey'])
    df_merge_5 = df_merge_5[['ps_supplycost', 'ps_suppkey', 'p_partkey']]
    df_merge_6 = df_filter_6.merge(df_merge_5, left_on=['s_suppkey'], right_on=['ps_suppkey'])
    df_merge_6 = df_merge_6[['ps_supplycost', 's_nationkey', 'p_partkey']]
    df_filter_9 = nation[['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']]
    df_filter_10 = region[region.r_name == 'EUROPE']
    df_filter_10 = df_filter_10[['r_regionkey']]
    df_merge_7 = df_filter_9.merge(df_filter_10, left_on=['n_regionkey'], right_on=['r_regionkey'])
    df_merge_7 = df_merge_7[['n_nationkey']]
    df_merge_8 = df_merge_6.merge(df_merge_7, left_on=['s_nationkey'], right_on=['n_nationkey'])
    df_merge_8 = df_merge_8[['ps_supplycost', 'p_partkey']]
    df_group_1 = df_merge_8 \
        .groupby(['p_partkey'], sort=False) \
        .agg(
            min_ps_supplycost=("ps_supplycost", "min"),
        )
    df_group_1['minps_supplycost'] = df_group_1.min_ps_supplycost
    df_group_1 = df_group_1[['minps_supplycost']]
    df_merge_9 = df_merge_4.merge(df_group_1, left_on=['ps_supplycost', 'p_partkey'], right_on=['minps_supplycost', 'p_partkey'])
    df_merge_9 = df_merge_9[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]
    df_sort_1 = df_merge_9.sort_values(by=['s_acctbal', 'n_name', 's_name', 'p_partkey'], ascending=[False, True, True, True])
    df_sort_1 = df_sort_1[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]
    df_limit_1 = df_sort_1[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]
    df_limit_1 = df_limit_1.head(100)
    return df_limit_1
