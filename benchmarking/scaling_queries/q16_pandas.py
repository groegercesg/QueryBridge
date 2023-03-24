import pandas as pd
def q16(part, supplier, partsupp):
    df_filter_1 = partsupp[['ps_partkey', 'ps_suppkey']]
    df_filter_2 = part[part.p_size.isin([49, 14, 23, 45, 19, 3, 36, 9]) & (part.p_brand != 'Brand#45') & (part.p_type.str.contains("^MEDIUM POLISHED.*?$",regex=True) == False)]
    df_filter_2 = df_filter_2[['p_partkey', 'p_brand', 'p_type', 'p_size']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['ps_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_filter_3 = supplier[(supplier.s_comment.str.contains("^.*?Customer.*?Complaints.*?$",regex=True))]
    df_filter_3 = df_filter_3[['s_comment', 's_suppkey']]
    df_merge_2 = df_merge_1[~df_merge_1.ps_suppkey.isin(df_filter_3["s_suppkey"])]
    df_group_1 = df_merge_2 \
        .groupby(['p_brand', 'p_type', 'p_size'], sort=False) \
        .agg(
            supplier_cnt=("ps_suppkey", lambda x: x.nunique()),
        )
    df_group_1 = df_group_1[['supplier_cnt']]
    df_sort_1 = df_group_1.sort_values(by=['supplier_cnt', 'p_brand', 'p_type', 'p_size'], ascending=[False, True, True, True])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1
