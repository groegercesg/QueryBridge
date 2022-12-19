df_filter_1 = supplier[((supplier.s_comment).str.contains("^.*?Customer.*?Complaints.*?$", regex=True))]
df_filter_1 = df_filter_1[['s_suppkey']]
df_filter_2 = partsupp[~partsupp.ps_suppkey.isin(df_filter_1["s_suppkey"])]
df_filter_2 = df_filter_2[['ps_partkey', 'ps_suppkey']]
df_filter_3 = part[(part.p_brand != 'Brand#45') & ((part.p_type).str.contains("^MEDIUM POLISHED.*?$", regex=True) == False) & (part.p_size.isin([49,14,23,45,19,3,36,9]))]
df_filter_3 = df_filter_3[['p_brand', 'p_type', 'p_size', 'p_partkey']]
df_sort_1 = df_filter_3.sort_values(by=['p_partkey'], ascending=[True])
df_sort_1 = df_sort_1[['p_brand', 'p_type', 'p_size', 'p_partkey']]
df_merge_1 = df_filter_2.merge(df_sort_1, left_on=['ps_partkey'], right_on=['p_partkey'])
df_merge_1 = df_merge_1[['p_brand', 'p_type', 'p_size', 'ps_suppkey']]
df_sort_2 = df_merge_1.sort_values(by=['p_brand', 'p_type', 'p_size'], ascending=[True, True, True])
df_sort_2 = df_sort_2[['p_brand', 'p_type', 'p_size', 'ps_suppkey']]
df_group_1 = df_sort_2 \
    .groupby(['p_brand', 'p_type', 'p_size'], sort=False) \
    .agg(
        supplier_cnt=("ps_suppkey", lambda x: x.nunique()),
    )
df_group_1 = df_group_1[['supplier_cnt']]
df_sort_3 = df_group_1.sort_values(by=['supplier_cnt', 'p_brand', 'p_type', 'p_size'], ascending=[False, True, True, True])
df_sort_3 = df_sort_3[['supplier_cnt']]
df_limit_1 = df_sort_3[['supplier_cnt']]
df_limit_1 = df_limit_1.head(1)
return df_limit_1
