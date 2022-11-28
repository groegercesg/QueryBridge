df_filter_1 = lineitem[(lineitem.l_shipdate >= pd.Timestamp('1996-01-01 00:00:00')) & (lineitem.l_shipdate < pd.Timestamp('1996-04-01 00:00:00'))]
df_filter_1['l_extendedprice1l_discount'] = df_filter_1.l_extendedprice * ( 1 - df_filter_1.l_discount )
df_group_1 = df_filter_1 \
    .groupby(['l_suppkey']) \
    .agg(
        suml_extendedprice1l_discount=("l_extendedprice1l_discount", "sum"),
    )
df_aggr_1 = pd.DataFrame()
df_aggr_1['maxsuml_extendedprice1l_discount'] = [(df_group_1.suml_extendedprice1l_discount).max()]
dollar_0 = df_aggr_1['maxsuml_extendedprice1l_discount'][0]

df_filter_2 = supplier
df_filter_3 = lineitem[(lineitem.l_shipdate >= pd.Timestamp('1996-01-01 00:00:00')) & (lineitem.l_shipdate < pd.Timestamp('1996-04-01 00:00:00'))]
df_filter_3['l_extendedprice1l_discount'] = df_filter_3.l_extendedprice * ( 1 - df_filter_3.l_discount )
df_group_2 = df_filter_3 \
    .groupby(['l_suppkey']) \
    .agg(
        suml_extendedprice1l_discount=("l_extendedprice1l_discount", "sum"),
    )
df_group_2 = df_group_2[df_group_2.suml_extendedprice1l_discount == dollar_0]
df_group_2 = df_group_2.rename_axis(['l_suppkey']).reset_index()
df_rename_1 = pd.DataFrame()
df_rename_1['total_revenue'] = df_group_2['suml_extendedprice1l_discount']
df_rename_1['supplier_no'] = df_group_2['l_suppkey']
df_sort_1 = df_rename_1.sort_values(by=['supplier_no'], ascending=[True])
df_merge_1 = df_filter_2.merge(df_sort_1, left_on="s_suppkey", right_on="supplier_no")
df_limit_1 = df_merge_1
result = df_limit_1.head(1)
return result
