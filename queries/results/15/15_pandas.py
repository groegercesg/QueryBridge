df_filter_1 = lineitem[(lineitem_1.l_shipdate >= pd.Timestamp('1996-01-01 00:00:00')) & (lineitem_1.l_shipdate < pd.Timestamp('1996-04-01 00:00:00'))]
df_filter_1 = df_filter_1[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
df_group_1 = df_filter_1 \
    .groupby(['l_suppkey']) \
    .agg(
        suml_extendedprice * 1 - l_discount=("l_extendedprice * (1 - l_discount)", "sum"),
    )
df_group_1 = df_group_1[['suml_extendedprice * 1 - l_discount']]
df_aggr_1 = pd.DataFrame()
df_aggr_1['a'] = [((df_group_1.l_extendedprice * ( 1 - df_group_1.l_discount )).sum()).max()]
df_aggr_1 = df_aggr_1[['max(sum(l_extendedprice * (1 - l_discount)))']]
dollar_0 = df_aggr_1
df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
df_filter_2 = lineitem[(lineitem.l_shipdate >= pd.Timestamp('1996-01-01 00:00:00')) & (lineitem.l_shipdate < pd.Timestamp('1996-04-01 00:00:00'))]
df_filter_2 = df_filter_2[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
df_group_1 = df_filter_2 \
    .groupby(['l_suppkey']) \
    .agg(
        suml_extendedprice * 1 - l_discount=("l_extendedprice * (1 - l_discount)", "sum"),
        suml_extendedprice * 1 - l_discount=("l_extendedprice * (1 - l_discount)", "sum"),
    )
df_group_1 = df_group_1[df_group_1.suml_extendedprice * 1 - l_discount = dollar_0]
df_group_1 = df_group_1[['suml_extendedprice * 1 - l_discount']]
df_rename_1 = df_rename_1[['revenue0.total_revenue', 'revenue0.supplier_no']]
df_sort_1 = df_rename_1.sort_values(by=['revenue0.supplier_no'], ascending=[True])
df_sort_1 = df_sort_1[['revenue0.total_revenue', 'revenue0.supplier_no']]
df_merge_1 = df_filter_1.merge(df_sort_1, left_on="s_suppkey", right_on="supplier_no")
df_merge_1 = df_merge_1[['s_suppkey', 's_name', 's_address', 's_phone', 'revenue0.total_revenue']]
df_limit_1 = df_merge_1[['s_suppkey', 's_name', 's_address', 's_phone', 'revenue0.total_revenue']]
result = df_limit_1.head(0)
return result
<plan_to_explain_tree.limit_node object at 0x7ff984055ab0> = return result
