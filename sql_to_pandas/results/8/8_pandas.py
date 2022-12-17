df_filter_1 = part[(part.p_type) == 'ECONOMY ANODIZED STEEL']
df_filter_1 = df_filter_1[['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment']]
df_filter_2 = lineitem[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['p_partkey'], right_on=['l_partkey'])
df_merge_1 = df_merge_1[['l_extendedprice', 'l_discount', 'l_suppkey', 'l_orderkey']]
df_filter_3 = orders[(orders.o_orderdate >= pd.Timestamp('1995-01-01 00:00:00')) & (orders.o_orderdate <= pd.Timestamp('1996-12-31 00:00:00'))]
df_filter_3 = df_filter_3[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['l_orderkey'], right_on=['o_orderkey'])
df_merge_2 = df_merge_2[['l_extendedprice', 'l_discount', 'l_suppkey', 'o_orderdate', 'o_custkey']]
df_filter_4 = customer[['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment']]
df_merge_3 = df_merge_2.merge(df_filter_4, left_on=['o_custkey'], right_on=['c_custkey'])
df_merge_3 = df_merge_3[['l_extendedprice', 'l_discount', 'l_suppkey', 'o_orderdate', 'c_nationkey']]
df_filter_5 = region[region.r_name == 'AMERICA']
df_filter_5 = df_filter_5[['r_regionkey', 'r_name', 'r_comment']]
df_filter_6 = nation[['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']]
df_merge_4 = df_filter_5.merge(df_filter_6, left_on=['r_regionkey'], right_on=['n_regionkey'])
df_merge_4 = df_merge_4[['n_nationkey']]
df_merge_5 = df_merge_3.merge(df_merge_4, left_on=['c_nationkey'], right_on=['n_nationkey'])
df_merge_5 = df_merge_5[['l_extendedprice', 'l_discount', 'l_suppkey', 'o_orderdate']]
df_filter_7 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
df_merge_6 = df_merge_5.merge(df_filter_7, left_on=['l_suppkey'], right_on=['s_suppkey'])
df_merge_6 = df_merge_6[['l_extendedprice', 'l_discount', 's_nationkey', 'o_orderdate']]
df_filter_8 = nation[['n_name', 'n_nationkey']]
df_merge_7 = df_merge_6.merge(df_filter_8, left_on=['s_nationkey'], right_on=['n_nationkey'])
df_merge_7['o_year'] = df_merge_7.o_orderdate.dt.year
df_merge_7 = df_merge_7[['o_year', 'n_name', 'l_extendedprice', 'l_discount']]
df_sort_1 = df_merge_7.sort_values(by=['o_year'], ascending=[True])
df_sort_1 = df_sort_1[['o_year', 'n_name', 'l_extendedprice', 'l_discount']]
df_sort_1['case_a'] = df_sort_1.apply(lambda x: ( x["l_extendedprice"] * ( 1 - x["l_discount"] )) if ( x["n_name"] == 'BRAZIL' ) else 0, axis=1)
df_sort_1['before_1'] = ((df_sort_1.l_extendedprice) * (1 - (df_sort_1.l_discount)))
df_group_1 = df_sort_1 \
    .groupby(['o_year']) \
    .agg(
        sum_before_1=("before_1", "sum"),
    )
df_group_1['sumcase_asuml_extendedprice1l_discount'] = ((df_sort_1.case_a).sum() / df_group_1.sum_before_1)
df_group_1 = df_group_1[['sumcase_asuml_extendedprice1l_discount']]
df_limit_1 = df_group_1[['sumcase_asuml_extendedprice1l_discount']]
df_limit_1 = df_limit_1.head(1)
return df_limit_1
