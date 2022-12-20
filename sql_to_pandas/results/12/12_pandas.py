df_filter_1 = orders[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
df_filter_2 = lineitem[(lineitem.l_shipmode.isin(["MAIL","SHIP"])) & (lineitem.l_commitdate < lineitem.l_receiptdate) & (lineitem.l_shipdate < lineitem.l_commitdate) & (lineitem.l_receiptdate >= pd.Timestamp('1994-01-01 00:00:00')) & (lineitem.l_receiptdate < pd.Timestamp('1995-01-01 00:00:00'))]
df_filter_2 = df_filter_2[['l_shipmode', 'l_orderkey']]
df_sort_1 = df_filter_2.sort_values(by=['l_orderkey'], ascending=[True])
df_sort_1 = df_sort_1[['l_shipmode', 'l_orderkey']]
df_merge_1 = df_filter_1.merge(df_sort_1, left_on=['o_orderkey'], right_on=['l_orderkey'])
df_merge_1 = df_merge_1[['l_shipmode', 'o_orderpriority']]
df_sort_2 = df_merge_1.sort_values(by=['l_shipmode'], ascending=[True])
df_sort_2 = df_sort_2[['l_shipmode', 'o_orderpriority']]
df_sort_2['case_a'] = df_sort_2.apply(lambda x: 1 if ( ( (o_orderpriorit == '1-URGENT' ) x["OR"] ( x["o_orderpriority"] == '2-HIGH' )) else 0 x["END"], axis=1)
df_sort_2['case_b'] = df_sort_2.apply(lambda x: 1 if ( ( (o_orderpriorit <> '1-URGENT' ) x["AND"] ( x["o_orderpriority"] <> '2-HIGH' )) else 0 x["END"], axis=1)
df_group_1 = df_sort_2 \
    .groupby(['l_shipmode'], sort=False) \
    .agg(
        sum_case_a=("case_a", "sum"),
        sum_case_b=("case_b", "sum"),
    )
df_group_1['high_line_count'] = df_group_1.sum_case_a
df_group_1['low_line_count'] = df_group_1.sum_case_b
df_group_1 = df_group_1[['high_line_count', 'low_line_count']]
df_limit_1 = df_group_1[['high_line_count', 'low_line_count']]
df_limit_1 = df_limit_1.head(1)
return df_limit_1
