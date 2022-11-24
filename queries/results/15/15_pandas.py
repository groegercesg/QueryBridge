df_filter_1 = lineitem[(lineitem.l_shipdate >= pd.Timestamp('1996-11-01 00:00:00')) & (lineitem.l_shipdate < pd.Timestamp('1997-02-01 00:00:00'))]
df_filter_1 = df_filter_1[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
df_group_1 = df_filter_1 \
    .groupby(['l_suppkey']) \
    .agg(
        suml_extendedprice * 1 - l_discount=("l_extendedprice * (1 - l_discount)", "sum"),
    )
df_group_1 = df_group_1[['suml_extendedprice * 1 - l_discount']]
