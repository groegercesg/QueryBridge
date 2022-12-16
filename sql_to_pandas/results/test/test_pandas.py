df_filter_1 = lineitem[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
df_filter_1['before_1'] = (df_filter_1.l_quantity)
df_filter_1['before_2'] = ((df_filter_1.l_discount) * 0.5)
df_filter_1['before_3'] = (df_filter_1.l_extendedprice)
df_filter_1['before_4'] = (df_filter_1.l_tax)
df_filter_1['before_5'] = (df_filter_1.l_quantity)
df_filter_1['before_6'] = (df_filter_1.l_discount)
df_filter_1['before_7'] = (((df_filter_1.l_extendedprice) * (1 - (df_filter_1.l_discount))) * (1 + (df_filter_1.l_tax)))
df_group_1 = df_filter_1 \
    .groupby(['l_returnflag', 'l_linestatus']) \
    .agg(
        sum_before_1=("before_1", "sum"),
        mean_before_2=("before_2", "mean"),
        sum_before_3=("before_3", "sum"),
        min_before_4=("before_4", "min"),
        count_before_5=("before_5", "count"),
        min_before_6=("before_6", "min"),
        sum_before_7=("before_7", "sum"),
    )
df_group_1['sum_qty'] = ((df_group_1.sum_before_1 * df_group_1.mean_before_2) - 72)
df_group_1['sum_base_price'] = (df_group_1.sum_before_3 / (((df_group_1.count_before_5 - df_group_1.min_before_6) - df_group_1.min_before_4) * 0.5))
df_group_1['sum_charge'] = df_group_1.sum_before_7
df_group_1 = df_group_1[['sum_qty', 'sum_base_price', 'sum_charge']]
return df_group_1
