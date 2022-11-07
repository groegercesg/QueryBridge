import pandas as pd
def query(lineitem):
    df_filter_1 = lineitem[lineitem.l_shipdate <= pd.Timestamp('1998-09-02 00:00:00')]
    df_filter_1 = df_filter_1[['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']]
    df_filter_1['sum_disc_price'] = df_filter_1.l_extendedprice * ( 1 - df_filter_1.l_discount )
    df_filter_1['sum_charge'] = ( df_filter_1.l_extendedprice * ( 1 - df_filter_1.l_discount )) * ( 1 + df_filter_1.l_tax )
    df_group_1 = df_filter_1 \
        .groupby(['l_returnflag', 'l_linestatus']) \
        .agg(
            sum_qty=("l_quantity", "sum"),
            sum_base_price=("l_extendedprice", "sum"),
            sum_disc_price=("sum_disc_price", "sum"),
            sum_charge=("sum_charge", "sum"),
            avg_qty=("l_quantity", "mean"),
            avg_price=("l_extendedprice", "mean"),
            avg_disc=("l_discount", "mean"),
            count_order=("l_returnflag", "count"),
        )
    df_group_1 = df_group_1[['sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']]
    df_sort_1 = df_group_1.sort_values(by=['l_returnflag', 'l_linestatus'], ascending=[True, True])
    df_sort_1 = df_sort_1[['sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']]
    df_limit_1 = df_sort_1[['sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']]
    result = df_limit_1.head(1)
    return result
