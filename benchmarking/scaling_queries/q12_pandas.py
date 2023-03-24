import pandas as pd
def q12(lineitem, orders):
    df_filter_1 = lineitem[(lineitem.l_shipdate<'1995-01-01') & (~lineitem.l_shipdate.isnull()) & (lineitem.l_receiptdate>='1994-01-01') & (lineitem.l_receiptdate<'1995-01-01') & (~lineitem.l_receiptdate.isnull()) & (lineitem.l_commitdate<'1995-01-01') & (~lineitem.l_commitdate.isnull()) & (lineitem.l_commitdate < lineitem.l_receiptdate) & (lineitem.l_shipdate < lineitem.l_commitdate) & lineitem.l_shipmode.isin(['MAIL', 'SHIP'])]
    df_filter_1 = df_filter_1[['l_orderkey', 'l_shipmode', 'l_commitdate', 'l_receiptdate', 'l_shipdate']]
    df_filter_2 = orders[['o_orderkey', 'o_orderpriority']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_merge_1['case_a'] = df_merge_1.apply(lambda x: ( 1 ) if ( x['o_orderpriority'] == '1-URGENT' ) | ( x['o_orderpriority'] == '2-HIGH' ) else 0, axis=1)
    df_merge_1['case_b'] = df_merge_1.apply(lambda x: ( 1 ) if ( x['o_orderpriority'] != '1-URGENT' ) & ( x['o_orderpriority'] != '2-HIGH' ) else 0, axis=1)
    df_group_1 = df_merge_1 \
        .groupby(['l_shipmode'], sort=False) \
        .agg(
            high_line_count=("case_a", "sum"),
            low_line_count=("case_b", "sum"),
        )
    df_group_1 = df_group_1[['high_line_count', 'low_line_count']]
    df_sort_1 = df_group_1.sort_values(by=['l_shipmode'], ascending=[True])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1
