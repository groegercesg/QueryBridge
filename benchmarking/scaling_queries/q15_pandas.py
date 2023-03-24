import pandas as pd
def q15(supplier, lineitem):
    df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_phone']]
    df_filter_2 = lineitem[(lineitem.l_shipdate>='1996-01-01') & (lineitem.l_shipdate<'1996-04-01') & (~lineitem.l_shipdate.isnull())]
    df_filter_2 = df_filter_2[['l_shipdate', 'l_suppkey', 'l_extendedprice', 'l_discount']]
    df_filter_2['supplier_no'] = df_filter_2.l_suppkey
    df_filter_2['before_1'] = ((df_filter_2.l_extendedprice) * (1 - (df_filter_2.l_discount)))
    df_group_1 = df_filter_2 \
        .groupby(['supplier_no'], sort=False) \
        .agg(
            total_revenue=("before_1", "sum"),
        )
    df_group_1 = df_group_1[['total_revenue']]
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_1 = df_filter_1.merge(df_group_1, left_on=['s_suppkey'], right_on=['supplier_no'], how="inner", sort=False)
    df_filter_3 = lineitem[(lineitem.l_shipdate>='1996-01-01') & (lineitem.l_shipdate<'1996-04-01') & (~lineitem.l_shipdate.isnull())]
    df_filter_3 = df_filter_3[['l_shipdate', 'l_suppkey', 'l_extendedprice', 'l_discount']]
    df_filter_3['supplier_no'] = df_filter_3.l_suppkey
    df_filter_3['before_1'] = ((df_filter_3.l_extendedprice) * (1 - (df_filter_3.l_discount)))
    df_group_2 = df_filter_3 \
        .groupby(['supplier_no'], sort=False) \
        .agg(
            total_revenue=("before_1", "sum"),
        )
    df_group_2 = df_group_2[['total_revenue']]
    df_group_2 = df_group_2.reset_index()
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['maxtotal_revenue'] = [(df_group_2.total_revenue).max()]
    df_aggr_1 = df_aggr_1[['maxtotal_revenue']]
    df_limit_1 = df_aggr_1.head(1)
    df_merge_2 = df_merge_1.merge(df_limit_1, left_on=['total_revenue'], right_on=['maxtotal_revenue'], how="inner", sort=False)
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['s_suppkey'] = (df_merge_2.s_suppkey)
    df_aggr_2['s_name'] = (df_merge_2.s_name)
    df_aggr_2['s_address'] = (df_merge_2.s_address)
    df_aggr_2['s_phone'] = (df_merge_2.s_phone)
    df_aggr_2['total_revenue'] = (df_merge_2.total_revenue)
    df_aggr_2 = df_aggr_2[['s_suppkey', 's_name', 's_address', 's_phone', 'total_revenue']]
    df_sort_1 = df_aggr_2.sort_values(by=['s_suppkey'], ascending=[True])
    df_limit_2 = df_sort_1.head(1)
    return df_limit_2
