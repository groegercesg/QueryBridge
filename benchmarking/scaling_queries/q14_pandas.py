import pandas as pd
def q14(part, lineitem):
    df_filter_1 = lineitem[(lineitem.l_shipdate>='1995-09-01') & (lineitem.l_shipdate<'1995-10-01') & (~lineitem.l_shipdate.isnull())]
    df_filter_1 = df_filter_1[['l_partkey', 'l_shipdate', 'l_extendedprice', 'l_discount']]
    df_filter_2 = part[['p_partkey', 'p_type']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_merge_1['case_a'] = df_merge_1.apply(lambda x: ( x["l_extendedprice"] * ( 1 - x["l_discount"] )) if x["p_type"].startswith("PROMO") else 0, axis=1)
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['sumcase_a'] = [(df_merge_1.case_a).sum()]
    df_aggr_1['suml_extendedprice1l_discount'] = [((df_merge_1.l_extendedprice) * (1 - (df_merge_1.l_discount))).sum()]
    df_aggr_1 = df_aggr_1[['sumcase_a', 'suml_extendedprice1l_discount']]
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['promo_revenue'] = ((100.00 * (df_aggr_1.sumcase_a)) / (df_aggr_1.suml_extendedprice1l_discount))
    df_aggr_2 = df_aggr_2[['promo_revenue']]
    df_limit_1 = df_aggr_2.head(1)
    return df_limit_1
