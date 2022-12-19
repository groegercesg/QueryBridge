import pandas as pd
def query(part, lineitem):
    df_filter_1 = part[['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment']]
    df_filter_2 = lineitem[(lineitem.l_shipdate >= pd.Timestamp('1995-09-01 00:00:00')) & (lineitem.l_shipdate < pd.Timestamp('1995-10-01 00:00:00'))]
    df_filter_2 = df_filter_2[['l_extendedprice', 'l_discount', 'l_partkey']]
    df_sort_1 = df_filter_2.sort_values(by=['l_partkey'], ascending=[True])
    df_sort_1 = df_sort_1[['l_extendedprice', 'l_discount', 'l_partkey']]
    df_merge_1 = df_filter_1.merge(df_sort_1, left_on=['p_partkey'], right_on=['l_partkey'])
    df_merge_1 = df_merge_1[['p_type', 'l_extendedprice', 'l_discount']]
    df_merge_1['case_a'] = df_merge_1.apply(lambda x: ( x["l_extendedprice"] * ( 1 - x["l_discount"] )) if x["p_type"].startswith("PROMO") else 0, axis=1)
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['promo_revenue'] = [((100.00 * (df_merge_1.case_a).sum()) / ((df_merge_1.l_extendedprice) * (1 - (df_merge_1.l_discount))).sum())]
    df_aggr_1 = df_aggr_1[['promo_revenue']]
    df_limit_1 = df_aggr_1[['promo_revenue']]
    df_limit_1 = df_limit_1.head(1)
    return df_limit_1
