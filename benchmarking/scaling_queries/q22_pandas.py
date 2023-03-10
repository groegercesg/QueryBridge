import pandas as pd
def q22(orders, customer):
    df_filter_1 = customer[customer.c_phone.str.slice(0, 2).isin(['13', '31', '23', '29', '30', '18', '17'])]
    df_filter_1 = df_filter_1[['c_phone', 'c_acctbal', 'c_custkey']]
    df_filter_2 = customer[(customer.c_acctbal>0) & (~customer.c_acctbal.isnull()) & customer.c_phone.str.slice(0, 2).isin(['13', '31', '23', '29', '30', '18', '17'])]
    df_filter_2 = df_filter_2[['c_acctbal', 'c_phone']]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['avgc_acctbal'] = [(df_filter_2.c_acctbal).mean()]
    df_aggr_1 = df_aggr_1[['avgc_acctbal']]
    df_limit_1 = df_aggr_1.head(1)
    df_merge_1 = df_filter_1.merge(df_limit_1, how="cross", sort=False)
    df_merge_1 = df_merge_1[(df_merge_1.c_acctbal > df_merge_1.avgc_acctbal)]
    df_filter_3 = orders[['o_custkey']]
    df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['c_custkey'], right_on=['o_custkey'], how="outer", indicator=True, sort=False)
    df_merge_2 = df_merge_2[df_merge_2._merge == "left_only"]
    df_merge_2['cntrycode'] = df_merge_2.c_phone.str.slice(0, 2)
    df_group_1 = df_merge_2 \
        .groupby(['cntrycode'], sort=False) \
        .agg(
            numcust=("cntrycode", "count"),
            totacctbal=("c_acctbal", "sum"),
        )
    df_group_1 = df_group_1[['numcust', 'totacctbal']]
    df_sort_1 = df_group_1.sort_values(by=['cntrycode'], ascending=[True])
    df_limit_2 = df_sort_1.head(1)
    return df_limit_2
