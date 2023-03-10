import pandas as pd
def q17(lineitem, part):
    df_filter_1 = lineitem[['l_partkey', 'l_quantity', 'l_extendedprice']]
    df_filter_2 = part[(part.p_brand == 'Brand#23') & (~part.p_brand.isnull()) & (part.p_container == 'MED BOX') & (~part.p_container.isnull())]
    df_filter_2 = df_filter_2[['p_partkey', 'p_brand', 'p_container']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_filter_3 = lineitem[['l_partkey', 'l_quantity']]
    df_group_1 = df_filter_3 \
        .groupby(['l_partkey'], sort=False) \
        .agg(
            mean_l_quantity=("l_quantity", "mean"),
        )
    df_group_1['avgl_quantity'] = df_group_1.mean_l_quantity
    df_group_1 = df_group_1[['avgl_quantity']]
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_2 = df_merge_1.merge(df_group_1, left_on=['p_partkey'], right_on=['l_partkey'], how="inner", sort=False)
    df_merge_2 = df_merge_2[(df_merge_2.l_quantity < 0.200000 * df_merge_2.avgl_quantity)]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['suml_extendedprice'] = [(df_merge_2.l_extendedprice).sum()]
    df_aggr_1 = df_aggr_1[['suml_extendedprice']]
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['avg_yearly'] = ((df_aggr_1.suml_extendedprice) / 7.0)
    df_aggr_2 = df_aggr_2[['avg_yearly']]
    df_limit_1 = df_aggr_2.head(1)
    return df_limit_1
