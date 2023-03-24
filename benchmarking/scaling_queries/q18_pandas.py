import pandas as pd
def q18(customer, lineitem, orders):
    df_filter_1 = lineitem[['l_orderkey', 'l_quantity']]
    df_filter_2 = orders[['o_orderkey', 'o_custkey', 'o_orderdate', 'o_totalprice']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_3 = customer[['c_custkey', 'c_name']]
    df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['o_custkey'], right_on=['c_custkey'], how="inner", sort=False)
    df_filter_4 = lineitem[['l_orderkey', 'l_quantity']]
    df_group_1 = df_filter_4 \
        .groupby(['l_orderkey'], sort=False) \
        .agg(
            sum_l_quantity=("l_quantity", "sum"),
        )
    df_group_1['suml_quantity'] = df_group_1.sum_l_quantity
    df_group_1 = df_group_1[df_group_1.suml_quantity > 300.000]
    df_group_1 = df_group_1[['suml_quantity']]
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_3 = df_merge_2[df_merge_2.o_orderkey.isin(df_group_1["l_orderkey"])]
    df_group_2 = df_merge_3 \
        .groupby(['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice'], sort=False) \
        .agg(
            suml_quantity=("l_quantity", "sum"),
        )
    df_group_2 = df_group_2[['suml_quantity']]
    df_sort_1 = df_group_2.sort_values(by=['o_totalprice', 'o_orderdate'], ascending=[False, True])
    df_limit_1 = df_sort_1.head(100)
    return df_limit_1
