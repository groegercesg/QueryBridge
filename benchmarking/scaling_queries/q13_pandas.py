import pandas as pd
def q13(customer, orders):
    df_filter_1 = orders[(orders.o_comment.str.contains("^.*?special.*?requests.*?$",regex=True) == False)]
    df_filter_1 = df_filter_1[['o_custkey', 'o_comment', 'o_orderkey']]
    df_filter_2 = customer[['c_custkey']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['o_custkey'], right_on=['c_custkey'], how="right", sort=False)
    df_group_1 = df_merge_1 \
        .groupby(['c_custkey'], sort=False) \
        .agg(
            c_count=("o_orderkey", "count"),
        )
    df_group_1 = df_group_1[['c_count']]
    df_group_2 = df_group_1 \
        .groupby(['c_count'], sort=False) \
        .agg(
            custdist=("c_count", "count"),
        )
    df_group_2 = df_group_2[['custdist']]
    df_sort_1 = df_group_2.sort_values(by=['custdist', 'c_count'], ascending=[False, False])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1
