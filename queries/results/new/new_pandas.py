df_filter_1 = supplier[supplier.s_address != ALL ({Germany,France,UK})]
df_filter_1 = df_filter_1[['s_suppkey']]
