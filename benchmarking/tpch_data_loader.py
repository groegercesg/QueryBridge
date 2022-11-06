import pandas as pd

l_columnnames = ["L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX",
                "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"]

for i in range(len(l_columnnames)):
    l_columnnames[i] = l_columnnames[i].lower()

l_data_types = {
    'l_orderkey': int,
    'l_partkey': int,
    'l_suppkey': int,
    'l_linenumber': int,
    'l_quantity': float,
    'l_extendedprice': float,
    'l_discount': float,
    'l_tax': float,
    'l_returnflag': str,
    'l_linestatus': str,
    'l_shipinstruct': str,
    'l_shipmode': str,
    'l_comment': str
}

l_parse_dates = ['l_shipdate', 'l_commitdate', 'l_receiptdate']

# Don't set indexes, as we can't access them with Pandas selection!
lineitem = pd.read_table("../tpch-pgsql-master/data/load/lineitem.tbl.csv", sep="|", names=l_columnnames, dtype=l_data_types, parse_dates=l_parse_dates)