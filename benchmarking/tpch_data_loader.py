import pandas as pd


# Lineitem

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

# Order

o_columnnames = ["O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"]

for i in range(len(o_columnnames)):
    o_columnnames[i] = o_columnnames[i].lower()
    
o_data_types = {
    'o_orderkey': int,
    'o_custkey': int,
    'o_orderstatus': str,
    'o_totalprice': float,
    'o_orderpriority': str,
    'o_clerk': str,
    'o_shippriority': int,
    'o_comment': str
}

o_parse_dates = ['o_orderdate']

# Don't set indexes, as we can't access them with Pandas selection!
orders = pd.read_table("../tpch-pgsql-master/data/load/orders.tbl.csv", sep="|", names=o_columnnames, dtype=o_data_types, parse_dates=o_parse_dates)

# Customer

c_columnnames = ["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"]

for i in range(len(c_columnnames)):
    c_columnnames[i] = c_columnnames[i].lower()
    
c_data_types = {
    'c_custkey': int,
    'c_name': str,
    'c_address': str,
    'c_nationkey': int,
    'c_phone': str,
    'c_acctbal': float,
    'c_mktsegment': str,
    'c_comment': str
}

c_parse_dates = []

# Don't set indexes, as we can't access them with Pandas selection!
customer = pd.read_table("../tpch-pgsql-master/data/load/customer.tbl.csv", sep="|", names=c_columnnames, dtype=c_data_types, parse_dates=c_parse_dates)