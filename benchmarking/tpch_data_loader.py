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
lineitem = pd.read_table("data/lineitem.tbl.csv", sep="|", names=l_columnnames, dtype=l_data_types, parse_dates=l_parse_dates)

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
orders = pd.read_table("data/orders.tbl.csv", sep="|", names=o_columnnames, dtype=o_data_types, parse_dates=o_parse_dates)

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
customer = pd.read_table("data/customer.tbl.csv", sep="|", names=c_columnnames, dtype=c_data_types, parse_dates=c_parse_dates)

# Part

p_columnnames = ["P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT"]

for i in range(len(p_columnnames)):
    p_columnnames[i] = p_columnnames[i].lower()
    
p_data_types = {
    'p_partkey': int, 
    'p_name': str,
    'p_mfgr': str,
    'p_brand': str,
    'p_type': str,
    'p_size': int,
    'p_container': str,
    'p_retailprice': float,
    'p_comment': str
}

p_parse_dates = []

# Don't set indexes, as we can't access them with Pandas selection!
part = pd.read_table("data/part.tbl.csv", sep="|", names=p_columnnames, dtype=p_data_types, parse_dates=p_parse_dates)

# Nation

n_columnnames = ["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"]

for i in range(len(n_columnnames)):
    n_columnnames[i] = n_columnnames[i].lower()
    
n_data_types = {
    'n_nationkey': int,
    'n_name': str,
    'n_regionkey': int,
    'n_comment': str,
}

n_parse_dates = []

# Don't set indexes, as we can't access them with Pandas selection!
nation = pd.read_table("data/nation.tbl.csv", sep="|", names=n_columnnames, dtype=n_data_types, parse_dates=n_parse_dates)

# Supplier

s_columnnames = ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"]

for i in range(len(s_columnnames)):
    s_columnnames[i] = s_columnnames[i].lower()

s_data_types = {
    's_suppkey': int,
    's_name': str,
    's_address': str,
    's_nationkey': int,
    's_phone': str,
    's_acctbal': float,
    's_comment': str
}

s_parse_dates = []

# Don't set indexes, as we can't access them with Pandas selection!
supplier = pd.read_table("data/supplier.tbl.csv", sep="|", names=s_columnnames, dtype=s_data_types, parse_dates=s_parse_dates)

# Partsupp

ps_columnnames = ["PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY", "PS_SUPPLYCOST", "PS_COMMENT"]

for i in range(len(ps_columnnames)):
    ps_columnnames[i] = ps_columnnames[i].lower()

ps_data_types = {
    'ps_partkey': int,
    'ps_suppkey': int,
    'ps_availqty': int,
    'ps_supplycost': float,
    'ps_comment': str
}

ps_parse_dates = []

# Don't set indexes, as we can't access them with Pandas selection!
partsupp = pd.read_table("data/partsupp.tbl.csv", sep="|", names=ps_columnnames, dtype=ps_data_types, parse_dates=ps_parse_dates)
