                
def configure_table_schema(inSchema: dict()) -> dict():
    if inSchema == dict():
        # None passed, use TPC-H
        schema_tpcH = {
            'region': ({('r_regionkey',)}, {}, ['r_regionkey', 'r_name', 'r_comment']), 
            'part': ({('p_partkey',)}, {}, ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment']), 
            'nation': ({('n_nationkey',)}, {'n_regionkey': ('r_regionkey', 'region')}, ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']), 
            'lineitem': ({('l_orderkey', 'l_linenumber')}, {'l_orderkey': ('orders', 'o_orderkey'), ('l_partkey', 'l_suppkey'): ('partsupp', ('ps_partkey', 'ps_suppkey')), 'l_partkey': ('partsupp', 'ps_partkey'), 'l_suppkey': ('partsupp', 'ps_suppkey')}, ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']), 
            'partsupp': ({('ps_partkey', 'ps_suppkey')}, {'ps_partkey': ('p_partkey', 'part'), 'ps_suppkey': ('s_suppkey', 'supplier')}, ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']), 
            'supplier': ({('s_suppkey',)}, {'s_nationkey': ('n_nationkey', 'nation')}, ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']), 
            'orders': ({('o_orderkey',)}, {'o_custkey': ('c_custkey', 'customer')}, ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']), 
            'customer': ({('c_custkey',)}, {'c_nationkey': ('n_nationkey', 'nation')}, ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'])
        }
        return schema_tpcH
    else:
        return inSchema