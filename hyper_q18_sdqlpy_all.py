from sdqlpy.sdql_lib import *
from sdqlpy_benchmark_runner import bench_runner

print("Starting to Load Data")

dataset_path = "/home/callum/Documents/Academia/University/Year4/PROJ/dataframe-sql-benchmark/data_storage/"

customer_type = {record({"c_custkey": int, "c_name": string(25), "c_address": string(40), "c_nationkey": int, "c_phone": string(15), "c_acctbal": float, "c_mktsegment": string(10), "c_comment": string(117), "c_NA": string(1)}): bool}
lineitem_type = {record({"l_orderkey": int, "l_partkey": int, "l_suppkey": int, "l_linenumber": int, "l_quantity": float, "l_extendedprice": float, "l_discount": float, "l_tax": float, "l_returnflag": string(1), "l_linestatus": string(1), "l_shipdate": date, "l_commitdate": date, "l_receiptdate": date, "l_shipinstruct": string(25), "l_shipmode": string(10), "l_comment": string(44), "l_NA": string(1)}): bool}
orders_type = {record({"o_orderkey": int, "o_custkey": int, "o_orderstatus": string(1), "o_totalprice": float, "o_orderdate": date, "o_orderpriority": string(15), "o_clerk": string(15), "o_shippriority": int, "o_comment": string(79), "o_NA": string(1)}): bool}

customer = read_csv(dataset_path + "customer.tbl.csv", customer_type, "customer")
lineitem = read_csv(dataset_path + "lineitem.tbl.csv", lineitem_type, "lineitem")
orders = read_csv(dataset_path + "orders.tbl.csv", orders_type, "orders")
print("Data Loaded")

sdqlpy_init(1, 1)

@sdql_compile({'customer': customer_type, 'lineitem': lineitem_type, 'orders': orders_type})
def q18(customer, lineitem, orders):
    indexed_1 = customer.sum(
        lambda p:
            {
                record({"c_custkey": p[0].c_custkey}):
                record({"c_name": p[0].c_name, "c_custkey": p[0].c_custkey})
            }
        , True
    )
    group_1 = lineitem.sum(lambda p :
        {
            record({"l_orderkey": p[0].l_orderkey}):
            record({"sum_l_quantity": p[0].l_quantity})
        }
    )
    indexed_2 = group_1.sum(
        lambda p:
            {
                record({"l_orderkey": p[0].l_orderkey}):
                record({"l_orderkey": p[0].l_orderkey, "sum_l_quantity": p[1].sum_l_quantity})
            }
        if
            (p[1].sum_l_quantity > 300.0)
        else
            None
        , True
    )
    join_1 = orders.sum(
        lambda p : 
            {
                record({"o_orderkey": p[0].o_orderkey, "o_totalprice": p[0].o_totalprice, "o_custkey": p[0].o_custkey, "o_orderdate": p[0].o_orderdate}):
                True
            }
        if
            indexed_2[record({'l_orderkey': p[0].o_orderkey})] != None
        else
            None
        , True
    )
    join_2 = join_1.sum(
        lambda p : 
            {
                record({"o_orderkey": p[0].o_orderkey}):
                record({"c_custkey": indexed_1[record({'c_custkey': p[0].o_custkey})].c_custkey, "o_orderkey": p[0].o_orderkey, "o_totalprice": p[0].o_totalprice, "o_custkey": p[0].o_custkey, "o_orderdate": p[0].o_orderdate, "c_name": indexed_1[record({'c_custkey': p[0].o_custkey})].c_name})
            }
        if
            indexed_1[record({'c_custkey': p[0].o_custkey})] != None
        else
            None
        , True
    )
    join_3 = lineitem.sum(
        lambda p : 
            {
                record({"c_name": join_2[record({'o_orderkey': p[0].l_orderkey})].c_name, "c_custkey": join_2[record({'o_orderkey': p[0].l_orderkey})].c_custkey, "o_orderkey": join_2[record({'o_orderkey': p[0].l_orderkey})].o_orderkey, "o_orderdate": join_2[record({'o_orderkey': p[0].l_orderkey})].o_orderdate, "o_totalprice": join_2[record({'o_orderkey': p[0].l_orderkey})].o_totalprice}):
                record({"sum": p[0].l_quantity})
            }
        if
            join_2[record({'o_orderkey': p[0].l_orderkey})] != None
        else
            None
        , True
    )
    results = join_3.sum(lambda p : {unique(p[0].concat(p[1])): True})
    return results

################
query_function = q18
query_tables = [customer, lineitem, orders]
query_columns = ['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice', 'sum']
iterations = 6
data_write_path = "QBXMUEVGNLTF.json"

bench_runner(iterations, query_function, query_tables, query_columns, data_write_path)
