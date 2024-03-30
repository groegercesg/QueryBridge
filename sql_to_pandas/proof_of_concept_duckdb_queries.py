from duckdb_nodes import *
from expression_operators import *
import datetime

def Query6():
    l_shipdate = ColumnValue("l_shipdate", "Date")
    l_discount = ColumnValue("l_discount", "Double")
    l_linenumber = ColumnValue("l_linenumber", "Double")
    l_orderkey = ColumnValue("l_orderkey", "Integer")
    l_quantity = ColumnValue("l_quantity", "Integer")
    l_extendedprice = ColumnValue("l_extendedprice", "Double")
    l_partkey = ColumnValue("l_partkey", "Integer")
    l_suppkey = ColumnValue("l_suppkey", "Integer")
    
    l_shipdate.essential = True
    l_discount.essential = True
    l_linenumber.essential = True
    l_orderkey.essential = True
    l_quantity.essential = True
    l_extendedprice.essential = True
    l_partkey.essential = True
    l_suppkey.essential = True
    
    lineitem_columns = [
        l_orderkey,
        l_linenumber,
        l_partkey,
        l_suppkey,
        l_shipdate,
        l_discount,
        l_quantity,
        l_extendedprice
    ]
    
    shipdate_and = AndOperator()
    shipdate_geq = GreaterThanEqOperator()
    shipdate_geq.addLeft(l_shipdate)
    shipdate_geq.addRight(ConstantValue(datetime.date(1994, 1, 1), "Datetime"))
    shipdate_l = LessThanOperator()
    shipdate_l.addLeft(l_shipdate)
    shipdate_l.addRight(ConstantValue(datetime.date(1995, 1, 1), "Datetime"))
    shipdate_and.addLeft(shipdate_geq)
    shipdate_and.addRight(shipdate_l)
    discount_and = AndOperator()
    discount_geq = GreaterThanEqOperator()
    discount_geq.addLeft(l_discount)
    discount_geq.addRight(ConstantValue(0.05, "Float"))
    discount_leq = LessThanEqOperator()
    discount_leq.addLeft(l_discount)
    discount_leq.addRight(ConstantValue(0.07, "Float"))
    discount_and.addLeft(discount_geq)
    discount_and.addRight(discount_leq)
    l_cond_left = AndOperator()
    l_cond_left.addLeft(shipdate_and)
    l_cond_left.addRight(discount_and)
    quantity_l = LessThanOperator()
    quantity_l.addLeft(l_quantity)
    quantity_l.addRight(ConstantValue(24, "Integer"))
    
    lineitem_condition = AndOperator()
    lineitem_condition.addLeft(l_cond_left)
    lineitem_condition.addRight(quantity_l)
    scan_lineitem = DSeqScan("lineitem", lineitem_columns, lineitem_condition)
    scan_lineitem.setCardinality(114160)
    scan_lineitem.addID(1)
    
    
    mul_extended_discount = MulOperator()
    mul_extended_discount.addLeft(l_extendedprice)
    mul_extended_discount.addRight(l_discount)
    mul_extended_discount.codeName = "mul_extended_discount"
    sum_eqn = SumAggrOperator()
    sum_eqn.addChild(mul_extended_discount)
    
    mult = DSimpleAggregate([sum_eqn])
    mult.addChild(scan_lineitem)
    mult.setCardinality(1)
    mult.addID(2)
    
    return mult

def Query16():
    p_partkey = ColumnValue("p_partkey", "Integer")
    p_brand = ColumnValue("p_brand", "Varchar")
    p_type = ColumnValue("p_type", "Varchar")
    p_size = ColumnValue("p_size", "Integer")
    
    p_partkey.essential = True
    p_brand.essential = True
    p_type.essential = True
    p_size.essential = True
    
    part_columns = [
        p_partkey,
        p_brand,
        p_type,
        p_size
    ]
    
    scan_part = DSeqScan("part", part_columns, None)
    scan_part.setCardinality(200000)
    scan_part.addID(1)
    
    chunk = DChunkScan()
    chunk.setCardinality(8)
    chunk.addID(2)
    
    p_size_set = InSetOperator()
    p_size_set.addChild(p_size)
    p_size_set.addToSet(ConstantValue(49, "Integer"))
    p_size_set.addToSet(ConstantValue(14, "Integer"))
    p_size_set.addToSet(ConstantValue(23, "Integer"))
    p_size_set.addToSet(ConstantValue(45, "Integer"))
    p_size_set.addToSet(ConstantValue(19, "Integer"))
    p_size_set.addToSet(ConstantValue(3, "Integer"))
    p_size_set.addToSet(ConstantValue(36, "Integer"))
    p_size_set.addToSet(ConstantValue(9, "Integer"))
    p_brand_check = NotEqualsOperator()
    p_brand_check.addLeft(p_brand)
    p_brand_check.addRight(ConstantValue("Brand#45", "String"))
    p_type_not = NotOperator()
    p_type_not.addChild(LikeOperator(p_type, ConstantValue("MEDIUM POLISHED%", "String")))
    join_mark_cond_l = AndOperator()
    join_mark_cond_l.addLeft(p_size_set)
    join_mark_cond_l.addRight(p_brand_check)
    
    join_mark_condition = AndOperator()
    join_mark_condition.addLeft(join_mark_cond_l)
    join_mark_condition.addRight(p_type_not)
    
    join_mark = DHashJoinNode("MARK", join_mark_condition, [], [])
    join_mark.setCardinality(200000)
    join_mark.addID(3)
    join_mark.addLeft(scan_part)
    join_mark.addRight(chunk)
    
    ps_partkey = ColumnValue("ps_partkey", "Integer")
    ps_suppkey = ColumnValue("ps_suppkey", "Integer")
    
    ps_partkey.essential = True
    ps_suppkey.essential = True
    
    partsupp_columns = [
        ps_partkey,
        ps_suppkey
    ]
    
    scan_partsupp = DSeqScan("partsupp", partsupp_columns, None)
    scan_partsupp.setCardinality(800000)
    scan_partsupp.addID(4)
    
    join_inner_cond = EqualsOperator()
    join_inner_cond.addLeft(ps_partkey)
    join_inner_cond.addRight(p_partkey)
    join_inner = DHashJoinNode("INNER", join_inner_cond, [ps_partkey], [p_partkey])
    join_inner.setCardinality(118324)
    join_inner.addID(5)
    join_inner.addLeft(scan_partsupp)
    join_inner.addRight(join_mark)
    
    s_nationkey = ColumnValue("s_nationkey", "Integer")
    s_comment = ColumnValue("s_comment", "Varchar")
    s_suppkey = ColumnValue("s_suppkey", "Integer")
    s_nationkey.essential = True
    s_comment.essential = True
    s_suppkey.essential = True
    
    supplier_columns = [
        s_nationkey,
        s_comment,
        s_suppkey
    ]
    
    scan_supplier = DSeqScan("supplier", supplier_columns, None)
    scan_supplier.setCardinality(10000)
    scan_supplier.addID(6)
    
    supplier_cond = LikeOperator(s_comment, ConstantValue("%Customer%Compliants%", "String"))
    filter_supplier = DFilter(supplier_cond)
    filter_supplier.setCardinality(4)
    filter_supplier.addID(7)
    filter_supplier.addChild(scan_supplier)
    
    join_mark_subquery_cond = EqualsOperator()
    join_mark_subquery_cond.addLeft(ps_suppkey)
    not_subquery = NotOperator()
    not_subquery.addChild(DSubqueryOp())
    join_mark_subquery_cond.addRight(not_subquery)
    
    join_mark_subquery = DHashJoinNode("MARK", join_mark_subquery_cond, [s_suppkey], [ps_suppkey])
    join_mark_subquery.setCardinality(118274)
    join_mark_subquery.addID(8)
    join_mark_subquery.addLeft(filter_supplier)
    join_mark_subquery.addRight(join_inner)
    
    count_distinct = CountDistinctAggrOperator()
    count_distinct.addChild(ps_suppkey)
    count_distinct.codeName = "supplier_cnt"
    final_groupby = DHashGroupBy([p_brand, p_type, p_size], [count_distinct])
    final_groupby.setCardinality(18314)
    final_groupby.addID(9)
    final_groupby.addChild(join_mark_subquery)
    
    return final_groupby

def Query3():
    
    l_linenumber = ColumnValue("l_linenumber", "Integer")
    l_orderkey = ColumnValue("l_orderkey", "Integer")
    l_shipdate = ColumnValue("l_shipdate", "Date")
    l_extendedprice = ColumnValue("l_extendedprice", "Double")
    l_discount = ColumnValue("l_discount", "Double")
    l_partkey = ColumnValue("l_partkey", "Integer")
    l_suppkey = ColumnValue("l_suppkey", "Integer")
    
    l_linenumber.essential = True
    l_orderkey.essential = True
    l_shipdate.essential = True
    l_extendedprice.essential = True
    l_discount.essential = True
    l_partkey.essential = True
    l_suppkey.essential = True
    
    lineitem_columns = [
        l_linenumber,
        l_orderkey,
        l_shipdate,
        l_extendedprice,
        l_discount,
        l_partkey,
        l_suppkey
    ]
    
    lineitem_cond = GreaterThanOperator()
    lineitem_cond.addLeft(l_shipdate)
    lineitem_cond.addRight(ConstantValue(datetime.date(1995, 3, 15), "Datetime"))
    
    scan_lineitem = DSeqScan("lineitem", lineitem_columns, lineitem_cond)
    scan_lineitem.setCardinality(3241776)
    scan_lineitem.addID(1)
    
    o_custkey = ColumnValue("o_custkey", "Integer")
    o_orderkey = ColumnValue("o_orderkey", "Integer")
    o_orderdate = ColumnValue("o_orderdate", "Date")
    o_shippriority = ColumnValue("o_shippriority", "Integer")
    
    o_custkey.essential = True
    o_orderkey.essential = True
    o_orderdate.essential = True
    o_shippriority.essential = True
    
    orders_columns = [
        o_custkey,
        o_orderkey,
        o_orderdate,
        o_shippriority
    ]
    
    order_cond = LessThanOperator()
    order_cond.addLeft(o_orderdate)
    order_cond.addRight(ConstantValue(datetime.date(1995, 3, 15), "Datetime"))
    
    scan_orders = DSeqScan("orders", orders_columns, order_cond)
    scan_orders.setCardinality(727305)
    scan_orders.addID(2)
    
    
    initial_join_cond = EqualsOperator()
    initial_join_cond.addLeft(l_orderkey)
    initial_join_cond.addRight(o_orderkey)
    initial_join = DHashJoinNode("INNER", initial_join_cond, [l_orderkey], [o_orderkey])
    initial_join.setCardinality(151331)
    initial_join.addID(3)
    initial_join.addLeft(scan_lineitem)
    initial_join.addRight(scan_orders)
    
    c_mktsegment = ColumnValue("c_mktsegment", "Varchar")
    c_custkey = ColumnValue("c_custkey", "Integer")
    c_nationkey = ColumnValue("c_nationkey", "Integer")
    
    c_mktsegment.essential = True
    c_custkey.essential = True
    c_nationkey.essential = True
    
    customer_columns = [
        c_mktsegment,
        c_custkey,
        c_nationkey
    ]
    
    customer_cond = EqualsOperator()
    customer_cond.addLeft(c_mktsegment)
    customer_cond.addRight(ConstantValue("BUILDING", "String"))
    
    scan_customer = DSeqScan("customer", customer_columns, customer_cond)
    scan_customer.setCardinality(151331)
    scan_customer.addID(4)
    
    top_join_cond = EqualsOperator()
    top_join_cond.addLeft(o_custkey)
    top_join_cond.addRight(c_custkey)
    top_join = DHashJoinNode("INNER", top_join_cond, [o_custkey], [c_custkey])
    top_join.setCardinality(30519)
    top_join.addID(5)
    top_join.addLeft(initial_join)
    top_join.addRight(scan_customer)
    
    subtract = SubOperator()
    subtract.addLeft(ConstantValue(1, "Integer"))
    subtract.addRight(l_discount)
    mult = MulOperator()
    mult.addLeft(l_extendedprice)
    mult.addRight(subtract)
    sum_mult = SumAggrOperator()
    sum_mult.addChild(mult)
    sum_mult.codeName = "revenue"
    final_ops = [sum_mult]
    final_group = DHashGroupBy([l_orderkey, o_orderdate, o_shippriority], final_ops)
    final_group.setCardinality(11620)
    final_group.addID(6)
    final_group.addChild(top_join)
    
    return final_group

# The query with delimjoins

def Query21():
    
    l_orderkey_1 = ColumnValue("l_orderkey", "Integer")
    l_suppkey_1 = ColumnValue("l_suppkey", "Integer")
    l_receiptdate_1 = ColumnValue("l_receiptdate", "Date")
    l_commitdate_1 = ColumnValue("l_commitdate", "Date")
    l_linenumber_1 = ColumnValue("l_linenumber", "Integer")
    l_partkey_1 = ColumnValue("l_partkey", "Integer")
    
    l_orderkey_1.essential = True
    l_suppkey_1.essential = True
    l_receiptdate_1.essential = True
    l_commitdate_1.essential = True
    l_linenumber_1.essential = True
    l_partkey_1.essential = True
    
    l_orderkey_2 = ColumnValue("l_orderkey", "Integer")
    l_suppkey_2 = ColumnValue("l_suppkey", "Integer")
    
    l_orderkey_2.essential = True
    l_suppkey_2.essential = True
    
    
    
    l_orderkey_3 = ColumnValue("l_orderkey", "Integer")
    l_suppkey_3 = ColumnValue("l_suppkey", "Integer")
    l_receiptdate_3 = ColumnValue("l_receiptdate", "Date")
    l_commitdate_3 = ColumnValue("l_commitdate", "Date")
    
    l_orderkey_3.essential = True
    l_suppkey_3.essential = True
    l_receiptdate_3.essential = True
    l_commitdate_3.essential = True
    
    lineitem_columns_1 = [
        l_orderkey_1,
        l_suppkey_1,
        l_receiptdate_1,
        l_commitdate_1,
        l_linenumber_1,
        l_partkey_1
    ]
    
    scan_lineitem_1 = DSeqScan("lineitem", lineitem_columns_1, None)
    scan_lineitem_1.setCardinality(6001215)
    scan_lineitem_1.addID(1)
    
    lineitem_1_cond = GreaterThanOperator()
    lineitem_1_cond.addLeft(l_receiptdate_1)
    lineitem_1_cond.addRight(l_commitdate_1)
    filter_lineitem_1 = DFilter(lineitem_1_cond)
    filter_lineitem_1.setCardinality(3793296)
    filter_lineitem_1.addID(2)
    filter_lineitem_1.addChild(scan_lineitem_1)
    
    delim_lineitem_1 = DDelimScan()
    delim_lineitem_1.setCardinality(0)
    delim_lineitem_1.addID(3)
    
    
    join_lineitem_1_cond_eq = EqualsOperator()
    # Changed: 11:49
    join_lineitem_1_cond_eq.addLeft(l_orderkey_1)
    join_lineitem_1_cond_eq.addRight(l_orderkey_3)
    
    join_lineitem_1_cond_neq = NotEqualsOperator()
    join_lineitem_1_cond_neq.addLeft(l_suppkey_1)
    join_lineitem_1_cond_neq.addRight(l_suppkey_3)
    
    join_lineitem_1_cond = AndOperator()
    join_lineitem_1_cond.addLeft(join_lineitem_1_cond_eq)
    join_lineitem_1_cond.addRight(join_lineitem_1_cond_neq)
    
    join_lineitem_1 = DHashJoinNode("INNER", join_lineitem_1_cond, [], [])
    join_lineitem_1.setCardinality(190909)
    join_lineitem_1.addID(4)
    join_lineitem_1.addLeft(delim_lineitem_1)
    join_lineitem_1.addRight(filter_lineitem_1)
    
    chunk_lineitem_1 = DChunkScan()
    chunk_lineitem_1.setCardinality(73089)
    chunk_lineitem_1.addID(5)
    
    join_anti_li_1_cond = AndOperator()
    l_orderkey_eq = EqualsOperator()
    l_orderkey_eq.addLeft(l_orderkey_3)
    l_orderkey_eq.addRight(l_orderkey_2)
    join_anti_li_1_cond.addLeft(l_orderkey_eq)
    
    l_suppkey_eq = EqualsOperator()
    l_suppkey_eq.addLeft(l_suppkey_2)
    l_suppkey_eq.addRight(l_suppkey_1)
    join_anti_li_1_cond.addRight(l_suppkey_eq)
    join_anti_li_1 = DHashJoinNode("ANTI", join_anti_li_1_cond, [], [])
    join_anti_li_1.setCardinality(4141)
    join_anti_li_1.addID(6)
    join_anti_li_1.addLeft(chunk_lineitem_1)
    join_anti_li_1.addRight(join_lineitem_1)
    
    
    
    # Next lineitem
    
    
    l_linenumber_2 = ColumnValue("l_linenumber", "Integer")
    l_partkey_2 = ColumnValue("l_partkey", "Integer")
    
    l_linenumber_2.essential = True
    l_partkey_2.essential = True
    
    lineitem_columns_2 = [
        l_orderkey_2,
        l_suppkey_2,
        l_linenumber_2,
        l_partkey_2
    ]
    
    scan_lineitem_2 = DSeqScan("lineitem", lineitem_columns_2, None)
    scan_lineitem_2.setCardinality(6001215)
    scan_lineitem_2.addID(7)
    
    delim_lineitem_2 = DDelimScan()
    delim_lineitem_2.setCardinality(0)
    delim_lineitem_2.addID(8)
    
    join_lineitem_2_cond_eq = EqualsOperator()
    join_lineitem_2_cond_eq.addLeft(l_orderkey_2)
    join_lineitem_2_cond_eq.addRight(l_orderkey_3)
    
    join_lineitem_2_cond_neq = NotEqualsOperator()
    join_lineitem_2_cond_neq.addLeft(l_suppkey_2)
    join_lineitem_2_cond_neq.addRight(l_suppkey_3)
    
    join_lineitem_2_cond = AndOperator()
    join_lineitem_2_cond.addLeft(join_lineitem_2_cond_eq)
    join_lineitem_2_cond.addRight(join_lineitem_2_cond_neq)
    
    join_lineitem_2 = DHashJoinNode("INNER", join_lineitem_2_cond, [], [])
    join_lineitem_2.setCardinality(302356)
    join_lineitem_2.addID(9)
    join_lineitem_2.addLeft(delim_lineitem_2)
    join_lineitem_2.addRight(scan_lineitem_2)
    
    
    chunk_lineitem_2 = DChunkScan()
    chunk_lineitem_2.setCardinality(75871)
    chunk_lineitem_2.addID(10)
    
    
    # Lineitem 3
    
    
    
    join_semi_li_2_cond = AndOperator()
    l_orderkey_eq = EqualsOperator()
    l_orderkey_eq.addLeft(l_orderkey_2)
    l_orderkey_eq.addRight(l_orderkey_3)
    join_semi_li_2_cond.addLeft(l_orderkey_eq)
    
    l_suppkey_eq = EqualsOperator()
    l_suppkey_eq.addLeft(l_suppkey_2)
    l_suppkey_eq.addRight(l_suppkey_3)
    join_semi_li_2_cond.addRight(l_suppkey_eq)
    
    join_semi_li_2 = DHashJoinNode("SEMI", join_semi_li_2_cond, [], [])
    join_semi_li_2.setCardinality(73089)
    join_semi_li_2.addID(11)
    join_semi_li_2.addLeft(chunk_lineitem_2)
    join_semi_li_2.addRight(join_lineitem_2)
    
    
    l_linenumber_3 = ColumnValue("l_linenumber", "Integer")
    l_partkey_3 = ColumnValue("l_partkey", "Integer")
    
    l_linenumber_3.essential = True
    l_partkey_3.essential = True
    
    lineitem_columns_3 = [
        l_orderkey_3,
        l_suppkey_3,
        l_receiptdate_3,
        l_commitdate_3,
        l_linenumber_3,
        l_partkey_3
    ]
    
    scan_lineitem_3 = DSeqScan("lineitem", lineitem_columns_3, None)
    scan_lineitem_3.setCardinality(6001215)
    scan_lineitem_3.addID(12)
    
    lineitem_3_cond = GreaterThanOperator()
    lineitem_3_cond.addLeft(l_receiptdate_3)
    lineitem_3_cond.addRight(l_commitdate_3)
    filter_lineitem_3 = DFilter(lineitem_3_cond)
    filter_lineitem_3.setCardinality(3793296)
    filter_lineitem_3.addID(13)
    filter_lineitem_3.addChild(scan_lineitem_3)
    
    o_orderkey = ColumnValue("o_orderkey", "Integer")
    o_orderstatus = ColumnValue("o_orderstatus", "Integer")
    o_custkey = ColumnValue("o_custkey", "Integer")
    
    o_orderkey.essential = True
    o_orderstatus.essential = True
    o_custkey.essential = True
    
    orders_columns = [
        o_orderkey,
        o_orderstatus,
        o_custkey
    ]
    
    scan_orders_cond = EqualsOperator()
    scan_orders_cond.addLeft(o_orderstatus)
    scan_orders_cond.addRight(ConstantValue("F", "String"))
    scan_orders = DSeqScan("orders", orders_columns, scan_orders_cond)
    scan_orders.setCardinality(729413)
    scan_orders.addID(14)
    
    join_lineitem_3_orders_cond = EqualsOperator()
    join_lineitem_3_orders_cond.addLeft(l_orderkey_3)
    join_lineitem_3_orders_cond.addRight(o_orderkey)
    join_lineitem_3_orders = DHashJoinNode("INNER", join_lineitem_3_orders_cond, [l_orderkey_3], [o_orderkey])
    join_lineitem_3_orders.setCardinality(1828911)
    join_lineitem_3_orders.addID(15)
    join_lineitem_3_orders.addLeft(filter_lineitem_3)   
    join_lineitem_3_orders.addRight(scan_orders)
    
    
    s_suppkey = ColumnValue("s_suppkey", "Integer")
    s_nationkey = ColumnValue("s_nationkey", "Integer")
    s_name = ColumnValue("s_name", "Varchar")
    
    s_suppkey.essential = True
    s_nationkey.essential = True
    s_name.essential = True
    
    supplier_columns = [
        s_suppkey,
        s_nationkey,
        s_name
    ]
    
    scan_supplier = DSeqScan("supplier", supplier_columns, None)
    scan_supplier.setCardinality(10000)
    scan_supplier.addID(16)
    
    
    n_nationkey = ColumnValue("n_nationkey", "Integer")
    n_name = ColumnValue("n_name", "Varchar")
    n_regionkey = ColumnValue("n_regionkey", "Integer")
    
    n_nationkey.essential = True
    n_name.essential = True
    n_regionkey.essential = True
    
    nation_columns = [
        n_nationkey,
        n_name,
        n_regionkey
    ]
    
    scan_nation_cond = EqualsOperator()
    scan_nation_cond.addLeft(n_name)
    scan_nation_cond.addRight(ConstantValue("SAUDI ARABIA", "String"))
    scan_nation = DSeqScan("nation", nation_columns, scan_nation_cond)
    scan_nation.setCardinality(1)
    scan_nation.addID(17)
    
    join_supplier_nation_cond = EqualsOperator()
    join_supplier_nation_cond.addLeft(s_nationkey)
    join_supplier_nation_cond.addRight(n_nationkey)
    
    join_supplier_nation = DHashJoinNode("INNER", join_supplier_nation_cond, [s_nationkey], [n_nationkey])
    join_supplier_nation.setCardinality(411)
    join_supplier_nation.addID(18)
    join_supplier_nation.addLeft(scan_supplier)
    join_supplier_nation.addRight(scan_nation)
    
    join_li_3_supp_nation_cond = EqualsOperator()
    join_li_3_supp_nation_cond.addLeft(l_suppkey_3)
    join_li_3_supp_nation_cond.addRight(s_suppkey)
    join_li_3_supp_nation = DHashJoinNode("INNER", join_li_3_supp_nation_cond, [l_suppkey_3], [s_suppkey])
    join_li_3_supp_nation.setCardinality(75871)
    join_li_3_supp_nation.addID(19)
    join_li_3_supp_nation.addLeft(join_lineitem_3_orders)
    join_li_3_supp_nation.addRight(join_supplier_nation)
    
    
    hash_group_by_1 = DHashGroupByLeaf()
    hash_group_by_1.setCardinality(75864)
    hash_group_by_1.addID(20)
    
    delim_join_1 = DDelimJoin("SEMI", [], [])
    delim_join_1.setCardinality(0)
    delim_join_1.addID(21)
    delim_join_1.addLeft(join_li_3_supp_nation)
    delim_join_1.addMiddle(join_semi_li_2)
    delim_join_1.addRight(hash_group_by_1)
    
    hash_group_by_2 = DHashGroupByLeaf()
    hash_group_by_2.setCardinality(73082)
    hash_group_by_2.addID(22)
    
    delim_join_2 = DDelimJoin("ANTI", [], [])
    delim_join_2.setCardinality(0)
    delim_join_2.addID(23)
    delim_join_2.addLeft(delim_join_1)
    delim_join_2.addMiddle(join_anti_li_1)
    delim_join_2.addRight(hash_group_by_2)
    
    count_star = CountAggrOperator()
    count_star.addChild(s_name)
    count_star.codeName = "numwait"
    
    final_group_by = DHashGroupBy([s_name], [count_star])
    final_group_by.setCardinality(411)
    final_group_by.addID(24)
    final_group_by.addChild(delim_join_2)
    
    return final_group_by
