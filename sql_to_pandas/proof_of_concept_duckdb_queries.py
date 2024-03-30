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
    
    scan_lineitem
    
    return None
