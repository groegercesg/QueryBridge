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
    final_groupby = DHashGroupBy([p_brand, p_type, p_size], [count_distinct])
    final_groupby.setCardinality(18314)
    final_groupby.addID(9)
    final_groupby.addChild(join_mark_subquery)
    
    return final_groupby

def Query21():
    return None
