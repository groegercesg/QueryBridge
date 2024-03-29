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
    pass
    
    