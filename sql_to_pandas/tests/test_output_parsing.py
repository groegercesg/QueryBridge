from ..output_parse import parse
from lark import Tree, Token

"""

"""

def test_parse_table_column():
    in_string = "lineitem.l_returnflag"
    out_tree = parse(in_string)
    intended_tree = Tree('table_ref', [Token('WORD', 'lineitem'), Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'returnflag')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_column():
    in_string = "s_numberofcats"
    out_tree = parse(in_string)
    intended_tree = Tree('col_ref', [Token('WORD', 's'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'numberofcats')])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"
    
def test_parse_count_star():
    in_string = "count(*)"
    out_tree = parse(in_string)
    intended_tree = Tree('count_star', [])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_avg():
    in_string = "avg(l_extendedprice)"
    out_tree = parse(in_string)
    intended_tree = Tree('avg', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'extendedprice')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_nested():
    in_string = "l_extendedprice * (1 - l_discount)"
    out_tree = parse(in_string)
    intended_tree = Tree('mul', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'extendedprice')]), Tree('sub', [Tree('number', [Token('NUMBER', '1')]), Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'discount')])])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_sort_desc():
    in_string = "supplier.s_acctbal DESC"
    out_tree = parse(in_string)
    intended_tree = Tree('sort_desc', [Tree('table_ref', [Token('WORD', 'supplier'), Tree('col_ref', [Token('WORD', 's'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'acctbal')])])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_equate():
    in_string = "ps_suppkey = s_suppkey"
    out_tree = parse(in_string)
    intended_tree = Tree('eq', [Tree('col_ref', [Token('WORD', 'ps'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'suppkey')]), Tree('col_ref', [Token('WORD', 's'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'suppkey')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_date():
    in_string = "l_shipdate=1998-09-02"
    out_tree = parse(in_string)
    intended_tree = Tree('eq', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'shipdate')]), Tree('date', [Token('YEAR', '1998'), Token('MONTH', '09'), Token('DAY', '02')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_leq():
    in_string = "l_shipdate<=1998-09-02"
    out_tree = parse(in_string)
    intended_tree = Tree('leq', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'shipdate')]), Tree('date', [Token('YEAR', '1998'), Token('MONTH', '09'), Token('DAY', '02')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_is_not_null():
    in_string = "l_shipdate IS NOT NULL"
    out_tree = parse(in_string)
    intended_tree = Tree('isnotnull', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'shipdate')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_and():
    in_string = "l_shipdate<=1998-09-02 AND l_shipdate IS NOT NULL"
    out_tree = parse(in_string)
    intended_tree = Tree('and', [Tree('leq', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'shipdate')]), Tree('date', [Token('YEAR', '1998'), Token('MONTH', '09'), Token('DAY', '02')])]), Tree('isnotnull', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'shipdate')])])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_or():
    in_string = "p_size=15 OR p_size IS NOT NULL"
    out_tree = parse(in_string)
    intended_tree = Tree('or', [Tree('eq', [Tree('col_ref', [Token('WORD', 'p'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'size')]), Tree('number', [Token('NUMBER', '15')])]), Tree('isnotnull', [Tree('col_ref', [Token('WORD', 'p'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'size')])])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_sum_sort_desc():
    in_string = "sum((partsupp.ps_supplycost * partsupp.ps_availqty)) DESC"
    out_tree = parse(in_string)
    intended_tree = Tree('sort_desc', [Tree('sum', [Tree('mul', [Tree('table_ref', [Token('WORD', 'partsupp'), Tree('col_ref', [Token('WORD', 'ps'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'supplycost')])]), Tree('table_ref', [Token('WORD', 'partsupp'), Tree('col_ref', [Token('WORD', 'ps'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'availqty')])])])])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_equate_string():
    in_string = "n_name = 'FRANCE'"
    out_tree = parse(in_string)
    intended_tree = Tree('eq', [Tree('col_ref', [Token('WORD', 'n'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'name')]), Tree('string', [Token('STRING', 'FRANCE')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_negate_string():
    in_string = "o_orderpriority != '1-URGENT'"
    out_tree = parse(in_string)
    intended_tree = Tree('neq', [Tree('col_ref', [Token('WORD', 'o'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'orderpriority')]), Tree('string', [Token('STRING', '1-URGENT')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_and_or_complex():
    in_string = "(((n_name = 'FRANCE') AND (n_name = 'GERMANY')) OR ((n_name = 'GERMANY') AND (n_name = 'FRANCE')))"
    out_tree = parse(in_string)
    intended_tree = Tree('or', [Tree('and', [Tree('eq', [Tree('col_ref', [Token('WORD', 'n'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'name')]), Tree('string', [Token('STRING', 'FRANCE')])]), Tree('eq', [Tree('col_ref', [Token('WORD', 'n'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'name')]), Tree('string', [Token('STRING', 'GERMANY')])])]), Tree('and', [Tree('eq', [Tree('col_ref', [Token('WORD', 'n'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'name')]), Tree('string', [Token('STRING', 'GERMANY')])]), Tree('eq', [Tree('col_ref', [Token('WORD', 'n'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'name')]), Tree('string', [Token('STRING', 'FRANCE')])])])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_and_or_complex_2():
    in_string = "((l_commitdate < l_receiptdate) AND (l_shipdate < l_commitdate) AND ((l_shipmode = 'MAIL') OR (l_shipmode = 'SHIP')))"
    out_tree = parse(in_string)
    intended_tree = Tree('and', [Tree('and', [Tree('lt', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'commitdate')]), Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'receiptdate')])]), Tree('lt', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'shipdate')]), Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'commitdate')])])]), Tree('or', [Tree('eq', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'shipmode')]), Tree('string', [Token('STRING', 'MAIL')])]), Tree('eq', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'shipmode')]), Tree('string', [Token('STRING', 'SHIP')])])])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_or_complex():
    in_string = "((l_shipmode = 'AIR') OR (l_shipmode = 'AIR REG'))"
    out_tree = parse(in_string)
    intended_tree = Tree('or', [Tree('eq', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'shipmode')]), Tree('string', [Token('STRING', 'AIR')])]), Tree('eq', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'shipmode')]), Tree('string', [Token('STRING', 'AIR'), Token('STRING', 'REG')])])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)
    print(intended_tree.pretty())

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_gt_number():
    in_string = "(sum(l_quantity) > 300.000)"
    out_tree = parse(in_string)
    intended_tree = Tree('gt', [Tree('sum', [Tree('col_ref', [Token('WORD', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'quantity')])]), Tree('number', [Token('NUMBER', '300.000')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_countdistinct():
    in_string = "count(DISTINCT o_orderpriority)"
    out_tree = parse(in_string)
    intended_tree = Tree('countdistinct', [Tree('col_ref', [Token('WORD', 'o'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'orderpriority')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_suffix():
    in_string = "suffix(p_type, 'BRASS')"
    out_tree = parse(in_string)
    intended_tree = Tree('suffix', [Tree('col_ref', [Token('WORD', 'p'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'type')]), Tree('string', [Token('STRING', 'BRASS')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"


# CASE  WHEN (((o_orderpriority = '1-URGENT') OR (o_orderpriority = '2-HIGH'))) THEN (1) ELSE 0 END
# CASE  WHEN (((o_orderpriority != '1-URGENT') AND (o_orderpriority != '2-HIGH'))) THEN (1) ELSE 0 END
# n_name=SAUDI ARABIA AND n_name IS NOT NULL