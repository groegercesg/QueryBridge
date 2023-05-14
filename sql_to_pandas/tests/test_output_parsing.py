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
    intended_tree = Tree('equate', [Tree('col_ref', [Token('WORD', 'ps'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'suppkey')]), Tree('col_ref', [Token('WORD', 's'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'suppkey')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

# l_shipdate<=1998-09-02 AND l_shipdate IS NOT NULL
# p_size=15 AND p_size IS NOT NULL
# suffix(p_type, 'BRASS')
# (((n_name = 'FRANCE') AND (n_name = 'GERMANY')) OR ((n_name = 'GERMANY') AND (n_name = 'FRANCE')))
# sum((partsupp.ps_supplycost * partsupp.ps_availqty)) DESC
# CASE  WHEN (((o_orderpriority = '1-URGENT') OR (o_orderpriority = '2-HIGH'))) THEN (1) ELSE 0 END\nCASE  WHEN (((o_orderpriority != '1-URGENT') AND (o_orderpriority != '2-HIGH'))) THEN (1) ELSE 0 END
# ((l_commitdate < l_receiptdate) AND (l_shipdate < l_commitdate) AND ((l_shipmode = 'MAIL') OR (l_shipmode = 'SHIP')))
# count(DISTINCT #3)
# (sum(l_quantity) > 300.000)
# ((l_shipmode = 'AIR') OR (l_shipmode = 'AIR REG'))
# n_name=SAUDI ARABIA AND n_name IS NOT NULL