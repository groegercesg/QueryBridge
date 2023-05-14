from ..output_parse import parse
from lark import Tree, Token

"""

"""

def test_parse_table_column():
    in_string = "lineitem.l_returnflag"
    out_tree = parse(in_string)
    intended_tree = Tree('table_ref', [Token('WORD', 'lineitem'), Tree('col_ref', [Token('LETTER', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'returnflag')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_column():
    in_string = "s_numberofcats"
    out_tree = parse(in_string)
    intended_tree = Tree('col_ref', [Token('LETTER', 's'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'numberofcats')])
    
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
    intended_tree = Tree('avg', [Tree('col_ref', [Token('LETTER', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'extendedprice')])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"

def test_parse_nested():
    in_string = "l_extendedprice * (1 - l_discount)"
    out_tree = parse(in_string)
    intended_tree = Tree('mul', [Tree('col_ref', [Token('LETTER', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'extendedprice')]), Tree('sub', [Tree('number', [Token('NUMBER', '1')]), Tree('col_ref', [Token('LETTER', 'l'), Tree(Token('RULE', 'underscore'), []), Token('WORD', 'discount')])])])
    
    print("Out Tree:")
    print(out_tree)
    print(out_tree.pretty())
    print("Intended String:")
    print(intended_tree)

    assert out_tree == intended_tree, "Test Assertion Failed"