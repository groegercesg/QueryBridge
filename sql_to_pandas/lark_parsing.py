# Use the lark package to parse strings from the Output into a tree-based class representation
import lark
from lark import ParseTree

def parse_larks(texts: list[str]) -> list[ParseTree]:
    lark_parse_trees = []
    for text in texts:
        if text != "":
            lark_parse_trees.append(parse_lark(text))
    return lark_parse_trees

def parse_lark(text):
    parser = lark.Lark.open('grammars/duck.lark', rel_to=__file__, parser="earley", start="start")

    tree = parser.parse(text)

    return tree
