# Use the lark package to parse strings from the Output into a tree-based class representation
import lark

def parse(text):
    parser = lark.Lark.open('grammars/duck.lark', rel_to=__file__, parser="earley", start="start")

    tree = parser.parse(text)

    return tree
