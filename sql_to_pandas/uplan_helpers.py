import random

from expression_operators import *

def handleEmptyCodeName(value, previousColumns):
    # Takes a expr with no codename, at the top level
    # Use the subnodes to create one
    # If this already exists, add randomness
    def getRelevantStrings(value):
        # Preorder Traversal
        # Current
        current_strings = []
        
        match value:
            case SumAggrOperator():
                current_strings.append("sum")
            case ColumnValue():
                current_strings.append(value.codeName)
            case MulOperator():
                current_strings.append("mul")
            case SubOperator():
                current_strings.append("sub")
            case ConstantValue():
                pass
            case MaxAggrOperator():
                current_strings.append("max")
            case CaseOperator():
                current_strings.append("case")
            case CountAllOperator():
                current_strings.append("count")
            case AddOperator():
                current_strings.append("add")
            case DivOperator():
                current_strings.append("div")
            case CountDistinctAggrOperator():
                current_strings.append("countdistinct")
            case ExtractYearOperator():
                current_strings.append("extractyear")
            case _:
                raise Exception(f"Unknown operator: {type(value)}")
        
        # Visit Children
        if isinstance(value, BinaryExpressionOperator):
            leftStrings = getRelevantStrings(value.left)
            rightStrings = getRelevantStrings(value.right)
            
            current_strings.extend(leftStrings)
            current_strings.extend(rightStrings)
        elif isinstance(value, UnaryExpressionOperator):
            childStrings = getRelevantStrings(value.child)
            
            current_strings.extend(childStrings)
        else:
            # A value node
            assert isinstance(value, LeafNode)
            pass
        
        return current_strings
    
    assert value.codeName == ""
    discovered_strings = getRelevantStrings(value)
    initialCodeName = "_".join(discovered_strings)
    while initialCodeName in previousColumns:
        # It's in the parser Created set
        # Add randomness
        initialCodeName = f"{initialCodeName}{random.randint(0,9)}"
    value.codeName = initialCodeName
