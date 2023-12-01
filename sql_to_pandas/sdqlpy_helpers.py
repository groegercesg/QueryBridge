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

def setSourceNodeColumnValues(value, l_lambda_idx, l_columns, r_lambda_idx, r_columns, r_lambda_val=None, r_columns_vals=None):
    def check_and_fix_columns(columns):
        if isinstance(columns, set) and isinstance(next(iter(columns)), ExpressionBaseNode):
            return getCodeNameFromSetColumnValues(columns)
        elif (isinstance(columns, list) and (columns == [] or isinstance(columns[0], str))):
            return set(columns)
        elif isinstance(columns, list) and isinstance(columns[0], ExpressionBaseNode):
            return getCodeNameFromSetColumnValues(columns)
        elif isinstance(columns, str):
            return set([columns])
        else:
            raise Exception(f"Unexpected type for columns: {type(columns)}")
    
    # Check that we have required number of arguments
    assert (l_lambda_idx != None) and (l_columns != None) and (r_lambda_idx != None) and (r_columns != None)
    
    l_columns_str = check_and_fix_columns(l_columns)
    r_columns_str = check_and_fix_columns(r_columns)
    # Construct list of tupules:
    # [(Idx_name, columns_str), ...]
    sourcePairs = [(l_lambda_idx, l_columns_str), (r_lambda_idx, r_columns_str)]
    
    if (r_lambda_val != None) and (r_columns_vals != None):
        # Add the r_vals, if we have them
        r_vals_str = check_and_fix_columns(r_columns_vals)
        sourcePairs.append((r_lambda_val, r_vals_str))
    
    setSourceNodeColumnValuesNPairs(value, sourcePairs)

def getCodeNameFromSetColumnValues(columns):
    columns_str = list()
    for x in columns:
        # if isinstance(x, ColumnValue):
        #     assert x.codeName == x.value
        if x.created == False:
            columns_str.append(x.value)
        elif x.created == True:
            columns_str.append(x.codeName)
        else:
            raise Exception("Unexpected value for column creation")
    columns_str = set(columns_str)
        
    # Assert none are empty string
    assert not any([True for x in columns_str if x == ""])
    
    return columns_str
    
def setSourceNodeColumnValuesNPairs(value, sourcePairs):
    match value:
        case CaseOperator():
            # run on the case and outputValue of every caseInstance
            for caseInst in value.caseInstances:
                setSourceNodeColumnValuesNPairs(caseInst.case, sourcePairs)
                setSourceNodeColumnValuesNPairs(caseInst.outputValue, sourcePairs)
            # run on elseExpr
            setSourceNodeColumnValuesNPairs(value.elseExpr, sourcePairs)
        case SDQLpyThirdNodeWrapper():
            set_index = False
            for index_name, columns in sourcePairs:
                if value.target_key.codeName in columns:
                    assert set_index == False
                    value.set_sourceValue(index_name)
                    set_index = True
            
            if set_index == False:
                raise Exception("The ThirdNode Target Key must be in either Left or Right")
        case InSetOperator():
            set_index = False
            for index_name, columns in sourcePairs:
                if value.child.codeName in columns:
                    assert set_index == False
                    value.child.sourceNode = index_name
                    set_index = True
            
            if set_index == False:
                raise Exception(f"Value ({value.codeName}) wasn't in either left or right")
        case IntervalNotionOperator() | LikeOperator():
            set_index = False
            for index_name, columns in sourcePairs:
                if value.value.codeName in columns:
                    assert set_index == False
                    value.value.sourceNode = index_name
                    set_index = True
            
            if set_index == False:
                raise Exception(f"Value ({value.codeName}) wasn't in either left or right")
        case ColumnValue():
            set_index = False
            for index_name, columns in sourcePairs:
                if value.value in columns:
                    assert set_index == False
                    if value.sourceNode == None:
                        value.sourceNode = index_name
                        set_index = True
                    else:
                        assert value.sourceNode == index_name
                        set_index = True
                elif value.codeName in columns:
                    assert set_index == False
                    if value.sourceNode == None:
                        value.sourceNode = index_name
                        set_index = True
                    else:
                        assert value.sourceNode == index_name
                        set_index = True
            
            if set_index == False and value.value != '':
                raise Exception(f"Value ({value.codeName}) wasn't in either left or right")
            
    if isinstance(value, BinaryExpressionOperator):
        setSourceNodeColumnValuesNPairs(value.left, sourcePairs)
        setSourceNodeColumnValuesNPairs(value.right, sourcePairs)
    elif isinstance(value, UnaryExpressionOperator):
        setSourceNodeColumnValuesNPairs(value.child, sourcePairs)
    else:
        # A value node
        assert isinstance(value, LeafNode)
        pass
    
def resetColumnValues(value):
    if isinstance(value, BinaryExpressionOperator):
        resetColumnValues(value.left)
        resetColumnValues(value.right)
    elif isinstance(value, UnaryExpressionOperator):
        resetColumnValues(value.child)
    else:
        # A value node
        assert isinstance(value, LeafNode)
        pass
    
    match value:
        case ColumnValue():
            if value.sourceNode != None:
                value.sourceNode = None
        case IntervalNotionOperator():
            if value.value.sourceNode != None:
                value.value.sourceNode = None
