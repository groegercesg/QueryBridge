from expression_operators import *

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
    columns_str = set([x.codeName for x in columns])
        
    # Assert none are empty string
    assert not any([True for x in columns_str if x == ""])
    
    return columns_str
    
def setSourceNodeColumnValuesNPairs(value, sourcePairs):
    match value:
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
        case IntervalNotionOperator():
            set_index = False
            for index_name, columns in sourcePairs:
                if value.value.codeName in columns:
                    assert set_index == False
                    value.value.sourceNode = index_name
                    set_index = True
            
            if set_index == False:
                raise Exception(f"Value ({value.codeName}) wasn't in either left or right")
        case ColumnValue() | SumAggrOperator():
            set_index = False
            for index_name, columns in sourcePairs:
                if value.codeName in columns:
                    assert set_index == False
                    if value.sourceNode == None:
                        value.sourceNode = index_name
                        set_index = True
                    else:
                        assert value.sourceNode == index_name
            
            # if set_index == False:
            #     raise Exception(f"Value ({value.codeName}) wasn't in either left or right")
            
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
        case ColumnValue() | SumAggrOperator():
            if value.sourceNode != None:
                value.sourceNode = None
        case IntervalNotionOperator():
            if value.value.sourceNode != None:
                value.value.sourceNode = None
