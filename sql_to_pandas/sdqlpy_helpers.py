from expression_operators import *

def setSourceNodeColumnValues(value, l_lambda_idx, l_columns, r_lambda_idx=None, r_columns=None):
    def check_and_fix_columns(columns):
        if isinstance(columns, set) and isinstance(next(iter(columns)), ExpressionBaseNode):
            return getCodeNameFromSetColumnValues(columns)
        elif isinstance(columns, list) and isinstance(columns[0], str):
            return set(columns)
        elif isinstance(columns, list) and isinstance(columns[0], ExpressionBaseNode):
            return getCodeNameFromSetColumnValues(set(columns))
        elif isinstance(columns, str):
            return set([columns])
        else:
            raise Exception(f"Unexpected type for columns: {type(columns)}")
            
    if (r_lambda_idx == None) and (r_columns == None):
        l_columns_str = check_and_fix_columns(l_columns)
        setSourceNodeColumnValuesOneLambda(value, l_lambda_idx, l_columns_str)
    elif (l_lambda_idx != None) and (l_columns != None) and (r_lambda_idx != None) and (r_columns != None):
        l_columns_str = check_and_fix_columns(l_columns)
        r_columns_str = check_and_fix_columns(r_columns)
        setSourceNodeColumnValuesTwoLambda(value, l_lambda_idx, r_lambda_idx, l_columns_str, r_columns_str)
    else:
        raise Exception("Unsuitable parameters set in setSourceNodeColumnValues")

def getCodeNameFromSetColumnValues(columns):
    columns_str = set([x.codeName for x in columns])
        
    # Assert none are empty string
    assert not any([True for x in columns_str if x == ""])
    
    return columns_str

def setSourceNodeColumnValuesOneLambda(value, lambda_idx, columns):
    assert isinstance(columns, set) and isinstance(next(iter(columns)), str)
    
    if isinstance(value, BinaryExpressionOperator):
        setSourceNodeColumnValuesOneLambda(value.left, lambda_idx, columns)
        setSourceNodeColumnValuesOneLambda(value.right, lambda_idx, columns)
    elif isinstance(value, UnaryExpressionOperator):
        setSourceNodeColumnValuesOneLambda(value.child, lambda_idx, columns)
    else:
        # A value node
        assert isinstance(value, LeafNode)
        pass
    
    match value:
        case ColumnValue():
            decidedSourceValue = None
            if value.codeName in columns:
                decidedSourceValue = lambda_idx
            else:
                raise Exception(f"Value ({value.codeName}) wasn't in either left or right")
            
            assert decidedSourceValue != None
            if value.sourceNode == None:
                value.sourceNode = decidedSourceValue
            else:
                assert value.sourceNode == decidedSourceValue
        case IntervalNotionOperator():
            setSourceNodeColumnValuesOneLambda(value.value, lambda_idx, columns)

def setSourceNodeColumnValuesTwoLambda(value, l_lambda_idx, r_lambda_idx, l_columns, r_columns):
    if isinstance(value, BinaryExpressionOperator):
        setSourceNodeColumnValuesTwoLambda(value.left, l_lambda_idx, r_lambda_idx, l_columns, r_columns)
        setSourceNodeColumnValuesTwoLambda(value.right, l_lambda_idx, r_lambda_idx, l_columns, r_columns)
    elif isinstance(value, UnaryExpressionOperator):
        setSourceNodeColumnValuesTwoLambda(value.child, l_lambda_idx, r_lambda_idx, l_columns, r_columns)
    else:
        # A value node
        assert isinstance(value, LeafNode)
        pass
    
    match value:
        case SDQLpyThirdNodeWrapper():
            if value.target_key.codeName in l_columns:
                value.set_sourceValue(l_lambda_idx)
            elif value.target_key.codeName in r_columns:
                value.set_sourceValue(r_lambda_idx)
            else:
                raise Exception("The ThirdNode Target Key must be in either Left or Right")
        case InSetOperator():
            if value.child.codeName in l_columns:
                value.child.sourceNode = l_lambda_idx
            elif value.child.codeName in r_columns:
                value.child.sourceNode = r_lambda_idx
            else:
                raise Exception(f"Value ({value.codeName}) wasn't in either left or right")
        case IntervalNotionOperator():
            if value.value.codeName in l_columns:
                value.value.sourceNode = l_lambda_idx
            elif value.value.codeName in r_columns:
                value.value.sourceNode = r_lambda_idx
            else:
                raise Exception(f"Value ({value.codeName}) wasn't in either left or right")
        case ColumnValue():
            decidedSourceValue = None
            if value.codeName in l_columns:
                decidedSourceValue = l_lambda_idx
            elif value.codeName in r_columns:
                decidedSourceValue = r_lambda_idx
            else:
                raise Exception(f"Value ({value.codeName}) wasn't in either left or right")
            
            assert decidedSourceValue != None
            if value.sourceNode == None:
                value.sourceNode = decidedSourceValue
            else:
                assert value.sourceNode == decidedSourceValue
    
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
