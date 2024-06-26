from expression_operators import *

def gatherColumns(in_col):
    current_types = []
    if isinstance(in_col, BinaryExpressionOperator):
        leftTypes = gatherColumns(in_col.left)
        rightTypes = gatherColumns(in_col.right)

        current_types.extend(leftTypes)
        current_types.extend(rightTypes)
    elif isinstance(in_col, UnaryExpressionOperator):
        childTypes = gatherColumns(in_col.child)
        
        current_types.extend(childTypes)
    else:
        pass
    
    current_types.append(in_col)
    return current_types
        
def getColumnsFromGatherColumns(filter_cond):
    filterColumns = list(filter(lambda x: type(x) == ColumnValue, gatherColumns(filter_cond)))
    return filterColumns

def setSourceNodeByIDs(value, l_lambda_idx_key, l_keys_IDs,
                               l_lambda_idx_val, l_values_IDs, 
                               r_lambda_idx_key, r_keys_IDs,
                               r_lambda_idx_val, r_values_IDs):
    if id(value) in l_keys_IDs:
        value.sourceNode = l_lambda_idx_key
    elif id(value) in l_values_IDs:
        value.sourceNode = l_lambda_idx_val
    elif id(value) in r_keys_IDs:
        value.sourceNode = r_lambda_idx_key
    elif id(value) in r_values_IDs:
        value.sourceNode = r_lambda_idx_val
    else:
        raise Exception(f"We could not find the value ({value}) in any of the lists of IDs")

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
            if hasattr(x, "value"):
                columns_str.append(x.value)
            else:
                columns_str.append(x.codeName)
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
        case SubstringOperator():
            set_index = False
            for index_name, columns in sourcePairs:
                if value.value.codeName in columns:
                    assert set_index == False
                    value.value.sourceNode = index_name
                    set_index = True
            
            if set_index == False:
                raise Exception(f"Value ({value.child.codeName}) wasn't in either left or right")
        case InSetOperator():
            set_index = False
            for index_name, columns in sourcePairs:
                if isinstance(value.child, SubstringOperator):
                    if value.child.value.codeName in columns:
                        assert set_index == False
                        value.child.sourceNode = index_name
                        set_index = True
                else:
                    if value.child.codeName in columns:
                        assert set_index == False
                        value.child.sourceNode = index_name
                        set_index = True
            
            if set_index == False:
                raise Exception(f"Value ({value.child.codeName}) wasn't in either left or right")
        case IntervalNotionOperator() | LikeOperator():
            set_index = False
            for index_name, columns in sourcePairs:
                if value.value.codeName in columns:
                    assert set_index == False
                    value.value.sourceNode = index_name
                    set_index = True
            
            if set_index == False:
                raise Exception(f"Value ({value.codeName}) wasn't in either left or right")
        case MulOperator() | AvgAggrOperator() | MinAggrOperator() | MaxAggrOperator() | CountAllOperator():
            if value.created == True:
                set_index = False
                for index_name, columns in sourcePairs:
                    if value.codeName in columns:
                        assert set_index == False
                        value.sourceNode = index_name
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
                        break
                    else:
                        assert value.sourceNode == index_name
                        set_index = True
                elif value.codeName in columns:
                    assert set_index == False
                    if value.sourceNode == None:
                        value.sourceNode = index_name
                        set_index = True
                        break
                    else:
                        assert value.sourceNode == index_name
                        set_index = True
            
            if set_index == False and value.value != '':
                if hasattr(value, "isOuterLookup") and value.isOuterLookup == True:
                    pass
                else:
                    raise Exception(f"Value ({value.codeName}) wasn't in either left or right")
    
    if value.created == True and isinstance(value, (MulOperator, AvgAggrOperator, MinAggrOperator, MaxAggrOperator, CountAllOperator)):
        pass
    elif isinstance(value, BinaryExpressionOperator):
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
    
    if isinstance(value, (IntervalNotionOperator, LikeOperator)):
        if value.value.sourceNode != None:
            value.value.sourceNode = None
    elif isinstance(value, InSetOperator):
        if value.child.sourceNode != None:
            value.child.sourceNode = None
    elif isinstance(value, SubstringOperator):
        value.sourceNode = None
        value.value.sourceNode = None
    elif isinstance(value, LookupOperator):
        for val in value.values:
            val.sourceNode = None
    else:
        if hasattr(value, "sourceNode") and value.sourceNode != None:
            value.sourceNode = None
