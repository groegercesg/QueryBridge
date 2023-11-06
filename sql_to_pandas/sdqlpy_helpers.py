from expression_operators import *

def convert_expr_to_sdqlpy(value, lambda_idx, node_columns):
    setSourceNodeColumnValues(value, lambda_idx, node_columns)
    expr_content = convert_expression_operator_to_sdqlpy(value)
    resetColumnValues(value)
    
    return expr_content

def setSourceNodeColumnValues(value, l_lambda_idx, l_columns, r_lambda_idx=None, r_columns=None):
    def check_and_fix_columns(columns):
        if isinstance(columns, set) and isinstance(next(iter(columns)), ExpressionBaseNode):
            return getCodeNameFromSetColumnValues(columns)
        elif isinstance(columns, list) and isinstance(columns[0], str):
            return set(columns)
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

def convert_expression_operator_to_sdqlpy(expr_tree: ExpressionBaseNode) -> str:
    def handle_ConstantValue(expr: ConstantValue):
        if expr.type == "String":
            return f"'{expr.value}'"
        elif expr.type == "Float":
            return expr.value
        elif expr.type == "Datetime":
            year = str(expr.value.year).zfill(4)
            month = str(expr.value.month).zfill(2)
            day = str(expr.value.day).zfill(2)
            return f"{year}{month}{day}"
        elif expr.type == "Integer":
            return f'{expr.value}.0'
        else:
            raise Exception(f"Unknown Constant Value Type: {expr.type}")
        
    def handle_IntervalNotion(expr: IntervalNotionOperator):
        match expr.mode:
            case "[]":
                leftExpr = GreaterThanEqOperator()
                rightExpr = LessThanEqOperator()
            case "()":
                leftExpr = GreaterThanOperator()
                rightExpr = LessThanOperator()
            case "[)":
                leftExpr = GreaterThanEqOperator()
                rightExpr = LessThanOperator()
            case "(]":
                leftExpr = GreaterThanOperator()
                rightExpr = LessThanEqOperator()
            case _:
                raise Exception(f"Unknown Internal Notion operator: {expr.mode}")
        
        leftExpr.left = expr.value
        leftExpr.right = expr.left
                
        rightExpr.left = expr.value
        rightExpr.right = expr.right
        
        convertedExpression = AndOperator()
        convertedExpression.left = leftExpr
        convertedExpression.right = rightExpr
        
        sdqlpyExpression = convert_expression_operator_to_sdqlpy(convertedExpression)
        return sdqlpyExpression
    
    def handle_SDQLpyThirdNodeWrapper(expr_tree):
        outputString = f"{expr_tree.third_node.tableName}[{expr_tree.sourceNode}.{expr_tree.target_key.codeName}].{expr_tree.col.codeName}"
        return outputString
    
    def handle_InSetOperator(expr: InSetOperator):
        # rewrite as child == set[0] or child == set[1]
        equating = []
        for set_opt in expr.set:
            eq_op = EqualsOperator()
            eq_op.addLeft(expr.child)
            eq_op.addRight(set_opt)
            equating.append(
                eq_op
            )
        
        or_tree = join_statements_with_operator(equating, "OrOperator")
        
        return f"({convert_expression_operator_to_sdqlpy(or_tree)})"
    
    # Visit Children
    if isinstance(expr_tree, BinaryExpressionOperator):
        leftNode = convert_expression_operator_to_sdqlpy(expr_tree.left)
        rightNode = convert_expression_operator_to_sdqlpy(expr_tree.right)
    elif isinstance(expr_tree, UnaryExpressionOperator):
        childNode = convert_expression_operator_to_sdqlpy(expr_tree.child)
    else:
        # A value node
        assert isinstance(expr_tree, LeafNode)
        pass
    
    expression_output = None
    match expr_tree:
        case ColumnValue():
            assert expr_tree.sourceNode != None
            expression_output = f"{expr_tree.sourceNode}.{expr_tree.value}"
        case ConstantValue():
            expression_output = handle_ConstantValue(expr_tree)
        case LessThanOperator():
            expression_output = f"({leftNode} < {rightNode})"
        case LessThanEqOperator():
            expression_output = f"({leftNode} <= {rightNode})"
        case GreaterThanOperator():
            expression_output = f"({leftNode} > {rightNode})"
        case GreaterThanEqOperator():
            expression_output = f"({leftNode} >= {rightNode})"
        case IntervalNotionOperator():
            expression_output = handle_IntervalNotion(expr_tree)
        case AndOperator():
            expression_output = f"{leftNode} and {rightNode}"
        case OrOperator():
            expression_output = f"{leftNode} or {rightNode}"
        case MulOperator():
            expression_output = f"{leftNode} * {rightNode}"
        case SubOperator():
            expression_output = f"({leftNode} - {rightNode})"
        case AddOperator():
            expression_output = f"({leftNode} + {rightNode})"
        case CountAllOperator():
            expression_output = "1"
        case EqualsOperator():
            expression_output = f"({leftNode} == {rightNode})"
        case SDQLpyThirdNodeWrapper():
            expression_output = handle_SDQLpyThirdNodeWrapper(expr_tree)
        case InSetOperator():
            expression_output = handle_InSetOperator(expr_tree)
        
        case _: 
            raise Exception(f"Unrecognised expression operator: {type(expr_tree)}")

    return expression_output