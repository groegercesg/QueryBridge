from expression_operators import *

def convert_expression_operator_to_sdqlpy(expr_tree: ExpressionBaseNode, lambdaName: str) -> str:
    def handleConstantValue(expr: ConstantValue):
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
        
    def handleIntervalNotion(expr: IntervalNotionOperator, lambdaName):
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
        
        sdqlpyExpression = convert_expression_operator_to_sdqlpy(convertedExpression, lambdaName)
        return sdqlpyExpression
    
    # Visit Children
    if isinstance(expr_tree, BinaryExpressionOperator):
        leftNode = convert_expression_operator_to_sdqlpy(expr_tree.left, lambdaName)
        rightNode = convert_expression_operator_to_sdqlpy(expr_tree.right, lambdaName)
    elif isinstance(expr_tree, UnaryExpressionOperator):
        childNode = convert_expression_operator_to_sdqlpy(expr_tree.child, lambdaName)
    else:
        # A value node
        assert isinstance(expr_tree, LeafNode)
        pass
    
    expression_output = None
    match expr_tree:
        case ColumnValue():
            expression_output = f"{lambdaName}.{expr_tree.value}"
        case ConstantValue():
            expression_output = handleConstantValue(expr_tree)
        case LessThanOperator():
            expression_output = f"({leftNode} < {rightNode})"
        case LessThanEqOperator():
            expression_output = f"({leftNode} <= {rightNode})"
        case GreaterThanOperator():
            expression_output = f"({leftNode} > {rightNode})"
        case GreaterThanEqOperator():
            expression_output = f"({leftNode} >= {rightNode})"
        case IntervalNotionOperator():
            expression_output = handleIntervalNotion(expr_tree, lambdaName)
        case AndOperator():
            expression_output = f"{leftNode} and {rightNode}"
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
        case _: 
            raise Exception(f"Unrecognised expression operator: {type(expr_tree)}")

    return expression_output