from collections import defaultdict

from universal_plan_nodes import *
from expression_operators import *

def convert_expression_operator_to_pandas(expr_tree: ExpressionBaseNode, dataFrameName: str) -> str:
    def handleConstantValue(expr: ExpressionBaseNode):
        if expr.type == "Datetime":
            return expr.value.strftime("%Y-%m-%d")
        else:
            return expr.value
        
    def handleIntervalNotion(expr: ExpressionBaseNode, leftNode, rightNode, dataFrameName):
        convertedExpression = None
        match expr.mode:
            case "[]":
                # TODO: (li.l_discount>=0.050) & (li.l_discount<=0.070)
                pass
            case _:
                raise Exception(f"Unknown Internal Notion operator: {expr.mode}")

        return convert_expression_operator_to_pandas(convertedExpression, dataFrameName)
    
    # Visit Children
    if isinstance(expr_tree, BinaryExpressionOperator):
        leftNode = convert_expression_operator_to_pandas(expr_tree.left, dataFrameName)
        rightNode = convert_expression_operator_to_pandas(expr_tree.right, dataFrameName)
    elif isinstance(expr_tree, UnaryExpressionOperator):
        childNode = convert_expression_operator_to_pandas(expr_tree.child, dataFrameName)
    else:
        # A value node
        assert isinstance(expr_tree, ValueNode)
        pass
    
    expression_output = None
    match expr_tree:
        case ColumnValue():
            expression_output = f"{dataFrameName}.{expr_tree.value}"
        case ConstantValue():
            expression_output = handleConstantValue(expr_tree)
        case AndOperator():
            pass
        case LessThanOperator():
            expression_output = f"{leftNode} < {rightNode}"
        case IntervalNotionOperator():
            expression_output = handleIntervalNotion(expr_tree, leftNode, rightNode, dataFrameName)
        case _: 
            raise Exception(f"Unrecognised expression operator: {expr_tree}")
        
    
    return expression_output
    

def convert_universal_to_pandas(op_tree: UniversalBaseNode):
    # Visit Children
    if isinstance(op_tree, BinaryBaseNode):
        leftNode = convert_universal_to_pandas(op_tree.left)
        rightNode = convert_universal_to_pandas(op_tree.right)
    elif isinstance(op_tree, UnaryBaseNode):
        childNode = convert_universal_to_pandas(op_tree.child)
    else:
        # A leaf node
        pass
    
    # Create a 'new_op_tree' from an existing 'op_tree'
    match op_tree:
        case ScanNode():
            new_op_tree = PandasScanNode(
                op_tree.tableName,
                op_tree.tableColumns,
                op_tree.tableRestrictions
            )
        case GroupNode():
            new_op_tree = PandasGroupNode(
                op_tree.keyExpressions,
                op_tree.preAggregateExpressions,
                op_tree.postAggregateOperations
            )
        case OutputNode():
            new_op_tree = PandasOutputNode(
                op_tree.outputColumns,
                op_tree.outputNames
            )
        case _:
            raise Exception(f"Unexpected op_tree, it was of class: {op_tree.__class__}")

    # Overwrite the existing OpNode
    op_tree = new_op_tree
    
    # Add in the children
    match op_tree:
        case UnaryPandasNode():
            op_tree.addChild(childNode)
        case BinaryPandasNode():
            op_tree.addLeft(leftNode)
            op_tree.addRight(rightNode)
        case _:
            # PandasScanNode
            assert isinstance(op_tree, PandasScanNode)
    
    return op_tree

# Classes for the Pandas Tree
class PandasBaseNode():
    def __init__(self):
        pass
    
class UnaryPandasNode(PandasBaseNode):
    def __init__(self):
        super().__init__()
        self.child = None
        
    def addChild(self, child: PandasBaseNode):
        assert self.child == None
        self.child = child
        
class BinaryPandasNode(PandasBaseNode):
    def __init__(self):
        super().__init__()
        self.left = None
        self.right = None
        
    def addLeft(self, left: PandasBaseNode):
        assert self.left == None
        self.left = left
        
    def addRight(self, right: PandasBaseNode):
        assert self.right == None
        self.right = right
        
# Classes for Nodes
class PandasScanNode(PandasBaseNode):
    def __init__(self, tableName, tableColumns, tableRestriction):
        super().__init__()
        self.tableName = tableName
        self.tableColumns = tableColumns
        self.tableRestriction = tableRestriction
        
    def getTableColumns(self) -> list[str]:
        outputColumns = []
        for col in self.tableColumns:
            assert isinstance(col, ColumnValue)
            outputColumns.append(col.value)
        return outputColumns
    
class PandasOutputNode(UnaryPandasNode):
    def __init__(self, outputColumns, outputNames):
        super().__init__()
        self.outputColumns = outputColumns
        self.outputNames = outputNames

class PandasGroupNode(UnaryPandasNode):
    def __init__(self, keyExpressions, preAggregateExpressions, postAggregateOperations):
        super().__init__()
        self.keyExpressions = keyExpressions
        self.preAggregateExpressions = preAggregateExpressions
        self.postAggregateOperations = postAggregateOperations

# Unparser

class UnparsePandasTree():
    def __init__(self, pandas_tree: PandasBaseNode) -> None:
        self.pandas_content = []
        self.nodesCounter = defaultdict(int)
        self.pandas_tree = pandas_tree
        
        self.__walk_tree(self.pandas_tree)
        
    def writeContent(self, content: str) -> None:
        self.pandas_content.append(content)
    
    def getPandasContent(self) -> list[str]:
        return self.pandas_content
    
    def __walk_tree(self, current_node):
        # Walk to children Children
        if isinstance(current_node, BinaryPandasNode):
            self.__walk_tree(current_node.left)
            self.__walk_tree(current_node.right)
        elif isinstance(current_node, UnaryPandasNode):
            self.__walk_tree(current_node.child)
        else:
            # A leaf node
            pass
        
        # Visit the current_node and add it to self.pandas_content
        targetVisitorMethod = f"visit{current_node.__class__.__name__}"
        if hasattr(self, targetVisitorMethod):
            getattr(self, targetVisitorMethod)(current_node)
        else:
            raise Exception(f"No visit method found for class name: {current_node.__class__.__name__}, was expected to find a: '{targetVisitorMethod}' method.")
    
    def visitPandasScanNode(self, node):
        self.nodesCounter[PandasScanNode] += 1
        nodeNumber = self.nodesCounter[PandasScanNode]
        createdDataFrameName = f"df_scan_{nodeNumber}"
        previousTableName = node.tableName
        
        # Use restrictions
        if node.tableRestriction != None:
            # Convert tableRestriction
            tableRestriction = convert_expression_operator_to_pandas(node.tableRestriction, previousTableName)
            print("a")
            # Update previousTableName
            previousTableName = createdDataFrameName
            
        # Limit by table columns
        self.writeContent(
            f"{createdDataFrameName} = {previousTableName}[{node.getTableColumns()}]"
        )
        
        # Set the tableName
        node.tableName = createdDataFrameName
        
        
    def visitPandasOutputNode(self, node):
        self.nodesCounter[PandasOutputNode] += 1
        nodeNumber = self.nodesCounter[PandasOutputNode]
        print(f"At the unparser for Pandas Output Node: {node}")
        
    def visitPandasGroupNode(self, node):
        self.nodesCounter[PandasGroupNode] += 1
        nodeNumber = self.nodesCounter[PandasGroupNode]
        print(f"At the unparser for Pandas Group Node: {node}")
