from collections import defaultdict

from universal_plan_nodes import *
from expression_operators import *

def convert_aggregation_tree_to_pandas(aggr_tree: ExpressionBaseNode, dataFrameName: str) -> str:
    # Visit Children
    if isinstance(aggr_tree, AggregationOperators):
        childNode = convert_aggregation_tree_to_pandas(aggr_tree.child, dataFrameName)
    else:
        # Should have a completed Code Name
        assert aggr_tree.codeName != ""
        return f"{dataFrameName}.{aggr_tree.codeName}"
    
    expression_output = ""
    match aggr_tree:
        case SumAggrOperator():
            expression_output = f"{childNode}.sum()"
        case _:
            raise Exception(f"Unknown Aggregation Operator: {aggr_tree}")
        
    return expression_output

def convert_expression_operator_to_column_name(expr_tree: ExpressionBaseNode):
    # Visit Children
    expression_output = []
    if isinstance(expr_tree, BinaryExpressionOperator):
        expression_output.extend(convert_expression_operator_to_column_name(expr_tree.left))
        expression_output.extend(convert_expression_operator_to_column_name(expr_tree.right))
    elif isinstance(expr_tree, UnaryExpressionOperator):
        expression_output.extend(convert_expression_operator_to_column_name(expr_tree.child))
    else:
        # A value node
        assert isinstance(expr_tree, ValueNode)
        pass
    
    match expr_tree:
        case ColumnValue():
            expression_output.append(f"{expr_tree.value}")
        case SumAggrOperator():
            expression_output.append("sum")
        case MinAggrOperator():
            expression_output.append("min")
        case AvgAggrOperator():
            expression_output.append("avg")
        case CountAggrOperator():
            expression_output.append("count")
        case AggregationOperators():
            raise Exception(f"We have an aggregation operator, but don't have a case for it: {expr_tree}")
    
    return expression_output

def convert_expression_operator_to_pandas(expr_tree: ExpressionBaseNode, dataFrameName: str) -> str:
    def handleConstantValue(expr: ExpressionBaseNode):
        if expr.type == "Datetime":
            return f"'{expr.value.strftime('%Y-%m-%d')}'"
        else:
            return expr.value
        
    def handleIntervalNotion(expr: ExpressionBaseNode, leftNode, rightNode, dataFrameName):
        pandasValue = convert_expression_operator_to_pandas(expr.value, dataFrameName)
        inclusiveOption = None
        match expr.mode:
            case "[]":
                inclusiveOption = "both"
            case "()":
                inclusiveOption = "neither"
            case "[)":
                inclusiveOption = "left"
            case "(]":
                inclusiveOption = "right"
            case _:
                raise Exception(f"Unknown Internal Notion operator: {expr.mode}")
        
        assert inclusiveOption in ["both", "neither", "left", "right"]
        convertedExpression = f"{pandasValue}.between({leftNode}, {rightNode}, inclusive='{inclusiveOption}')"
        return convertedExpression
    
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
            expression_output = f"({leftNode}) & ({rightNode})"
        case MulOperator():
            expression_output = f"{leftNode} * {rightNode}"
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
            if op_tree.keyExpressions == []:
                new_op_tree = PandasAggrNode(
                    op_tree.preAggregateExpressions,
                    op_tree.postAggregateOperations
                )
            else:
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
        # The columns, as strs
        self.columns = set()
        
    def addToTableColumns(self, incomingColumns):
        localColumns = set()
        if isinstance(incomingColumns, list):
            localColumns.update(incomingColumns)
        elif isinstance(incomingColumns, str):
            localColumns.add(incomingColumns)
        else:
            raise Exception(f"Unexpected format of incoming columns, {type(incomingColumns)}")
    
        self.columns.update(localColumns)
        
    def __updateTableColumns(self):
        # get from children
        if hasattr(self, "left"):
            self.columns.update(self.left.getTableColumns())
        if hasattr(self, "right"):
            self.columns.update(self.right.getTableColumns())
        if hasattr(self, "child"):
            self.columns.update(self.child.getTableColumns())
        
    def getTableColumns(self) -> set():
        self.__updateTableColumns()
        return self.columns
    
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
        
    def getTableColumnsForDF(self) -> list[str]:
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
        
class PandasAggrNode(UnaryPandasNode):
    def __init__(self, preAggregateExpressions, postAggregateOperations):
        super().__init__()
        self.preAggregateExpressions = preAggregateExpressions
        self.postAggregateOperations = postAggregateOperations

class PandasGroupNode(UnaryPandasNode):
    def __init__(self, keyExpressions, preAggregateExpressions, postAggregateOperations):
        super().__init__()
        self.keyExpressions = keyExpressions
        self.preAggregateExpressions = preAggregateExpressions
        self.postAggregateOperations = postAggregateOperations

import random

# Unparser

class UnparsePandasTree():
    def __init__(self, pandas_tree: PandasBaseNode) -> None:
        self.pandas_content = []
        self.nodesCounter = defaultdict(int)
        self.pandas_tree = pandas_tree
        
        self.__walk_tree(self.pandas_tree)
        
    def getChildTableNames(self, current: PandasBaseNode) -> list[str]:
        childTables = []
        if isinstance(current, BinaryPandasNode):
            childTables.append(current.left.tableName)
            childTables.append(current.right.tableName)
        elif isinstance(current, UnaryPandasNode):
            childTables.append(current.child.tableName)
        else:
            # A leaf node
            pass
        
        return childTables
    
    """
    We need to track new columns, surface previous
    Create new ones from just ColumnValues, make sure isn't in set
        If is, add randomness and try again
    Add to set, return new name in method
    Store this in the preAggrExpr
    """
    def getNewColumnName(self, expr: ExpressionBaseNode, current: PandasBaseNode):
        currentNodeColumns = current.getTableColumns()
        processedName = "".join(convert_expression_operator_to_column_name(expr))
        while processedName in currentNodeColumns:
            processedName = f"{processedName}{random.randint(0,9)}"
        current.addToTableColumns(processedName)
        expr.setCodeName(processedName)
        return processedName
        
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
            self.writeContent(
                f"{createdDataFrameName} = {previousTableName}[{tableRestriction}]"
            )
            # Update previousTableName
            previousTableName = createdDataFrameName
            
        # Limit by table columns
        self.writeContent(
            f"{createdDataFrameName} = {previousTableName}[{node.getTableColumnsForDF()}]"
        )
        
        # Set the tableName
        node.tableName = createdDataFrameName
        # Set the tableColumns
        node.addToTableColumns(node.getTableColumnsForDF())
        
    def visitPandasAggrNode(self, node):        
        self.nodesCounter[PandasGroupNode] += 1
        nodeNumber = self.nodesCounter[PandasGroupNode]
        
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 1
        childTable = childTableList[0]
        
        # Do preAggregateExpressions first
        if node.preAggregateExpressions != []:
            # Create the new column(s) in the childTable
            for preAggrExpr in node.preAggregateExpressions:
                newColumnExpression = convert_expression_operator_to_pandas(preAggrExpr, childTable)
                newColumnName = self.getNewColumnName(preAggrExpr, node.child)
                self.writeContent(
                    f"{childTable}['{newColumnName}'] = {newColumnExpression}"
                )

        # Create the new dataFrame, do the postAggregateOperations here
        createdDataFrameName = f"df_aggr_{nodeNumber}"
        self.writeContent(
            f"{createdDataFrameName} = pd.DataFrame()"
        )
        
        # Post Aggregate Expressions
        if node.postAggregateOperations != []:
            for postAggrOp in node.postAggregateOperations:
                newColumnExpression = convert_aggregation_tree_to_pandas(postAggrOp, childTable)
                newColumnName = self.getNewColumnName(postAggrOp, node)
                self.writeContent(
                    f"{createdDataFrameName}['{newColumnName}'] = [{newColumnExpression}]"
                )
        
        # Set the tableName
        node.tableName = createdDataFrameName
        
    def visitPandasOutputNode(self, node):
        self.nodesCounter[PandasOutputNode] += 1
        nodeNumber = self.nodesCounter[PandasOutputNode]
        
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 1
        childTable = childTableList[0]
        
        # Create the new dataFrame
        createdDataFrameName = f"df_output_{nodeNumber}"
        self.writeContent(
            f"{createdDataFrameName} = {childTable}"
        )
        
        # Rename the columns
        assert len(node.outputNames) == len(node.outputColumns)
        currentNewNameDict = {node.outputColumns[i].codeName: node.outputNames[i] for i in range(len(node.outputNames))}
        
        self.writeContent(
            f"{createdDataFrameName} = {createdDataFrameName}.rename(columns={currentNewNameDict})"
        )
            
        # Limit by node.outputNames
        # Limit by table columns
        self.writeContent(
            f"{createdDataFrameName} = {createdDataFrameName}[{node.outputNames}]"
        )
