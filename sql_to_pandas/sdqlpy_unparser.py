from collections import defaultdict

from universal_plan_nodes import *
from expression_operators import *

TAB = "    "

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
            expression_output = f"{lambdaName}[0].{expr_tree.value}"
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
        case _: 
            raise Exception(f"Unrecognised expression operator: {type(expr_tree)}")

    return expression_output

def convert_universal_to_sdqlpy(op_tree: UniversalBaseNode):
    # Visit Children
    leftNode, rightNode, childNode = None, None, None
    if isinstance(op_tree, BinaryBaseNode):
        leftNode = convert_universal_to_sdqlpy(op_tree.left)
        rightNode = convert_universal_to_sdqlpy(op_tree.right)
    elif isinstance(op_tree, UnaryBaseNode):
        childNode = convert_universal_to_sdqlpy(op_tree.child)
    else:
        # A leaf node
        pass
    
    # Create a 'new_op_tree' from an existing 'op_tree'
    match op_tree:
        case ScanNode():
            new_op_tree = SDQLpyScanNode(
                op_tree.tableName
            )
        case GroupNode():
            if op_tree.keyExpressions == []:
                new_op_tree = SDQLpyAggrNode(
                    op_tree.preAggregateExpressions,
                    op_tree.postAggregateOperations
                )
            else:
                raise Exception
        case OutputNode():
            new_op_tree = None
        case _:
            raise Exception(f"Unexpected op_tree, it was of class: {op_tree.__class__}")

    # Pipeline breaker
    if isinstance(new_op_tree, PipelineBreakerNode):
        match op_tree.child:
            case LeafBaseNode():
                match op_tree.child:
                    case ScanNode():
                        if op_tree.child.tableRestrictions != None:
                            new_op_tree.addFilterContent(op_tree.child.tableRestrictions)
                    case _:
                        raise Exception(f"We encountered an unknown child")
            case UnaryBaseNode():
                raise Exception("child is a Unary Node")
            case BinaryBaseNode():
                raise Exception("child is a Binary Node")
            case _:
                raise Exception()
    
    # Add in the children
    if new_op_tree == None:
        if (leftNode == None) and (rightNode == None) and (childNode == None):
            # pass, we're in a Leaf
            pass
        elif (leftNode == None) and (rightNode == None):
            # we're in a Unary node
            new_op_tree = childNode
        elif (childNode == None):
            raise Exception("Trying to replace for a Binary situation")
        else:
            raise Exception("Child and a Binary, impossible")
    else:
        match new_op_tree:
            case UnarySDQLpyNode():
                if childNode != None:
                    new_op_tree.addChild(childNode)
            case BinarySDQLpyNode():
                if (leftNode != None) and (rightNode != None):
                    new_op_tree.addLeft(leftNode)
                    new_op_tree.addRight(rightNode)
                else:
                    raise Exception("Binary with some Nones")
            case _:
                # LeafSDQLpyNode
                assert isinstance(new_op_tree, LeafSDQLpyNode)
    
    # Add nodeID to new_op_node
    assert hasattr(op_tree, "nodeID")
    if new_op_tree != None:
        if new_op_tree.nodeID == None:
            new_op_tree.addID(op_tree.nodeID)
        else:
            # We've already assigned one, don't overwrite it
            pass
    
    return new_op_tree

# Classes for the SDQLpy Tree
class SDQLpyBaseNode():
    def __init__(self):
        # The columns, as strs
        self.columns = set()
        self.nodeID = None
        
    def addID(self, value):
        assert self.nodeID == None
        self.nodeID = value
        
    def addToTableColumns(self, incomingColumns):
        localColumns = set()
        if isinstance(incomingColumns, list):
            assert all(isinstance(x, ExpressionBaseNode) for x in incomingColumns)
            localColumns.update(incomingColumns)
        elif isinstance(incomingColumns, ExpressionBaseNode):
            localColumns.add(incomingColumns)
        else:
            raise Exception(f"Unexpected format of incoming columns, {type(incomingColumns)}")
    
        self.columns.update(localColumns)
        
    def __updateTableColumns(self):
        if len(self.columns) > 0:
            pass
        else:
            # get from children
            if hasattr(self, "left"):
                #if self.left.columns:
                #    self.columns.update(self.left.columns)
                #else:
                self.columns.update(self.left.getTableColumnsInternal())
            if hasattr(self, "right"):
                self.columns.update(self.right.getTableColumnsInternal())
            if hasattr(self, "child"):
                self.columns.update(self.child.getTableColumnsInternal())
        
    def getTableColumnsString(self) -> set():
        self.__updateTableColumns()
        outputNames = set()
        for col in self.columns:
            if col.codeName != '':
                outputNames.add(col.codeName)
            else:
                outputNames.add(col.value)
        return outputNames

    def getTableColumnsInternal(self) -> set():
        self.__updateTableColumns()
        return self.columns
    
class LeafSDQLpyNode(SDQLpyBaseNode):
    def __init__(self):
        super().__init__()
        
class PipelineBreakerNode(SDQLpyBaseNode):
    def __init__(self):
        super().__init__()
        self.filterContent = None
    
    def addFilterContent(self, filterContent):
        assert self.filterContent == None
        self.filterContent = filterContent
        
class UnarySDQLpyNode(PipelineBreakerNode):
    def __init__(self):
        super().__init__()
        self.child = None
        
    def addChild(self, child: SDQLpyBaseNode):
        assert self.child == None
        self.child = child
        
class BinarySDQLpyNode(PipelineBreakerNode):
    def __init__(self):
        super().__init__()
        self.left = None
        self.right = None
        
    def addLeft(self, left: SDQLpyBaseNode):
        assert self.left == None
        self.left = left
        
    def addRight(self, right: SDQLpyBaseNode):
        assert self.right == None
        self.right = right

# Classes for Nodes
class SDQLpyScanNode(LeafSDQLpyNode):
    def __init__(self, tableName):
        super().__init__()
        self.tableName = tableName

class SDQLpyAggrNode(UnarySDQLpyNode):
    def __init__(self, preAggregateExpressions, postAggregateOperations):
        super().__init__()
        self.preAggregateExpressions = preAggregateExpressions
        self.postAggregateOperations = postAggregateOperations

# Unparser
class UnparseSDQLpyTree():
    def __init__(self, sdqlpy_tree: SDQLpyBaseNode) -> None:
        self.sdqlpy_content = []
        self.nodesCounter = defaultdict(int)
        self.sdqlpy_tree = sdqlpy_tree
        
        self.relations = set()
        self.parserCreatedColumns = set()
        self.nodeDict = {}
        self.gatherNodeDict(self.sdqlpy_tree)
        
        self.__walk_tree(self.sdqlpy_tree)
        
    def getChildTableNames(self, current: SDQLpyBaseNode) -> list[str]:
        childTables = []
        if isinstance(current, BinarySDQLpyNode):
            childTables.append(current.left.tableName)
            childTables.append(current.right.tableName)
        elif isinstance(current, UnarySDQLpyNode):
            childTables.append(current.child.tableName)
        else:
            # A leaf node
            pass
        
        return childTables
    
    def writeContent(self, content: str) -> None:
        self.sdqlpy_content.append(content)
        
    def getSDQLpyContent(self) -> list[str]:
        return self.sdqlpy_content
    
    def gatherNodeDict(self, current_node):
        if current_node == None:
            pass
        elif isinstance(current_node, BinarySDQLpyNode):
            self.gatherNodeDict(current_node.left)
            self.gatherNodeDict(current_node.right)
        elif isinstance(current_node, UnarySDQLpyNode):
            self.gatherNodeDict(current_node.child)
        else:
            # A leaf node
            pass
        
        if (current_node != None) and (current_node.nodeID != None):
            assert current_node.nodeID not in self.nodeDict
            self.nodeDict[current_node.nodeID] = current_node
    
    
    def __walk_tree(self, current_node):
        # Walk to children Children
        if current_node == None:
            pass
        elif isinstance(current_node, BinarySDQLpyNode):
            self.__walk_tree(current_node.left)
            self.__walk_tree(current_node.right)
        elif isinstance(current_node, UnarySDQLpyNode):
            self.__walk_tree(current_node.child)
        else:
            # A leaf node
            pass
        
        if current_node != None:
            # Visit the current_node and add it to self.pandas_content
            targetVisitorMethod = f"visit{current_node.__class__.__name__}"
            if hasattr(self, targetVisitorMethod):
                getattr(self, targetVisitorMethod)(current_node)
            else:
                raise Exception(f"No visit method found for class name: {current_node.__class__.__name__}, was expected to find a: '{targetVisitorMethod}' method.")
            
    def visitSDQLpyScanNode(self, node):
        # We don't do anything for a scan node
        self.relations.add(node.tableName)
        pass
            
    def visitSDQLpyAggrNode(self, node):
        self.nodesCounter[SDQLpyAggrNode] += 1
        nodeNumber = self.nodesCounter[SDQLpyAggrNode]
        
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 1
        childTable = childTableList[0]
        
        createdDictName = f"aggr_{nodeNumber}"
        lambda_index = "p"
        
        assert len(node.postAggregateOperations) == 1 and isinstance(node.postAggregateOperations[0], SumAggrOperator)
        
        assert len(node.preAggregateExpressions) == 1
        aggrContent = convert_expression_operator_to_sdqlpy(node.preAggregateExpressions[0], lambda_index)
        
        self.writeContent(
            f"{createdDictName} = {childTable}.sum(\n"
            f"{TAB}lambda {lambda_index} : {aggrContent}\n"
        )
        
        if node.filterContent != None:
            filterContent = convert_expression_operator_to_sdqlpy(node.filterContent, lambda_index)
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}0.0"
            )
        
        self.writeContent(
            f")"
        )
        
        # Set the tableName
        node.tableName = createdDictName
        # Set node.columns
        node.columns = set(node.postAggregateOperations)
