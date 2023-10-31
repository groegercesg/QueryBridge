from collections import defaultdict

from universal_plan_nodes import *
from expression_operators import *

from sdqlpy_classes import *

TAB = "    "

def audit_sdqlpy_tree_scannode(op_tree: SDQLpyBaseNode) -> bool:
    def get_leaf_nodes(op_tree: SDQLpyBaseNode) -> list[SDQLpyBaseNode]:
        leafs = []
        def _get_leaf_nodes(op_node: SDQLpyBaseNode):
            match op_node:
                case BinarySDQLpyNode():
                    _get_leaf_nodes(op_node.left)
                    _get_leaf_nodes(op_node.right)
                case UnarySDQLpyNode():
                    _get_leaf_nodes(op_node.child)
                case SDQLpyBaseNode():
                    leafs.append(op_node)
                case _:
                    raise Exception(f"We are auditing a universal plan tree, all nodes should be at minimum a UniversalBaseNode, not: {op_node.__class__}") 
        _get_leaf_nodes(op_tree)
        return leafs
    
    # Get all leaves, make sure they're all SDQLpyRecordNode
    all_leaves = get_leaf_nodes(op_tree)
    return all(isinstance(leaf, SDQLpyRecordNode) for leaf in all_leaves)

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
        case SubOperator():
            expression_output = f"({leftNode} - {rightNode})"
        case AddOperator():
            expression_output = f"({leftNode} + {rightNode})"
        case CountAllOperator():
            expression_output = "1"
        case _: 
            raise Exception(f"Unrecognised expression operator: {type(expr_tree)}")

    return expression_output

def convert_universal_to_sdqlpy(universal_tree: UniversalBaseNode) -> SDQLpyBaseNode:
    def convert_trees(op_tree: UniversalBaseNode) -> SDQLpyBaseNode:
        # Visit Children
        leftNode, rightNode, childNode = None, None, None
        if isinstance(op_tree, BinaryBaseNode):
            leftNode = convert_trees(op_tree.left)
            rightNode = convert_trees(op_tree.right)
        elif isinstance(op_tree, UnaryBaseNode):
            childNode = convert_trees(op_tree.child)
        else:
            # A leaf node
            pass
        
        # Create a 'new_op_tree' from an existing 'op_tree'
        match op_tree:
            case ScanNode():
                new_op_tree = SDQLpyRecordNode(
                    op_tree.tableName
                )
            case GroupNode():
                if op_tree.keyExpressions == []:
                    new_op_tree = SDQLpyAggrNode(
                        op_tree.preAggregateExpressions,
                        op_tree.postAggregateOperations
                    )
                else:
                    new_op_tree = SDQLpyGroupNode(
                        op_tree.keyExpressions,
                        op_tree.preAggregateExpressions,
                        op_tree.postAggregateOperations
                    )
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
    
    # Use the output node to set relevant code names
    def set_codeNames(topNode):
        assert isinstance(topNode, OutputNode)
        assert len(topNode.outputNames) == len(topNode.outputColumns)
        
        for idx, name in enumerate(topNode.outputNames):
            if topNode.outputColumns[idx].codeName != "":
                assert (name == topNode.outputColumns[idx].codeName)
            else:
                topNode.outputColumns[idx].codeName = name
                
    def orderTopNode(sdqlpy_tree, output_cols_order):
        match sdqlpy_tree:
            case SDQLpyGroupNode():
                # Do ordering, sort postAggregateOperations by output_cols_order
                ordering = {k:v for v,k in enumerate(output_cols_order)}
                sdqlpy_tree.postAggregateOperations.sort(key = lambda x : ordering.get(x.codeName))
            case SDQLpyAggrNode():
                # No ordering required, as it only returns a single value
                pass
            case _:
                raise Exception(f"No ordering configured for node: {type(sdqlpy_tree)}")
    
    # Set the code names
    set_codeNames(universal_tree)
    output_cols_order = universal_tree.outputNames
    # Call convert trees
    sdqlpy_tree = convert_trees(universal_tree)
    # Order the topNode correctly
    orderTopNode(sdqlpy_tree, output_cols_order)
    
    return sdqlpy_tree

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
        
        # Set top node of the sdqlpy_tree to True
        sdqlpy_tree.topNode = True
        
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
        if isinstance(current_node, BinarySDQLpyNode):
            self.gatherNodeDict(current_node.left)
            self.gatherNodeDict(current_node.right)
        elif isinstance(current_node, UnarySDQLpyNode):
            self.gatherNodeDict(current_node.child)
        else:
            # A leaf node
            pass
        
        if current_node.nodeID != None:
            assert current_node.nodeID not in self.nodeDict
            self.nodeDict[current_node.nodeID] = current_node
    
    
    def __walk_tree(self, current_node):
        # Walk to children Children
        if isinstance(current_node, BinarySDQLpyNode):
            self.__walk_tree(current_node.left)
            self.__walk_tree(current_node.right)
        elif isinstance(current_node, UnarySDQLpyNode):
            self.__walk_tree(current_node.child)
        else:
            # A leaf node
            assert isinstance(current_node, LeafSDQLpyNode)
            pass
        
        # Visit the current_node and add it to self.pandas_content
        targetVisitorMethod = f"visit_{current_node.__class__.__name__}"
        if hasattr(self, targetVisitorMethod):
            # Count number of nodes
            self.nodesCounter[current_node.__class__.__name__] += 1
            getattr(self, targetVisitorMethod)(current_node)
        else:
            raise Exception(f"No visit method found for class name: {current_node.__class__.__name__}, was expected to find a: '{targetVisitorMethod}' method.")
        
    def visit_SDQLpyRecordNode(self, node):
        # We don't do anything for a record node
        node.getTableName(self)
        self.relations.add(node.tableName)
        
    def visit_SDQLpyGroupNode(self, node):
        # Get child name
        childTable = node.getChildName(self)
        
        initialDictName = node.getTableName(self, not_output = True)
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        self.writeContent(
            f"{initialDictName} = {childTable}.sum(lambda {lambda_index} :"
        )
        
        # Write keys
        keyContent = []
        for key in node.keyExpressions:
            expr = convert_expression_operator_to_sdqlpy(key, lambda_index)
            keyContent.append(
                f'"{key.codeName}": {expr}'
            )
        keyFormatted = f"{{{', '.join(keyContent)}}}"
        self.writeContent(
            f"{TAB}{{\n"
            f"{TAB}{TAB}record({keyFormatted}):"
        )
        
        # Write aggregations
        aggrContent = []
        for aggr in node.postAggregateOperations:
            # Skip Average aggregations for now
            if not isinstance(aggr, AvgAggrOperator):
                # And get the expr of the child
                if isinstance(aggr, CountAllOperator):
                    expr = convert_expression_operator_to_sdqlpy(aggr, lambda_index)
                else:
                    expr = convert_expression_operator_to_sdqlpy(aggr.child, lambda_index)
                
                aggrContent.append(
                    f'"{aggr.codeName}": {expr}'
                )
        aggrFormatted = f"{{{', '.join(aggrContent)}}}"
        self.writeContent(
            f"{TAB}{TAB}record({aggrFormatted})\n"
            f"{TAB}}}\n"
        )
        
        # Write filterContent, if we have it
        if node.filterContent != None:
            filterContent = convert_expression_operator_to_sdqlpy(node.filterContent, lambda_index)
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}None"
            )
        
        self.writeContent(
            f")"
        )
        
        # Do the summation at the end
        self.writeContent(
            f"{createdDictName} = {initialDictName}.sum(lambda {lambda_index} : {{unique({lambda_index}[0].concat({lambda_index}[1])): True}})"
        )
        
        # Set node.columns
        node.columns = set(node.postAggregateOperations)
            
    def visit_SDQLpyAggrNode(self, node):        
        # Get child name
        childTable = node.getChildName(self)
        
        createdDictName = node.getTableName(self)
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
        
        # Set node.columns
        node.columns = set(node.postAggregateOperations)
