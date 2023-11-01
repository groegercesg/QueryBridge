from collections import defaultdict

from universal_plan_nodes import *
from expression_operators import *

from sdqlpy_classes import *
from sdqlpy_helpers import *

TAB = "    "

def audit_sdqlpy_tree_recordnode(op_tree: SDQLpyBaseNode) -> bool:
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
                    op_tree.tableName,
                    op_tree.tableColumns
                )
            case GroupNode():
                if op_tree.keyExpressions == []:
                    new_op_tree = SDQLpyAggrNode(
                        op_tree.postAggregateOperations
                    )
                else:
                    new_op_tree = SDQLpyGroupNode(
                        op_tree.keyExpressions,
                        op_tree.postAggregateOperations
                    )
            case OutputNode():
                new_op_tree = None
            case JoinNode():
                new_op_tree = SDQLpyJoinNode(
                    op_tree.joinMethod,
                    op_tree.joinType,
                    op_tree.joinCondition
                )
            case _:
                raise Exception(f"Unexpected op_tree, it was of class: {op_tree.__class__}")

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
                        if isinstance(new_op_tree, SDQLpyJoinNode):
                            new_op_tree.set_columns_variable()
                    else:
                        raise Exception("Binary with some Nones")
                case _:
                    # LeafSDQLpyNode
                    assert isinstance(new_op_tree, LeafSDQLpyNode)
        
        # Pipeline breaker
        if isinstance(new_op_tree, PipelineBreakerNode):
            if childNode == new_op_tree:
                # They are equal, we skip
                pass
            elif childNode != None :
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
                        if isinstance(childNode, SDQLpyJoinNode):
                            # Joins have a postJoinFilter, this should be carried up
                            if childNode.postJoinFilters != []:
                                new_op_tree.addFilterContent(childNode.addFilterContent)
                        else:
                            raise Exception(f"Unknown Binary Node that is a child: {type(childNode)}")
                    case _:
                        raise Exception()
            elif (leftNode != None) and (rightNode != None):
                if isinstance(new_op_tree, SDQLpyJoinNode):
                    # Run the join key separation now
                    new_op_tree.do_join_key_separation()
                    
                    # Make Left a JoinBuild, add the filter there
                    # Leave Right a ScanNode, a Propagate right filter up to here
                    if isinstance(op_tree.left, ScanNode):
                        left_joinBuild = SDQLpyJoinBuildNode(
                            op_tree.left.tableName,
                            new_op_tree.leftKeys,
                            op_tree.left.tableColumns
                        )
                        left_joinBuild.addFilterContent(op_tree.left.tableRestrictions)
                        # Store the Record below
                        left_joinBuild.child = leftNode
                        new_op_tree.left = left_joinBuild
                    elif isinstance(op_tree.left, JoinNode):
                        # Set an output record for this node in new_op_tree
                        createdOutputRecord = SDQLpyRecordOutput(
                            new_op_tree.leftKeys,
                            [],
                            list(leftNode.left.columns),
                            list(leftNode.right.columns),
                        )
                        new_op_tree.left.set_output_record(createdOutputRecord)
                    else:
                        raise Exception("Unknown format for left/right of a JoinNode")
                        
                    # Add right tableRestrictions
                    new_op_tree.addFilterContent(op_tree.right.tableRestrictions)
                    
                    
                else:
                    raise Exception(f"A binary node, but no behavior implemented for it: {type(new_op_tree)}")
                        
            else:
                raise Exception("A leaf")
        
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
                # Do ordering, sort aggregateOperations by output_cols_order
                ordering = {k:v for v,k in enumerate(output_cols_order)}
                sdqlpy_tree.aggregateOperations.sort(key = lambda x : ordering.get(x.codeName))
            case SDQLpyAggrNode():
                # No ordering required, as it only returns a single value
                pass
            case _:
                raise Exception(f"No ordering configured for node: {type(sdqlpy_tree)}")
    
    def mergeGroupNodeDown(sdqlpy_tree):
        if isinstance(sdqlpy_tree, SDQLpyGroupNode) and isinstance(sdqlpy_tree.child, SDQLpyJoinNode):
            assert sdqlpy_tree.child.outputRecord == None
            
            # Determine which side of the thing it came from
            leftColumns = sdqlpy_tree.child.left.columns
            rightColumns = sdqlpy_tree.child.right.columns
            
            # TODO: New, Better method for this, this is awful currently
            for associateList in [sdqlpy_tree.keyExpressions, sdqlpy_tree.aggregateOperations]:
                for value in associateList:
                    # Have to surface all ColumnValue classes, then add this association\
                    if value in leftColumns:
                        value.associatedWith = "L"
                    elif value in rightColumns:
                        value.associatedWith = "R"
                    else:
                        raise Exception("Unable to determine which side the value was associated with")
                
            createdOutputRecord = SDQLpyRecordOutput(
                sdqlpy_tree.keyExpressions,
                sdqlpy_tree.aggregateOperations
            )
            sdqlpy_tree.child.set_output_record(createdOutputRecord)
            
            # Replace GroupNode with a SDQLpyConcatNode
            new_sdqlpy_tree = SDQLpyConcatNode(
                sdqlpy_tree.columns
            )
            new_sdqlpy_tree.child = sdqlpy_tree.child
            return new_sdqlpy_tree
        else:
            return sdqlpy_tree
    
    # Set the code names
    set_codeNames(universal_tree)
    output_cols_order = universal_tree.outputNames
    # Call convert trees
    sdqlpy_tree = convert_trees(universal_tree)
    # Order the topNode correctly
    orderTopNode(sdqlpy_tree, output_cols_order)
    # TODO: Pass to merge the GroupNodes down into the JoinBuild (?)
    sdqlpy_tree = mergeGroupNodeDown(sdqlpy_tree)
    
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
        
    def visit_SDQLpyConcatNode(self, node):
        # Get child name
        childTable = node.getChildName(self)
        
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        # Do the summation at the end
        self.writeContent(
            f"{createdDictName} = {childTable}.sum(lambda {lambda_index} : {{unique({lambda_index}[0].concat({lambda_index}[1])): True}})"
        )
        
    def visit_SDQLpyJoinBuildNode(self, node):
        # Get child name
        childTable = node.getChildName(self)
        
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        self.writeContent(
            f"{createdDictName} = {childTable}.joinBuild(\n"
            f'{TAB}"{node.tableKey.codeName}",'
        )
        
        if node.filterContent == None:
            self.writeContent(
                f"{TAB}lambda {lambda_index}: True,"
            )
        else:
            filterContent = convert_expression_operator_to_sdqlpy(node.filterContent, lambda_index)
            self.writeContent(
                f"{TAB}lambda {lambda_index}: {filterContent},"
            )
        
        # additionalColumns
        if node.additionalColumns == []:
            self.writeContent(
                f"{TAB}[]"
            )
        else:
            columnNames = []
            for col in node.additionalColumns:
                assert isinstance(col, ColumnValue)
                columnNames.append(col.codeName)
            columnContent = ", ".join(map(lambda x: f'"{x}"', columnNames))
            self.writeContent(
                f"{TAB}[{columnContent}]"
            )
        
        self.writeContent(
            f")\n"
        )
        
    def visit_SDQLpyJoinNode(self, node):
        # Get child name
        leftTable, rightTable = node.getChildNames(self)
        
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        assert node.joinType == "inner" and node.joinMethod == "hash"
        
        assert len(node.rightKeys) == 1 and isinstance(node.rightKeys[0], ColumnValue)
        rightKey = node.rightKeys[0].codeName
        self.writeContent(
            f"{createdDictName} = {rightTable}.joinProbe(\n"
            f'{TAB}{leftTable},\n'
            f'{TAB}"{rightKey}",'
        )
        
        # Filter Content
        if node.filterContent == None:
            self.writeContent(
                f"{TAB}lambda {lambda_index}: True,"
            )
        else:
            filterContent = convert_expression_operator_to_sdqlpy(node.filterContent, lambda_index)
            self.writeContent(
                f"{TAB}lambda {lambda_index}: {filterContent},"
            )
        
        # Do outputRecord
        assert node.outputRecord != None
        
        output_lambda_index = "probeDictKey"
        self.writeContent(
            f"{TAB}lambda indexedDictValue, {output_lambda_index}:"
        )
        for output_line in node.outputRecord.generateSDQLpy(output_lambda_index):
            self.writeContent(
                f"{TAB}{output_line}"
            )    
        
        self.writeContent(
            f")\n"
        )
        
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
        for aggr in node.aggregateOperations:
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
            
    def visit_SDQLpyAggrNode(self, node):        
        # Get child name
        childTable = node.getChildName(self)
        
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        assert len(node.aggregateOperations) == 1 and isinstance(node.aggregateOperations[0], SumAggrOperator)
        
        aggrContent = convert_expression_operator_to_sdqlpy(node.aggregateOperations[0].child, lambda_index)
        
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
