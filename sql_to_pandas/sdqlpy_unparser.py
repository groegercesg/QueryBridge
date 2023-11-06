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
                if op_tree.tableRestrictions != []:
                    new_op_tree.addFilterContent(op_tree.tableRestrictions)
            case GroupNode():
                if op_tree.keyExpressions == []:
                    new_op_tree = SDQLpyAggrNode(
                        op_tree.postAggregateOperations
                    )
                else:
                    # A Group should be a Concat <- Group
                    group_node = SDQLpyGroupNode(
                        op_tree.keyExpressions,
                        op_tree.postAggregateOperations
                    )
                    new_op_tree = SDQLpyConcatNode(
                        group_node.outputColumns
                    )
                    new_op_tree.addChild(group_node)
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
        original_nodes = []
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
            # Only add to the lowest node
            # Find the lowest node
            lowest_node_pointer = new_op_tree
            searching = True
            while searching == True:
                match lowest_node_pointer:
                    case UnarySDQLpyNode():
                        if lowest_node_pointer.child != None:
                            original_nodes.append(lowest_node_pointer)
                            lowest_node_pointer = lowest_node_pointer.child
                        else:
                            searching = False
                    case _:
                        searching = False
            # Add lowest, as it won't have been gathered by the while
            original_nodes.append(lowest_node_pointer)
            match lowest_node_pointer:
                case UnarySDQLpyNode():
                    lowest_node_pointer.addChild(childNode)
                case BinarySDQLpyNode():
                    lowest_node_pointer.addLeft(leftNode)
                    lowest_node_pointer.addRight(rightNode)
                    if isinstance(lowest_node_pointer, SDQLpyJoinNode):
                        lowest_node_pointer.set_columns_variable()
                    else:
                        raise Exception("Binary with some Nones")
                case _:
                    # LeafSDQLpyNode
                    assert isinstance(lowest_node_pointer, LeafSDQLpyNode)
        
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
            # Order things that have output records
            case SDQLpyGroupNode() | SDQLpyJoinNode():
                assert sdqlpy_tree.outputRecord != None
                ordering = {k:v for v,k in enumerate(output_cols_order)}
                # Order keys as well
                sdqlpy_tree.outputRecord.keys.sort(key = lambda x : ordering.get(x.codeName))
                sdqlpy_tree.outputRecord.columns.sort(key = lambda x : ordering.get(x.codeName))
            case SDQLpyAggrNode():
                # No ordering required, as it only returns a single value
                pass
            case SDQLpyConcatNode():
                # This just concats the content from below
                # So we need to run the order method again
                orderTopNode(sdqlpy_tree.child, output_cols_order)
            case _:
                raise Exception(f"No ordering configured for node: {type(sdqlpy_tree)}")
            
    def foldConditionsAndOutputRecords(sdqlpy_tree):
        # Post Order traversal: Visit Children
        leftNode, rightNode, childNode = None, None, None
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = foldConditionsAndOutputRecords(sdqlpy_tree.left)
            rightNode = foldConditionsAndOutputRecords(sdqlpy_tree.right)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = foldConditionsAndOutputRecords(sdqlpy_tree.child)
        else:
            # A leaf node
            pass
        
        # Assign previous changes
        if (leftNode != None) and (rightNode != None):
            sdqlpy_tree.left = leftNode
            sdqlpy_tree.right = rightNode
        elif (childNode != None):
            sdqlpy_tree.child = childNode
        else:
            # A leaf node
            pass
        
        # Check child for filters, propagate them up
        if (childNode != None):
            # We are in a Unary Node
            match sdqlpy_tree.child:
                case LeafSDQLpyNode():
                    if isinstance(sdqlpy_tree.child, SDQLpyRecordNode):
                        if sdqlpy_tree.child.filterContent != None:
                            sdqlpy_tree.addFilterContent(sdqlpy_tree.child.filterContent)
                            sdqlpy_tree.child.filterContent = None
                    else:
                        raise Exception(f"Unknown Leaf Node that is a child: {type(sdqlpy_tree.child)}")
                case UnarySDQLpyNode():
                    if isinstance(sdqlpy_tree, PipelineBreakerNode):
                        match sdqlpy_tree:
                            case SDQLpyConcatNode():
                                # Can't filter in a concat
                                pass
                            case _:
                                raise Exception("An unknown Unary PipelineBreaker")
                case BinarySDQLpyNode():
                    if isinstance(sdqlpy_tree.child, SDQLpyJoinNode):
                        # If the current is a group, then we should set the output
                        if isinstance(sdqlpy_tree, SDQLpyGroupNode):
                            # Join should have no outputRecord yet
                            assert sdqlpy_tree.child.postJoinFilters == []
                            assert sdqlpy_tree.child.outputRecord == None
                            sdqlpy_tree.child.set_output_record(sdqlpy_tree.outputRecord)
                            # Set the output columns to be consistent
                            sdqlpy_tree.child.outputColumns = sdqlpy_tree.outputColumns
                            # Bump out the GroupNode
                            sdqlpy_tree = sdqlpy_tree.child
                        elif isinstance(sdqlpy_tree, SDQLpyAggrNode):
                            # Join should have no outputRecord yet
                            assert sdqlpy_tree.child.outputRecord == None
                            sdqlpy_tree.child.set_output_record(sdqlpy_tree.outputRecord)
                            # Set the output columns to be consistent
                            sdqlpy_tree.child.outputColumns = sdqlpy_tree.outputColumns
                            # Bump out the AggrNode
                            sdqlpy_tree = sdqlpy_tree.child
                        
                        # Joins have a postJoinFilter, this should be carried up
                        elif sdqlpy_tree.child.postJoinFilters != []:
                            sdqlpy_tree.addFilterContent(sdqlpy_tree.child.addFilterContent)
                    else:
                        raise Exception(f"Unknown Binary Node that is a child: {type(sdqlpy_tree.child)}")
                case _:
                    raise Exception()
        elif (leftNode != None) and (rightNode != None):
            if isinstance(sdqlpy_tree, SDQLpyJoinNode):
                # Run the join key separation now
                sdqlpy_tree.do_join_key_separation()
                
                # Make Left a JoinBuild, add the filter there
                # Leave Right a ScanNode, a Propagate right filter up to here
                if isinstance(sdqlpy_tree.left, SDQLpyRecordNode):
                    left_joinBuild = SDQLpyJoinBuildNode(
                        sdqlpy_tree.left.tableName,
                        sdqlpy_tree.leftKeys,
                        sdqlpy_tree.left.tableColumns
                    )
                    left_joinBuild.addFilterContent(sdqlpy_tree.left.filterContent)
                    sdqlpy_tree.left.filterContent = None
                    # Store the Record below
                    left_joinBuild.addChild(sdqlpy_tree.left)
                    sdqlpy_tree.left = left_joinBuild
                
                if isinstance(sdqlpy_tree.left, SDQLpyJoinNode):
                    # Set an output record for this node in new_op_tree
                    createdOutputRecord = SDQLpyRecordOutput(
                        sdqlpy_tree.leftKeys,
                        list(leftNode.left.outputColumns.union(leftNode.right.outputColumns) - set(sdqlpy_tree.leftKeys))
                    )
                    sdqlpy_tree.left.set_output_record(createdOutputRecord)
                elif isinstance(sdqlpy_tree.right, SDQLpyJoinNode):
                    # Set an output record for this node in new_op_tree
                    createdOutputRecord = SDQLpyRecordOutput(
                        sdqlpy_tree.rightKeys,
                        list(rightNode.left.outputColumns.union(rightNode.right.outputColumns) - set(sdqlpy_tree.rightKeys))
                    )
                    sdqlpy_tree.right.set_output_record(createdOutputRecord)
                    
                # Add right tableRestrictions
                if sdqlpy_tree.right.filterContent != None:
                    sdqlpy_tree.addFilterContent(sdqlpy_tree.right.filterContent)
                    # And reset right
                    sdqlpy_tree.right.filterContent = None
        else:
            assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
            
        # Return the tree, for the next iteration
        return sdqlpy_tree

    def joinPushDown(sdqlpy_tree):
        # Post Order traversal: Visit Children
        leftNode, rightNode, childNode = None, None, None
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = joinPushDown(sdqlpy_tree.left)
            rightNode = joinPushDown(sdqlpy_tree.right)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = joinPushDown(sdqlpy_tree.child)
        else:
            # A leaf node
            pass
        
        # Assign previous changes
        if (leftNode != None) and (rightNode != None):
            sdqlpy_tree.left = leftNode
            sdqlpy_tree.right = rightNode
        elif (childNode != None):
            sdqlpy_tree.child = childNode
        else:
            # A leaf node
            pass
        
        if (leftNode != None) and (rightNode != None):
            if isinstance(sdqlpy_tree, SDQLpyJoinNode):
                if not(isinstance(sdqlpy_tree.right, SDQLpyRecordNode) and isinstance(sdqlpy_tree.left, (SDQLpyJoinNode, SDQLpyJoinBuildNode))):
                    # Otherwise, we may have to do a join pushdown
                    assert isinstance(sdqlpy_tree.left, SDQLpyJoinBuildNode) and isinstance(sdqlpy_tree.right, SDQLpyJoinNode)
                    
                    # Make right a node that holds 3
                    # Attach to left to a child of right
                    sdqlpy_tree.right.add_third_node(sdqlpy_tree.left)
                    # Augment the outputRecord, to specify that the column
                        # Comes from 3rd child
                    originalOutputRecord = sdqlpy_tree.outputRecord
                    assert originalOutputRecord.checkForThirdNodeColumns(
                        sdqlpy_tree.left, sdqlpy_tree.rightKeys
                    ) >= 1
                    
                    # Push the outputRecord ontop of right's 
                    sdqlpy_tree.right.outputRecord = originalOutputRecord
                    # Set:
                    sdqlpy_tree = sdqlpy_tree.right
                    
        return sdqlpy_tree
    
    def set_update_sum_for_highest_join(sdqlpy_tree):
        match sdqlpy_tree:
            case SDQLpyJoinNode():
                sdqlpy_tree.update_update_sum(True)
                return
        
        # Post Order traversal: Visit Children
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            set_update_sum_for_highest_join(sdqlpy_tree.left)
            set_update_sum_for_highest_join(sdqlpy_tree.right)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            set_update_sum_for_highest_join(sdqlpy_tree.child)
        else:
            # A leaf node
            pass
    
    # Set the code names
    set_codeNames(universal_tree)
    output_cols_order = universal_tree.outputNames
    # Call convert trees
    sdqlpy_tree = convert_trees(universal_tree)
    # Fold conditions and output records into subsequent nodes
    sdqlpy_tree = foldConditionsAndOutputRecords(sdqlpy_tree)
    # Push down join conditions
    sdqlpy_tree = joinPushDown(sdqlpy_tree)
    # Set update sums correctly
    set_update_sum_for_highest_join(sdqlpy_tree)
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
        assert node.filterContent == None
        
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
            filterContent = convert_expr_to_sdqlpy(node.filterContent, f"{lambda_index}[0]", node.incomingColumns)
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
        # If it has a third_node, we should run that
        if node.third_node != None:
            self.__walk_tree(node.third_node)
        
        # Get child name
        leftTable, rightTable = node.getChildNames(self)
        
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        # TODO: We only support an inner hash join at the moment
        assert node.joinType == "inner" and node.joinMethod == "hash"
        
        assert isinstance(node.right, SDQLpyRecordNode) and isinstance(node.left, (SDQLpyJoinNode, SDQLpyJoinBuildNode))
        
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
            filterContent = convert_expr_to_sdqlpy(node.filterContent, f"{lambda_index}[0]", node.incomingColumns)
            self.writeContent(
                f"{TAB}lambda {lambda_index}: {filterContent},"
            )
        
        # Do outputRecord
        assert node.outputRecord != None
        
        left_lambda_index = "indexedDictValue"
        right_lambda_index = "probeDictKey"
        self.writeContent(
            f"{TAB}lambda {left_lambda_index}, {right_lambda_index}:"
        )
        for output_line in node.outputRecord.generateSDQLpyTwoLambda(
            left_lambda_index, right_lambda_index,
            node.left.outputColumns, node.right.outputColumns
        ):
            self.writeContent(
                f"{TAB}{output_line}"
            )
        
        # Write the postJoinFilters
        if node.postJoinFilters != None:
            filterContent = convert_expr_to_sdqlpy(
                node.postJoinFilters, left_lambda_index, node.left.outputColumns,
                right_lambda_index, node.right.outputColumns
            )
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}0.0"
            )
        
        # Write the update sum
        # Add comma to last part
        self.sdqlpy_content[-1] += ","
        self.writeContent(
            f"{TAB}{node.is_update_sum}\n"
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
        
        # Output the RecordOutput
        for output_line in node.outputRecord.generateSDQLpyOneLambda(
            f"{lambda_index}[0]", node.incomingColumns
        ):
            self.writeContent(
                f"{TAB}{output_line}"
            )
        
        # Write filterContent, if we have it
        if node.filterContent != None:
            filterContent = convert_expr_to_sdqlpy(node.filterContent, f"{lambda_index}[0]", node.incomingColumns)
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}None"
            )
        
        self.writeContent(
            f")"
        )
            
    def visit_SDQLpyAggrNode(self, node):        
        # Get child name
        childTable = node.getChildName(self)
        
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        self.writeContent(
            f"{createdDictName} = {childTable}.sum(\n"
            f"{TAB}lambda {lambda_index} :"
        )
        
        # Output the RecordOutput
        for output_line in node.outputRecord.generateSDQLpyOneLambda(
            f"{lambda_index}[0]", node.incomingColumns
        ):
            self.writeContent(
                f"{TAB}{output_line}"
            )
        
        if node.filterContent != None:
            filterContent = convert_expr_to_sdqlpy(node.filterContent, f"{lambda_index}[0]", node.incomingColumns)
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}0.0"
            )
        
        self.writeContent(
            f")"
        )
