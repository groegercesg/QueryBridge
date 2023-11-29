from collections import defaultdict
import copy

from universal_plan_nodes import *
from expression_operators import *

from sdqlpy_classes import *
from sdqlpy_helpers import *

TAB = "    "

def audit_sdqlpy_tree_leafnode(op_tree: SDQLpyBaseNode) -> bool:
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
    
    # Get all leaves, make sure they're all LeafSDQLpyNode
    all_leaves = get_leaf_nodes(op_tree)
    return all(isinstance(leaf, LeafSDQLpyNode) for leaf in all_leaves)

def convert_universal_to_sdqlpy(universal_tree: UniversalBaseNode, table_keys: dict[str, tuple]) -> SDQLpyBaseNode:
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
                if (op_tree.tableFilters != []) and (op_tree.tableRestrictions != []):
                    raise Exception("Two types of filters, we need to add support for understanding this")
                elif op_tree.tableFilters != []:
                    new_op_tree.addFilterContent(op_tree.tableFilters)
                elif op_tree.tableRestrictions != []:
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
                        op_tree.keyExpressions + op_tree.postAggregateOperations
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
            case FilterNode():
                new_op_tree = SDQLpyFilterNode()
                new_op_tree.addFilterContent(op_tree.condition)
            case NewColumnNode():
                assert isinstance(childNode, SDQLpyJoinNode)
                new_op_tree = None
                # Add the values to the child
                childNode.outputDict.keys.append(op_tree.values)
            case RetrieveNode():
                new_op_tree = SDQLpyRetrieveNode(
                    op_tree.tableColumns,
                    op_tree.retrieveTargetID
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
                    # if isinstance(lowest_node_pointer, SDQLpyJoinNode):
                    #     lowest_node_pointer.set_output_dict()
                    # else:
                    #     raise Exception("Binary with some Nones")
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
            # Add cardinality to new_op_node
            if new_op_tree.cardinality == None:
                new_op_tree.setCardinality(op_tree.cardinality)
        
        return new_op_tree
    
    # Use the output node to set relevant code names
    def set_codeNames(topNode):
        assert isinstance(topNode, OutputNode)
        assert len(topNode.outputNames) == len(topNode.outputColumns)
        
        for idx, name in enumerate(topNode.outputNames):
            topNode.outputColumns[idx].codeName = name
                
    def orderTopNode(sdqlpy_tree, output_cols_order):
        match sdqlpy_tree:
            # Order things that have output records
            case SDQLpyGroupNode() | SDQLpyJoinNode():
                assert sdqlpy_tree.outputDict != None
                ordering = {k:v for v,k in enumerate(output_cols_order)}
                # Order keys as well
                sdqlpy_tree.outputDict.keys.sort(key = lambda x : ordering.get(x.codeName, -1))
                sdqlpy_tree.outputDict.values.sort(key = lambda x : ordering.get(x.codeName, -1))
                # Drop keys that aren't in ordering
                sdqlpy_tree.outputDict.keys = [x for x in sdqlpy_tree.outputDict.keys if ordering.get(x.codeName, None) != None]
                sdqlpy_tree.outputDict.values = [x for x in sdqlpy_tree.outputDict.values if ordering.get(x.codeName, None) != None]
            case SDQLpyAggrNode():
                # No ordering required, as it only returns a single value
                pass
            case SDQLpyConcatNode():
                # This just concats the content from below
                # So we need to run the order method again
                orderTopNode(sdqlpy_tree.child, output_cols_order)
            case _:
                raise Exception(f"No ordering configured for node: {type(sdqlpy_tree)}")
            
    def insert_join_probe_nodes(sdqlpy_tree):
        # Post Order traversal: Visit Children
        leftNode, rightNode, childNode = None, None, None
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = insert_join_probe_nodes(sdqlpy_tree.left)
            rightNode = insert_join_probe_nodes(sdqlpy_tree.right)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = insert_join_probe_nodes(sdqlpy_tree.child)
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
        
        # Work on current
        if isinstance(sdqlpy_tree, SDQLpyJoinNode):
            sdqlpy_tree.do_join_key_separation()
            
            assert isinstance(sdqlpy_tree.right, (SDQLpyRecordNode, SDQLpyJoinNode, SDQLpyFilterNode, SDQLpyRetrieveNode))
            
            # Turn a Record or JoinNode on the left into a JoinBuildNode
            if isinstance(sdqlpy_tree.left, (SDQLpyRecordNode, SDQLpyJoinNode, SDQLpyFilterNode, SDQLpyAggrNode)):
                # Make it a SDQLpyJoinBuildNode
                jbNode = SDQLpyJoinBuildNode(
                    sdqlpy_tree.leftKeys,
                    list(sdqlpy_tree.left.outputDict.flatCols())
                )
                jbNode.addChild(sdqlpy_tree.left)
                jbNode.setCardinality(jbNode.child.cardinality)
                sdqlpy_tree.left = jbNode
            else:
                raise Exception("Unknown left of Join Node")
            
        # Return the tree, for the next iteration
        return sdqlpy_tree
    
    def wire_up_incoming_output_dicts(sdqlpy_tree, no_sumaggr_warn=False):
        # Post Order traversal: Visit Children
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            wire_up_incoming_output_dicts(sdqlpy_tree.left, no_sumaggr_warn)
            wire_up_incoming_output_dicts(sdqlpy_tree.right, no_sumaggr_warn)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            wire_up_incoming_output_dicts(sdqlpy_tree.child, no_sumaggr_warn)
        else:
            # A leaf node
            assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
            pass
        
        if isinstance(sdqlpy_tree, LeafSDQLpyNode):
            # Incoming is already set
            sdqlpy_tree.set_output_dict(no_sumaggr_warn)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            # Set incoming and run output dict
            sdqlpy_tree.incomingDict = sdqlpy_tree.child.outputDict
            sdqlpy_tree.set_output_dict(no_sumaggr_warn)
        else:
            assert isinstance(sdqlpy_tree, BinarySDQLpyNode)
            # Incoming dicts should already be set
            assert len(sdqlpy_tree.incomingDicts) == 2
            sdqlpy_tree.set_output_dict(no_sumaggr_warn)
            
    def duplicateFixTopGroupJoin(sdqlpy_tree):
        match sdqlpy_tree:
            # Order things that have output records
            case SDQLpyGroupNode():
                if isinstance(sdqlpy_tree.child, SDQLpyJoinNode):
                    # Add the child
                    assert len(sdqlpy_tree.child.outputDict.values) == 0
                    sdqlpy_tree.child.outputDict.set_duplicateCounter(True)
                    
                    # Use the group
                    sdqlpy_tree.outputDict.set_duplicateUser(True)
            case SDQLpyJoinNode():
                # No duplicates, it's a Join
                pass
            case SDQLpyAggrNode():
                # No ordering required, as it only returns a single value
                pass
            case SDQLpyConcatNode():
                # This just concats the content from below
                # So we need to run the order method again
                duplicateFixTopGroupJoin(sdqlpy_tree.child)
            case _:
                raise Exception(f"No duplicateFix configured for node: {type(sdqlpy_tree)}")
            
    def convertSumAggrOperatorToColumnNode(sdqlpy_tree):
        parserCreatedColumns = set()
        replacementDict = dict()
        
        # Post Order traversal: Visit Children
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftCreated, leftReplaces = convertSumAggrOperatorToColumnNode(sdqlpy_tree.left)
            rightCreated, rightReplaces = convertSumAggrOperatorToColumnNode(sdqlpy_tree.right)
            parserCreatedColumns = leftCreated.union(rightCreated)
            replacementDict.update(**leftReplaces, **rightReplaces)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            belowCreated, belowReplaces = convertSumAggrOperatorToColumnNode(sdqlpy_tree.child)
            parserCreatedColumns = belowCreated
            replacementDict.update(belowReplaces)
        else:
            # A leaf node
            assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
            pass
        
        assert sdqlpy_tree.outputDict != None
        
        after_current_node_values = list()
        
        # Iterate through keys and values
        # Create the options for the replacementDict
        for valuesLocation in [sdqlpy_tree.outputDict.keys, sdqlpy_tree.outputDict.values]:
            for idx, val in enumerate(valuesLocation):
                if isinstance(val, (SumAggrOperator)):
                    if str(id(val)) not in replacementDict:
                        # Create a new one in the replacementDict
                        if val.codeName == "":
                            handleEmptyCodeName(val, parserCreatedColumns)
                        # What is the Aggregations are nested
                        desiredChild = val.child
                        while isinstance(desiredChild, (SumAggrOperator)):
                            desiredChild = desiredChild.child
                        valueCopy = copy.deepcopy(desiredChild)
                        valueCopy.codeName = val.codeName
                        
                        assert valueCopy.type != ''
                        newValue = ColumnValue(valueCopy.codeName, valueCopy.type)
                        newValue.codeName = valueCopy.codeName
                        
                        replacementDict[str(id(val))] = valueCopy
                        after_current_node_values.extend(
                            [
                                (str(id(val)), newValue),
                                (str(id(valueCopy)), newValue)
                            ]
                        )
                elif isinstance(val, (MaxAggrOperator, AvgAggrOperator, MinAggrOperator)):
                    raise Exception("Max/Min/Avg operator detected, we don't have support for these")
        
        # Set the replacementDict
        sdqlpy_tree.setReplacementDict(replacementDict)
        
        # Use the replacementDict
        sdqlpy_tree.set_output_dict()
        
        # Update the replacementDict
        # We want to set the current_node to solely the ValueCopy,
        # but we want all subsequent to use the created version of that node
        # This is what we store along with the key in this list
        
        newReplacementDict = dict()
        # Fill with current dict values
        for key, value in replacementDict.items():
            newReplacementDict[key] = value
        if after_current_node_values != []:
            for key, value in after_current_node_values:
                newReplacementDict[key] = value
                
        return parserCreatedColumns, newReplacementDict
    
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
                
                # Catch more than 1 keys for a join, turn into a SDQLpyNKeyJoin Node
                if (len(sdqlpy_tree.leftKeys) > 1) or (len(sdqlpy_tree.rightKeys) > 1):
                    assert len(sdqlpy_tree.leftKeys) == len(sdqlpy_tree.rightKeys)
                    # Create the SDQLpyNKeyJoin node
                    newNKeyJoin = SDQLpyNKeyJoin(
                        sdqlpy_tree.left,
                        sdqlpy_tree.leftKeys,
                        sdqlpy_tree.rightKeys
                    )
                    # Replace sdqlpy_tree with the right node
                    sdqlpy_tree = sdqlpy_tree.right
                    
                    # Add the SDQLpyNKeyJoin to the new sdqlpy_tree as a postJoinFilters Filter
                    assert (sdqlpy_tree.postJoinFilters == None)
                    sdqlpy_tree.postJoinFilters = newNKeyJoin
                
                else:
                    # Make Left a JoinBuild, add the filter there
                    # Leave Right a ScanNode, a Propagate right filter up to here
                    if isinstance(sdqlpy_tree.left, SDQLpyRecordNode):
                        left_joinBuild = SDQLpyJoinBuildNode(
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
                        createdOutputRecord = SDQLpySRDict(
                            sdqlpy_tree.leftKeys,
                            list(leftNode.left.outputColumns.union(leftNode.right.outputColumns))  # - set(sdqlpy_tree.leftKeys))
                        )  
                        sdqlpy_tree.left.set_output_record(createdOutputRecord)
                    elif isinstance(sdqlpy_tree.right, SDQLpyJoinNode):
                        # Set an output record for this node in new_op_tree
                        createdOutputRecord = SDQLpySRDict(
                            sdqlpy_tree.rightKeys,
                            list(rightNode.left.outputColumns.union(rightNode.right.outputColumns)) # - set(sdqlpy_tree.rightKeys))
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
        
    def order_joins(sdqlpy_tree, table_keys):
        def getPrimaryKeys(table_keys: dict) -> set:
            allPrimaryKeys = set()
            for key, value in table_keys.items():
                assert isinstance(value[0], set)
                allPrimaryKeys.update(
                    value[0]
                )
            return allPrimaryKeys
        
        def countKeysInSet(list_keys: list, allKeys: set) -> int:
            assert isinstance(list_keys, list)
            for x in list_keys:
                if isinstance(x, ColumnValue):
                    assert x.value == x.codeName
            
            # Convert to set 
            currentKeys = set(
                [x.codeName for x in list_keys]
            )
            
            # Get intersection, the number of currentKeys that are in
            # allKeys
            return len(allKeys.intersection(currentKeys))
        
        def calculateProblemColumns(indexKeys: list, outputDict: SDQLpySRDict, table_keys: dict) -> int:
            def convertTableKeysToPrimaryOtherMapping(table_keys) -> dict:
                primaryDict = dict()
                for key, value in table_keys.items():
                    for primary in value[0]:
                        primaryDict[primary] = set(value[1].union(value[2]))
                return primaryDict
            
            # Get the number of columns that will be a problem, if we index on the indexKeys
            indexKeys_str = set(
                [x.codeName for x in indexKeys]
            )
            # Step 1: Carry forward keys that are type: Varchar/Date/Char
            potentialProblemKeys_str = set(
                [x.codeName for x in outputDict.flatCols() if x.type in ['Varchar', 'Date', 'Char']]
            )
            primaryKeyMapping = convertTableKeysToPrimaryOtherMapping(table_keys)
            
            # Step 2: Remove keys that are mapped from the indexKeys
            for index_key in indexKeys_str:
                # Use set(), for none return
                potentialProblemKeys_str = potentialProblemKeys_str - primaryKeyMapping.get(index_key, set())
            
            return len(potentialProblemKeys_str)
        
        # Post Order traversal: Visit Children
        leftNode, rightNode, childNode = None, None, None
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = order_joins(sdqlpy_tree.left, table_keys)
            rightNode = order_joins(sdqlpy_tree.right, table_keys)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = order_joins(sdqlpy_tree.child, table_keys)
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
        
        # Run on current node (sdqlpy_tree)
        if isinstance(sdqlpy_tree, SDQLpyJoinNode):
            sdqlpy_tree.do_join_key_separation()
            
            # Check the cardinality of left and right
            # Left should be <= right, otherwise we need to swap
            assert sdqlpy_tree.left.cardinality != None
            assert sdqlpy_tree.right.cardinality != None
            
            all_primary_keys = getPrimaryKeys(table_keys)
            leftKeysThatArePrimary = countKeysInSet(
                sdqlpy_tree.leftKeys, all_primary_keys
            )
            rightKeysThatArePrimary = countKeysInSet(
                sdqlpy_tree.rightKeys, all_primary_keys
            )           
            
            if leftKeysThatArePrimary > rightKeysThatArePrimary:
                # We should index on Left
                # No swap required
                pass
            elif leftKeysThatArePrimary < rightKeysThatArePrimary:
                # We should index on Right
                # Swap required
                sdqlpy_tree.swapLeftAndRight()
                
                # Check the primary keys have resolved
                leftKeysThatArePrimary = countKeysInSet(
                    sdqlpy_tree.leftKeys, all_primary_keys
                )
                rightKeysThatArePrimary = countKeysInSet(
                    sdqlpy_tree.rightKeys, all_primary_keys
                )
                
                assert leftKeysThatArePrimary > rightKeysThatArePrimary
            else:
                # number of Primary keys is the same
                # We should look at "Problem Keys", these are keys that will cause issues if we try
                # to use them in the "value" section of a summation
                
                leftProblems = calculateProblemColumns(sdqlpy_tree.leftKeys, sdqlpy_tree.left.outputDict, table_keys)
                rightProblems = calculateProblemColumns(sdqlpy_tree.rightKeys, sdqlpy_tree.right.outputDict, table_keys)
             
                if (leftProblems == 0) and (rightProblems == 0):
                    # Problem keys are not an issue
                    
                    # Check leftsemijoin
                    if sdqlpy_tree.joinType == "leftsemijoin":
                        sdqlpy_tree.joinType = "rightsemijoin"
                        
                        sdqlpy_tree.swapLeftAndRight()
                        # Assert cardinality okay
                        assert sdqlpy_tree.left.cardinality <= sdqlpy_tree.right.cardinality
                    else:
                        # Now order based on cardinality
                        if sdqlpy_tree.left.cardinality <= sdqlpy_tree.right.cardinality:
                            # We should index on Left
                            # No swap required
                            pass
                        else:
                            # We should index on Right
                            # Swap required
                            sdqlpy_tree.swapLeftAndRight()
                            assert sdqlpy_tree.left.cardinality <= sdqlpy_tree.right.cardinality
                else:
                    # Left or Right Problems exist
                    if (leftProblems > 0) and (rightProblems == 0):
                        # Left is an issue
                        # We should index on Right
                        # Swap Required
                        sdqlpy_tree.swapLeftAndRight()
                    elif (leftProblems == 0) and (rightProblems > 0):
                        # Right is an issue
                        # We should index on Left
                        # No swap required
                        pass
                    else:
                        raise Exception("We have problems on both Left and Right; this is an issue that cannot be resolved!")

        return sdqlpy_tree     
    
    def insertPostJoinFilterNodes(sdqlpy_tree):
        # Post Order traversal: Visit Children
        leftNode, rightNode, childNode = None, None, None
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = insertPostJoinFilterNodes(sdqlpy_tree.left)
            rightNode = insertPostJoinFilterNodes(sdqlpy_tree.right)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = insertPostJoinFilterNodes(sdqlpy_tree.child)
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
        
        # Run on current node (sdqlpy_tree)
        if isinstance(sdqlpy_tree, SDQLpyJoinNode):
            if sdqlpy_tree.postJoinFilters != None:
                # Make a filterNode
                newFilter = SDQLpyFilterNode()
                newFilter.addFilterContent(sdqlpy_tree.postJoinFilters)
                sdqlpy_tree.postJoinFilters = None
                newFilter.setCardinality(sdqlpy_tree.cardinality)
                newFilter.addChild(sdqlpy_tree)
                newFilter.set_output_dict()
                sdqlpy_tree = newFilter
                
        return sdqlpy_tree
    
    # Set the code names
    set_codeNames(universal_tree)
    output_cols_order = universal_tree.outputNames
    # Call convert trees
    sdqlpy_tree = convert_trees(universal_tree)
    # Wire up incoming/output Dicts
    wire_up_incoming_output_dicts(sdqlpy_tree, no_sumaggr_warn=True)
    # Convert SumAggrOperator to ColumnNode
    convertSumAggrOperatorToColumnNode(sdqlpy_tree)
    # Order joins, using cardinality information
    sdqlpy_tree = order_joins(sdqlpy_tree, table_keys)
    # Insert JoinProbe Nodes
    sdqlpy_tree = insert_join_probe_nodes(sdqlpy_tree)
    # move postJoinFilterNodes into a separate FilterNode
    sdqlpy_tree = insertPostJoinFilterNodes(sdqlpy_tree)
    # Wire up incoming/output Dicts
    wire_up_incoming_output_dicts(sdqlpy_tree)
    # solve Duplicates by multiplying by a counter
    duplicateFixTopGroupJoin(sdqlpy_tree)
    # Wire up incoming/output Dicts
    wire_up_incoming_output_dicts(sdqlpy_tree)
    
    # Everything below is relating to optimisations
    
    # Fold conditions and output records into subsequent nodes
    # sdqlpy_tree = foldConditionsAndOutputRecords(sdqlpy_tree)
    # Push down join conditions
    # sdqlpy_tree = joinPushDown(sdqlpy_tree)
    # Set update sums correctly
    # set_update_sum_for_highest_join(sdqlpy_tree)
    
    # Order the topNode correctly
    orderTopNode(sdqlpy_tree, output_cols_order)
    
    return sdqlpy_tree

# Unparser
class UnparseSDQLpyTree():
    def __init__(self, sdqlpy_tree: SDQLpyBaseNode) -> None:
        self.sdqlpy_content = []
        self.sdqlpy_temp_content = []
        self.nodesCounter = defaultdict(int)
        self.sdqlpy_tree = sdqlpy_tree
        
        self.relations = set()
        self.parserCreatedColumns = set()
        self.nodeDict = {}
        self.gatherNodeDict(self.sdqlpy_tree)
        
        # Variable dict
        self.variableDict = {}
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
    
    def writeTempContent(self, content: str) -> None:
        self.sdqlpy_temp_content.append(content)
    
    def commitTempContent(self) -> None:
        assert (self.sdqlpy_temp_content != [])
        self.sdqlpy_content.extend(self.sdqlpy_temp_content)
        self.sdqlpy_temp_content = []
        
    def getSDQLpyContent(self) -> list[str]:
        # We need to return, with also the variableDict content
        variableDictContent = []
        for var_name, var_string in self.variableDict.items():
            variableDictContent.append(
                # Enclose the RHS in quotation marks
                f"{var_name} = '{var_string}'"
            )
        
        if variableDictContent != []:
            # Add an empty string at the end, to create a newline
            variableDictContent.append("")
        
        return variableDictContent + self.sdqlpy_content
    
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
        # Refresh current_node's outputDict, before running
        assert hasattr(current_node, "set_output_dict")
        current_node.set_output_dict()
        if hasattr(self, targetVisitorMethod):
            # Count number of nodes
            self.nodesCounter[current_node.__class__.__name__] += 1
            getattr(self, targetVisitorMethod)(current_node)
        else:
            raise Exception(f"No visit method found for class name: {current_node.__class__.__name__}, was expected to find a: '{targetVisitorMethod}' method.")
        
    def visit_SDQLpyRetrieveNode(self, node):
        assert node.retrieveTargetID in self.nodeDict
        retrievedNode = self.nodeDict[node.retrieveTargetID]
        
        nodeTableColumnsCounter = Counter([type(x) for x in node.outputDict.flatCols()])
        retrievedNodeColumnsCounter = Counter([type(x) for x in retrievedNode.outputDict.flatCols()])
        
        assert len(node.outputDict.flatCols()) == len(retrievedNode.outputDict.flatCols())
        assert nodeTableColumnsCounter == retrievedNodeColumnsCounter
        assert len(set(nodeTableColumnsCounter.values())) <= 1, "All should have the same value"
        assert all(1 == x for x in nodeTableColumnsCounter.values()), "All should be 1"
        
        node.getTableName(self)
        node.tableName = retrievedNode.tableName
        
    def visit_SDQLpyFilterNode(self, node):
        # Get child name
        childTable = node.getChildName(self)
        
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        self.writeContent(
            f"{createdDictName} = {childTable}.sum(\n"
            f"{TAB}lambda {lambda_index} : "
        )
        
        for output_line in node.outputDict.generateSDQLpyOneLambda(
                self, f"{lambda_index}[0]", f"{lambda_index}[1]", node
            ):
                self.writeContent(
                    f"{TAB}{TAB}{output_line}"
                )
            
        # If there's a filter, then carry it out
        filterContent = self.convert_expr_to_sdqlpy(
            node.filterContent,
            f"{lambda_index}[0]",
            node.incomingDict.flatKeys(),
            f"{lambda_index}[1]",
            node.incomingDict.flatVals()
        )
        
        self.writeContent(
            f"{TAB}if\n"
            f"{TAB}{TAB}{filterContent}\n"
            f"{TAB}else\n"
            f"{TAB}{TAB}None\n"
            f")"
        )
    
    def visit_SDQLpyRecordNode(self, node):
        self.relations.add(node.tableName)
        createdDictName = node.getTableName(self)
        
        if node.filterContent != None:
            originalTableName = node.sdqlrepr
            lambda_index = "p"
            
            self.writeContent(
                f"{createdDictName} = {originalTableName}.sum(\n"
                f"{TAB}lambda {lambda_index} : "
            )
            
            for output_line in node.outputDict.generateSDQLpyOneLambda(
                self, f"{lambda_index}[0]", f"{lambda_index}[1]", node
            ):
                self.writeContent(
                    f"{TAB}{TAB}{output_line}"
                )
            
            # If there's a filter, then carry it out
            filterContent = self.convert_expr_to_sdqlpy(
                node.filterContent,
                f"{lambda_index}[0]",
                node.incomingDict.flatKeys(),
                f"{lambda_index}[1]",
                node.incomingDict.flatVals()
            )
            
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}None\n"
                f")"
            )
        
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
            f"{createdDictName} = {childTable}.sum(\n"
            f"{TAB}lambda {lambda_index}:"
        )
        
        for output_line in node.outputDict.generateSDQLpyOneLambda(
            self, f"{lambda_index}[0]", f"{lambda_index}[1]", node
        ):
            self.writeContent(
                f"{TAB}{TAB}{output_line}"
            )
            
        if node.filterContent != None:
            filterContent = self.convert_expr_to_sdqlpy(
                node.filterContent,
                f"{lambda_index}[0]",
                node.incomingDict.flatKeys(),
                f"{lambda_index}[1]",
                node.incomingDict.flatVals()
            )
            
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}None"
            )
        
        self.writeContent(
            f")"
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
        assert node.joinType in node.KNOWN_JOIN_TYPES and node.joinMethod == "hash"
        
        assert isinstance(node.left, SDQLpyJoinBuildNode) and isinstance(node.right, (SDQLpyRecordNode, SDQLpyJoinNode, SDQLpyFilterNode)) 
        
        self.writeTempContent(
            f"{createdDictName} = {rightTable}.sum(\n"
            f"{TAB}lambda {lambda_index} : "
        )
        
        leftTableRef = node.make_leftTableRef(self, lambda_index)
        
        # Write the output Record
        for output_line in node.get_output_dict().generateSDQLpyTwoLambda(
            self, leftTableRef, f"{lambda_index}[0]", f"{lambda_index}[1]",
            node.left, node.right
        ):
            self.writeTempContent(
                f"{TAB}{TAB}{output_line}"
            )
        
        self.writeTempContent(
            f"{TAB}if\n"
            f"{TAB}{TAB}{leftTableRef} != None\n"
            f"{TAB}else\n"
            f"{TAB}{TAB}None"
        )
        
        # Filter Content
        assert node.filterContent == None
        assert node.postJoinFilters == None
            
        self.writeTempContent(
            f")"
        )
        # Commit Temp Content, now that we're finished
        self.commitTempContent()
        
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
        for output_line in node.outputDict.generateSDQLpyOneLambda(
            self, f"{lambda_index}[0]", f"{lambda_index}[1]", node
        ):
            self.writeContent(
                f"{TAB}{output_line}"
            )
        
        # Write filterContent, if we have it
        if node.filterContent != None:
            filterContent = self.convert_expr_to_sdqlpy(
                node.filterContent,
                f"{lambda_index}[0]",
                node.incomingDict.flatKeys(),
                f"{lambda_index}[1]",
                node.incomingDict.flatVals()
            )
            
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}None"
            )
        
        self.writeContent(
            f")"
        )
        
    # def visit_SDQLpyNKeyJoin(self, node):
    #     # Make leftKeys a RecordOutput
    #     self.visit_SDQLpyRecordNode(node.leftNode)
    #     leftRecord = SDQLpySRDict(
    #         node.leftKeys,
    #         []
    #     )
    #     leftRecord.setUnique(True)
        
    #     childTable = node.leftNode.tableName
        
    #     createdDictName = f"{childTable}_project"
    #     lambda_index = "p"
        
    #     self.writeContent(
    #         f"{createdDictName} = {childTable}.sum(lambda {lambda_index}:"
    #     )
        
    #     for codeRow in leftRecord.generateSDQLpyOneLambda(
    #         self, f"{lambda_index}[0]", node.leftKeys
    #     ):
    #         self.writeContent(
    #             f"{TAB}{codeRow}"
    #         )
        
    #     self.writeContent(
    #         f")\n"
    #     )
        
    #     node.setTableName(createdDictName)
            
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
        for output_line in node.outputDict.generateSDQLpyOneLambda(
            self, f"{lambda_index}[0]", f"{lambda_index}[1]", node
        ):
            self.writeContent(
                f"{TAB}{output_line}"
            )
        
        if node.filterContent != None:
            filterContent = self.convert_expr_to_sdqlpy(
                node.filterContent,
                f"{lambda_index}[0]",
                node.incomingDict.flatKeys(),
                f"{lambda_index}[1]",
                node.incomingDict.flatVals()
            )
            
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}0.0"
            )
        
        self.writeContent(
            f")"
        )
    
    # =========================
    # Unparser helper functions
    
    
    def convert_nkeyjoin_to_sdqlpy(self, value, l_lambda_idx, l_node_columns, r_lambda_idx = None, r_node_columns = None):
        assert isinstance(value, SDQLpyNKeyJoin)
        self.visit_SDQLpyNKeyJoin(value)
        
        for r_key in value.rightKeys:
            setSourceNodeColumnValues(r_key, l_lambda_idx, l_node_columns, r_lambda_idx, r_node_columns)
        expr_content = self.__convert_expression_operator_to_sdqlpy(value)
        for r_key in value.rightKeys:
            resetColumnValues(r_key)
        
        return expr_content
    
    def convert_expr_to_sdqlpy(self, value, l_lambda_idx, l_node_columns, r_lambda_idx = None, r_node_columns = None):
        setSourceNodeColumnValues(value, l_lambda_idx, l_node_columns, r_lambda_idx, r_node_columns)
        expr_content = self.__convert_expression_operator_to_sdqlpy(value)
        resetColumnValues(value)
        
        return expr_content
    
    def __convert_expression_operator_to_sdqlpy(self, expr_tree: ExpressionBaseNode) -> str:
        # Visit Children
        if isinstance(expr_tree, BinaryExpressionOperator):
            leftNode = self.__convert_expression_operator_to_sdqlpy(expr_tree.left)
            rightNode = self.__convert_expression_operator_to_sdqlpy(expr_tree.right)
        elif isinstance(expr_tree, UnaryExpressionOperator):
            childNode = self.__convert_expression_operator_to_sdqlpy(expr_tree.child)
        else:
            # A value node
            assert isinstance(expr_tree, (LeafNode, SDQLpyNKeyJoin))
            pass
        
        expression_output = None
        match expr_tree:
            case ColumnValue():
                assert expr_tree.sourceNode != None
                expression_output = f"{expr_tree.sourceNode}.{expr_tree.value}"
                # Update the value after creation
                expr_tree.value = expr_tree.codeName
            case ConstantValue():
                expression_output = self.__handle_ConstantValue(expr_tree)
            case LessThanOperator():
                expression_output = f"({leftNode} < {rightNode})"
            case LessThanEqOperator():
                expression_output = f"({leftNode} <= {rightNode})"
            case GreaterThanOperator():
                expression_output = f"({leftNode} > {rightNode})"
            case GreaterThanEqOperator():
                expression_output = f"({leftNode} >= {rightNode})"
            case IntervalNotionOperator():
                expression_output = self.__handle_IntervalNotion(expr_tree)
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
                expression_output = "1.0"
            case EqualsOperator():
                expression_output = f"({leftNode} == {rightNode})"
            case SDQLpyThirdNodeWrapper():
                expression_output = self.__handle_SDQLpyThirdNodeWrapper(expr_tree)
            case InSetOperator():
                expression_output = self.__handle_InSetOperator(expr_tree)
            case SDQLpyNKeyJoin():
                expression_output = self.__handle_SDQLpyNKeyJoin(expr_tree)
            case SDQLpyLambdaReference():
                expression_output = f"{expr_tree.value}"
            case LikeOperator():
                expression_output = self.__handle_LikeOperator(expr_tree)
            case ExtractYearOperator():
                expression_output = self.__handle_ExtractYearOperator(expr_tree)
            case _: 
                raise Exception(f"Unrecognised expression operator: {type(expr_tree)}")

        return expression_output
    
    def __handle_ExtractYearOperator(self, expr: ExtractYearOperator):
        childValue = self.__convert_expression_operator_to_sdqlpy(expr.child)
        return f"extractYear({childValue})"
    
    def __handle_LikeOperator(self, expr: LikeOperator):
        # Process the comparator
        if expr.comparator.value.count("%") == 2:
            assert isinstance(expr.comparator, ConstantValue)
            assert isinstance(expr.value, ColumnValue)
            
            expr.comparator.value = expr.comparator.value.replace("%", "")
            
            # 2 percentages means "in"
            comparator_value = self.__convert_expression_operator_to_sdqlpy(expr.comparator)
            column_value = self.__convert_expression_operator_to_sdqlpy(expr.value)
            return f"{comparator_value} in {column_value}"
        else:
            # Unknown number of percentages
            # Should use "startsWith"/"endsWith" macros from SDQLpy
            raise Exception(f"Unknown format of comparator: {expr.comparator.value}")
    
    def __handle_ConstantValue(self, expr: ConstantValue):
        if expr.type == "String":
            # Save value in variableDict
            variableString = str(expr.value)
            newVariable = variableString.replace(" ", "_").replace("#", "").lower()
            if newVariable in self.variableDict:
                if self.variableDict.get(newVariable) == variableString:
                    # The same string is happening twice, just use the existing created reference
                    return f"{newVariable}"
                else:
                    # We need to complicate our variable naming futher
                    raise Exception("Variable name corresponds to different strings")
            else:
                self.variableDict[newVariable] = variableString
                return f"{newVariable}"
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
            
    def __handle_IntervalNotion(self, expr: IntervalNotionOperator):
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
        
        sdqlpyExpression = self.__convert_expression_operator_to_sdqlpy(convertedExpression)
        return sdqlpyExpression
        
    def __handle_SDQLpyThirdNodeWrapper(self, expr_tree):
        outputString = f"{expr_tree.third_node.tableName}[{expr_tree.sourceNode}.{expr_tree.target_key.codeName}].{expr_tree.col.codeName}"
        return outputString
        
    def __handle_InSetOperator(self, expr: InSetOperator):
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
        
        return f"({self.__convert_expression_operator_to_sdqlpy(or_tree)})"

    def __handle_SDQLpyNKeyJoin(self, expr_tree):
        keyContent = []
        for r_key in expr_tree.rightKeys:
            expr = self._UnparseSDQLpyTree__convert_expression_operator_to_sdqlpy(r_key)
            keyContent.append(
                f'"{r_key.codeName}": {expr}'
            )
        rightKeyFormatted = f"{', '.join(keyContent)}"
        outputString = f"{expr_tree.tableName}[record({{{rightKeyFormatted}}})] != None"
        return outputString
