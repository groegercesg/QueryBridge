import copy

from uplan_helpers import *
from uplan_nodes import *
from sdqlpy_classes import *

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

def surface_all_exprs(expr_tree):
    currentNodes = []

    if isinstance(expr_tree, BinaryExpressionOperator):
        leftNodes = surface_all_exprs(expr_tree.left)
        rightNodes = surface_all_exprs(expr_tree.right)
        
        currentNodes.extend(leftNodes)
        currentNodes.extend(rightNodes)
    elif isinstance(expr_tree, UnaryExpressionOperator):
        childNodes = surface_all_exprs(expr_tree.child)
        
        currentNodes.extend(childNodes)
    else:
        pass
    
    # Add current
    currentNodes.append(type(expr_tree))
    
    return currentNodes

def convert_universal_to_sdqlpy(universal_tree: UniversalBaseNode) -> SDQLpyBaseNode:
        
    nodeIDs = dict()
    def convert_trees(op_tree: UniversalBaseNode) -> SDQLpyBaseNode:
        # Visit Children
        leftNode, rightNode, childNode = None, None, None
        wasAChild = False
        if isinstance(op_tree, BinaryBaseNode):
            leftNode = convert_trees(op_tree.left)
            rightNode = convert_trees(op_tree.right)
        elif isinstance(op_tree, UnaryBaseNode):
            wasAChild = True
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
                    overallFilter = AndOperator()
                    overallFilter.addLeft(op_tree.tableFilters)
                    overallFilter.addRight(op_tree.tableRestrictions)
                    new_op_tree.addFilterContent(overallFilter)
                elif op_tree.tableFilters != []:
                    new_op_tree.addFilterContent(op_tree.tableFilters)
                elif op_tree.tableRestrictions != []:
                    new_op_tree.addFilterContent(op_tree.tableRestrictions)
                # Set Primary/Foreign information from Universal Plan
                new_op_tree.primaryKey = op_tree.primaryKey
                new_op_tree.foreignKeys = op_tree.foreignKeys
                new_op_tree.removeColumnIDs = op_tree.removeColumnIDs
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
                
                # Set Primary/Foreign information from Universal Plan
                new_op_tree.primaryKey = op_tree.primaryKey
                new_op_tree.foreignKeys = op_tree.foreignKeys
                new_op_tree.removeColumnIDs = op_tree.removeColumnIDs
            case OutputNode():
                new_op_tree = None
            case JoinNode():
                new_op_tree = SDQLpyJoinNode(
                    op_tree.joinMethod,
                    op_tree.joinType,
                    op_tree.joinCondition,
                    op_tree.leftKeys,
                    op_tree.rightKeys
                )
                # Set Primary/Foreign information from Universal Plan
                new_op_tree.primaryKey = op_tree.primaryKey
                new_op_tree.foreignKeys = op_tree.foreignKeys
                new_op_tree.removeColumnIDs = op_tree.removeColumnIDs
            case FilterNode():
                new_op_tree = SDQLpyFilterNode()
                new_op_tree.addFilterContent(op_tree.condition)
                # Set Primary/Foreign information from Universal Plan
                new_op_tree.primaryKey = op_tree.primaryKey
                new_op_tree.foreignKeys = op_tree.foreignKeys
                new_op_tree.removeColumnIDs = op_tree.removeColumnIDs
            case NewColumnNode():
                assert childNode.outputDict != None and len(childNode.outputDict.flatCols()) > 0
                allExprTypes = []
                for val in op_tree.values:
                    allExprTypes.extend(surface_all_exprs(val))
                valueTypeCounter = Counter(allExprTypes)
                
                if isinstance(childNode, SDQLpyConcatNode):
                    assert isinstance(childNode.child, SDQLpyGroupNode)
                    # We need to add a Aggr Node
                    newKeyExpressions = []
                    for keyExpr in childNode.child.keyExpressions:
                        newKeyExpressions.append(
                            # Guess at the type, not that useful
                            ColumnValue(keyExpr.codeName, "Varchar")
                        )
                    newGroup = SDQLpyGroupNode(
                        newKeyExpressions,
                        op_tree.values
                    )
                    newGroup.primaryKey = childNode.child.primaryKey
                    newGroup.foreignKeys = childNode.child.foreignKeys
                    newGroup.addChild(childNode.child)
                    new_op_tree = SDQLpyConcatNode(
                        newGroup.keyExpressions + newGroup.aggregateOperations
                    )
                    # Set Primary/Foreign information from Universal Plan
                    new_op_tree.primaryKey = op_tree.primaryKey
                    new_op_tree.foreignKeys = op_tree.foreignKeys
                    new_op_tree.removeColumnIDs = op_tree.removeColumnIDs
                    new_op_tree.addChild(newGroup)
                    childNode = None
                elif isinstance(childNode, SDQLpyAggrNode):
                    new_op_tree = SDQLpyAggrNode(
                        op_tree.values
                    )
                    # Set Primary/Foreign information from Universal Plan
                    new_op_tree.primaryKey = op_tree.primaryKey
                    new_op_tree.foreignKeys = op_tree.foreignKeys
                    new_op_tree.removeColumnIDs = op_tree.removeColumnIDs
                elif isinstance(childNode, SDQLpyGroupNode) and valueTypeCounter[SumAggrOperator] > 1:
                    # We need to add a Group Node
                    newKeyExpressions = []
                    for keyExpr in childNode.keyExpressions:
                        newKeyExpressions.append(
                            # Guess at the type, not that useful
                            ColumnValue(keyExpr.codeName, "Varchar")
                        )
                    new_op_tree = SDQLpyGroupNode(
                        newKeyExpressions,
                        op_tree.values
                    )
                    new_op_tree.primaryKey = childNode.primaryKey
                    new_op_tree.foreignKeys = childNode.foreignKeys
                    new_op_tree.removeColumnIDs = op_tree.removeColumnIDs
                    new_op_tree.addChild(childNode)
                    childNode = None
                else:
                    new_op_tree = None
                    childNode.outputDict.keys.extend(op_tree.values)
                    childNode.removeColumnIDs = set(childNode.removeColumnIDs).union(op_tree.removeColumnIDs)
            case RetrieveNode():
                # Build the use the nodeDict
                assert op_tree.retrieveTargetID in nodeIDs
                new_op_tree = nodeIDs[op_tree.retrieveTargetID]
                # new_op_tree = SDQLpyRetrieveNode(
                #     op_tree.tableColumns,
                #     op_tree.retrieveTargetID
                # )
                # # Set Primary/Foreign information from Universal Plan
                # new_op_tree.primaryKey = op_tree.primaryKey
                # new_op_tree.foreignKeys = op_tree.foreignKeys
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
        elif childNode == None and wasAChild == True:
            # Don't add the childNode if we've set it to None
            pass
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
                case _:
                    # LeafSDQLpyNode
                    assert isinstance(lowest_node_pointer, LeafSDQLpyNode)
                    
        # Create the output_dict
        wire_up_incoming_output_dicts(new_op_tree, True)
        
        # Add nodeID to new_op_node
        assert hasattr(op_tree, "nodeID")
        if new_op_tree != None:
            if new_op_tree.nodeID == None:
                new_op_tree.addID(op_tree.nodeID)
            else:
                # We've already assigned one, don't overwrite it
                pass
            # Add to the Nodedict
            if new_op_tree.nodeID != None:
                nodeIDs[new_op_tree.nodeID] = new_op_tree
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
                # Save topNode ids
                sdqlpy_tree.topNodeIds = [id(col) for col in sdqlpy_tree.outputDict.flatCols()]
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
            assert isinstance(sdqlpy_tree.right, (SDQLpyRecordNode, SDQLpyJoinNode, SDQLpyFilterNode, SDQLpyRetrieveNode, SDQLpyConcatNode, SDQLpyGroupNode, SDQLpyPromoteToFloatNode))
            
            # BNL
            if sdqlpy_tree.joinMethod == "bnl":
                pass
            # Turn a Record or JoinNode on the left into a JoinBuildNode
            elif isinstance(sdqlpy_tree.left, (SDQLpyRecordNode, SDQLpyJoinNode, SDQLpyFilterNode)):
                # Make it a SDQLpyJoinBuildNode
                leftKeyIDs = [str(id(x)) for x in [y.left for y in sdqlpy_tree.equatingConditions]]
                leftColumnIDs = [str(id(x)) for x in sdqlpy_tree.left.outputDict.flatCols()]
                assert len(set(leftKeyIDs) - set(leftColumnIDs)) == 0
                
                # Only set tableKeys as only the leftKeys that are in equatingConditions
                tableKeys = []
                for x in sdqlpy_tree.equatingConditions:
                    if id(x.left) in [id(x) for x in sdqlpy_tree.leftKeys]:
                        tableKeys.append(x.left)
                    elif id(x.right) in [id(x) for x in sdqlpy_tree.leftKeys]:
                        tableKeys.append(x.right)
                    else:
                        raise Exception("Not found")
                    
                assert tableKeys != []
                
                jbNode = SDQLpyJoinBuildNode(
                    tableKeys,
                    list(sdqlpy_tree.left.outputDict.flatCols())
                )
                jbNode.addChild(sdqlpy_tree.left)
                jbNode.setCardinality(jbNode.child.cardinality)
                # Set primary and foreign
                jbNode.primaryKey = tuple(tableKeys)
                jbNode.foreignKeys = sdqlpy_tree.left.foreignKeys
                jbNode.removeColumnIDs = sdqlpy_tree.left.removeColumnIDs
                sdqlpy_tree.left = jbNode
            elif isinstance(sdqlpy_tree.left, (SDQLpyAggrNode, SDQLpyPromoteToFloatNode)):
                # We shouldn't index on an aggr/or a promoteToFloat node
                assert len(sdqlpy_tree.left.outputDict.flatCols()) == 1
                pass
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
                elif isinstance(val, CountAggrOperator):
                    if str(id(val)) not in replacementDict:
                        # Create a new one in the replacementDict
                        if val.codeName == "":
                            handleEmptyCodeName(val, parserCreatedColumns)
                        # What is the Aggregations are nested
                        desiredChild = val.child
                        while isinstance(desiredChild, (CountAggrOperator)):
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
                # elif isinstance(val, (MaxAggrOperator, AvgAggrOperator, MinAggrOperator)):
                #     raise Exception("Max/Min/Avg operator detected, we don't have support for these")
        
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
        
    def solveSDQLpySpecificOrderJoins(sdqlpy_tree):
        # Post Order traversal: Visit Children
        leftNode, rightNode, childNode = None, None, None
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = solveSDQLpySpecificOrderJoins(sdqlpy_tree.left)
            rightNode = solveSDQLpySpecificOrderJoins(sdqlpy_tree.right)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = solveSDQLpySpecificOrderJoins(sdqlpy_tree.child)
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
            # Decompose
            sdqlpy_tree.decompose_join_condition()
            
            # Swap if it's a left semijoin
            if sdqlpy_tree.joinType == "leftsemijoin":
                sdqlpy_tree.joinType = "rightsemijoin"
                sdqlpy_tree.swapLeftAndRight()
            elif sdqlpy_tree.joinType == "leftantijoin":
                sdqlpy_tree.joinType = "rightantijoin"
                sdqlpy_tree.swapLeftAndRight()
            elif sdqlpy_tree.joinType == "outer":
                # Outer means left-outer
                # Make whatevers in left in Right, as we need all records for that
                sdqlpy_tree.swapLeftAndRight()
                # And make the left a Vector
                assert isinstance(sdqlpy_tree.left, SDQLpyRecordNode)
                sdqlpy_tree.left.vectorValue = True
                pass
            else:
                # Otherwise, it's grand - so just leave it
                pass
                
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
            if hasattr(sdqlpy_tree, "postJoinFilters") and sdqlpy_tree.postJoinFilters != None:
                # Make a filterNode
                newFilter = SDQLpyFilterNode()
                newFilter.addFilterContent(sdqlpy_tree.postJoinFilters)
                sdqlpy_tree.postJoinFilters = None
                newFilter.setCardinality(sdqlpy_tree.cardinality)
                newFilter.addChild(sdqlpy_tree)
                newFilter.set_output_dict()
                sdqlpy_tree = newFilter
                
        return sdqlpy_tree
    
    def solveCountAllOperator(sdqlpy_tree):
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            solveCountAllOperator(sdqlpy_tree.left)
            solveCountAllOperator(sdqlpy_tree.right)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            solveCountAllOperator(sdqlpy_tree.child)
        else:
            # A leaf node
            pass
        
        # Run on current node (sdqlpy_tree)
        for col in sdqlpy_tree.outputDict.flatCols():
            if isinstance(col, CountAllOperator) and col.codeName == "":
                incomingColumnStrings = sorted([x.codeName for x in sdqlpy_tree.incomingDict.flatCols()])
                # Choose the first one
                col.setCodeName(f"{incomingColumnStrings[0]}_count")
    
    def solveOutputDictEmptyCodename(sdqlpy_tree, parserCreatedColumns):
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            solveOutputDictEmptyCodename(sdqlpy_tree.left, parserCreatedColumns)
            solveOutputDictEmptyCodename(sdqlpy_tree.right, parserCreatedColumns)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            solveOutputDictEmptyCodename(sdqlpy_tree.child, parserCreatedColumns)
        else:
            # A leaf node
            pass
        
        # Run on current node (sdqlpy_tree)
        for col in sdqlpy_tree.outputDict.flatCols():
            if col.codeName == "":
                handleEmptyCodeName(col, parserCreatedColumns)
                assert col.codeName != ""
                parserCreatedColumns.add(col.codeName)
    
    def solveDuplicateColumnsNames(sdqlpy_tree):
        def getProblemCodeNamesFromJoinKeys(leftKeys: list, rightKeys: list) -> list:
            # Use left/rightKeys to determine this
            # Check that the codeNames are unique
            nameDict = defaultdict(int)
            for col in leftKeys:
                nameDict[col.codeName] += 1
            for col in rightKeys:
                nameDict[col.codeName] += 1
                
            problemCodeNames = [k for k, v in nameDict.items() if v > 1]
            return problemCodeNames, nameDict
        
        def getProblemCodeNamesFromSRDict(sr_dict: SDQLpySRDict) -> list:
            # Check that the codeNames are unique
            nameDict = defaultdict(int)
            for col in sr_dict.flatKeys():
                nameDict[col.codeName] += 1
                
            problemCodeNames = [k for k, v in nameDict.items() if v > 1]
            return problemCodeNames, nameDict
        
        # Post Order traversal: Visit Children
        leftNode, rightNode, childNode = None, None, None
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = solveDuplicateColumnsNames(sdqlpy_tree.left)
            rightNode = solveDuplicateColumnsNames(sdqlpy_tree.right)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = solveDuplicateColumnsNames(sdqlpy_tree.child)
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
            assert sdqlpy_tree.outputDict != None and len(sdqlpy_tree.outputDict.flatCols()) > 0
              
            # Any values are > 1, we have a problem and need to resolve duplicates
            problemCodeNames, problemCodeNameDict = getProblemCodeNamesFromSRDict(sdqlpy_tree.outputDict)
            if len(problemCodeNames) > 0:
                for badCodeName in problemCodeNames:
                    filtered_SRDictKeys = [x for x in sdqlpy_tree.outputDict.flatKeys() if x.codeName == badCodeName]
                    assert len(filtered_SRDictKeys) == problemCodeNameDict[badCodeName] and problemCodeNameDict[badCodeName] == 2
                    
                    # Change the codenames, for those in filtered
                    nameAffixes = ["_x", "_y"]
                    for idx, affix in enumerate(nameAffixes):
                        filtered_SRDictKeys[idx].codeName = f"{filtered_SRDictKeys[idx].codeName}{affix}"
                
                # Check this has resolved things
                problemCodeNames, _ = getProblemCodeNamesFromSRDict(sdqlpy_tree.outputDict)
                assert len(problemCodeNames) == 0
            
            # Check joinCondition for duplicates
            problemCodeNames, problemCodeNameDict = getProblemCodeNamesFromJoinKeys(sdqlpy_tree.leftKeys, sdqlpy_tree.rightKeys)
            if len(problemCodeNames) > 0:
                for badCodeName in problemCodeNames:
                    filtered_SRDictKeys = [x for x in (sdqlpy_tree.leftKeys + sdqlpy_tree.rightKeys) if x.codeName == badCodeName]
                    assert len(filtered_SRDictKeys) == problemCodeNameDict[badCodeName] and problemCodeNameDict[badCodeName] == 2
                    
                    # Change the codenames, for those in filtered
                    nameAffixes = ["_x", "_y"]
                    for idx, affix in enumerate(nameAffixes):
                        filtered_SRDictKeys[idx].codeName = f"{filtered_SRDictKeys[idx].codeName}{affix}"
                
                # Check this has resolved things
                problemCodeNames, _ = getProblemCodeNamesFromJoinKeys(sdqlpy_tree.leftKeys, sdqlpy_tree.rightKeys)
                assert len(problemCodeNames) == 0
            
        return sdqlpy_tree
    
    def solveRepeatedAggrs(sdqlpy_tree):
        # Post Order traversal: Visit Children
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            solveRepeatedAggrs(sdqlpy_tree.left)
            solveRepeatedAggrs(sdqlpy_tree.right)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            solveRepeatedAggrs(sdqlpy_tree.child)
        else:
            # A leaf node
            pass
        
        if isinstance(sdqlpy_tree, UnarySDQLpyNode) and isinstance(sdqlpy_tree.child, UnarySDQLpyNode):
            if isinstance(sdqlpy_tree, SDQLpyAggrNode) and isinstance(sdqlpy_tree.child, SDQLpyAggrNode):
                # We have detected repeated aggrs
                sdqlpy_tree.set_repeated_aggr()
            
    def solveNonPrimaryFilters(sdqlpy_tree):
        # Post Order traversal: Visit Children
        belowFilter = None
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftFilter = solveNonPrimaryFilters(sdqlpy_tree.left)
            rightFilter = solveNonPrimaryFilters(sdqlpy_tree.right)
            
            # Handle left and right Filter
            if (leftFilter != None) and (rightFilter != None):
                # Combine with AND, then use
                belowFilter = AndOperator()
                belowFilter.addLeft(leftFilter)
                belowFilter.addRight(rightFilter)
            elif (leftFilter != None) and (rightFilter == None):
                belowFilter = leftFilter
            elif (leftFilter == None) and (rightFilter != None):
                belowFilter = rightFilter
            else:
                assert (leftFilter == None) and (rightFilter == None)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childFilter = solveNonPrimaryFilters(sdqlpy_tree.child)
            # Add anyway, it's either something useful or NULL
            belowFilter = childFilter
        else:
            # A leaf node
            pass
        
        assert sdqlpy_tree.primaryKey != None
        
        # Determine if the current node is a suitable filter location
        # Get all codeNames for keys
        all_codeNames = [x.codeName for x in sdqlpy_tree.outputDict.flatKeys()]
        primaryKey_codeNames = [x.codeName for x in sdqlpy_tree.primaryKey]
        if (not set(primaryKey_codeNames).issubset(set(all_codeNames))) and (not isinstance(sdqlpy_tree, (SDQLpyAggrNode, SDQLpyJoinNode))):
            # IF all items of A are NOT present in B
            # This is a cause for concern - as a filter shouldn't be applied in this scenario
            if sdqlpy_tree.filterContent != None:
                # We have a filter we need to move
                if belowFilter == None:
                    # Simply add it
                    belowFilter = sdqlpy_tree.filterContent
                else:
                    oldBelowFilter = belowFilter
                    belowFilter = AndOperator()
                    belowFilter.addLeft(oldBelowFilter)
                    belowFilter.addRight(sdqlpy_tree.filterContent)
                # Reset current node's filter
                sdqlpy_tree.filterContent = None
        else:
            # We have a node where we can place a belowFilter, if there is one
            if belowFilter == None:
                # No eligible belowFilter, all good
                pass
            else:
                if isinstance(sdqlpy_tree, SDQLpyJoinNode):
                    # Add for a JoinNode, add to comparing tree
                    if sdqlpy_tree.comparingTree == None:
                        # No comparingTree, all good - just add it
                        sdqlpy_tree.comparingTree = belowFilter
                    else:
                        # We need to construct an AndOperator
                        oldComparingTree = sdqlpy_tree.comparingTree
                        newComparingTree = AndOperator()
                        newComparingTree.addLeft(oldComparingTree)
                        newComparingTree.addRight(belowFilter)
                        sdqlpy_tree.comparingTree = newComparingTree
                else:
                    # We have a belowFilter to add to the current node, does it have a filter already
                    if sdqlpy_tree.filterContent == None:
                        # No filter at the moment, all good - just add it
                        sdqlpy_tree.filterContent = belowFilter
                    else:
                        # We need to construct an AndOperator
                        oldFilterContent = sdqlpy_tree.filterContent
                        newFilterContent = AndOperator()
                        newFilterContent.addLeft(oldFilterContent)
                        newFilterContent.addRight(belowFilter)
                        sdqlpy_tree.filterContent = newFilterContent
                # Reset belowFilter, as it's been consumed
                belowFilter = None
                
        return belowFilter
    
    def solveCountDistinctOperator(sdqlpy_tree): 
        replacementDict = dict()
               
        # Post Order traversal: Visit Children
        leftNode, rightNode, childNode = None, None, None
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode, leftReplaces = solveCountDistinctOperator(sdqlpy_tree.left)
            rightNode, rightReplaces = solveCountDistinctOperator(sdqlpy_tree.right)
            replacementDict.update(**leftReplaces, **rightReplaces)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode, childReplaces = solveCountDistinctOperator(sdqlpy_tree.child)
            replacementDict.update(childReplaces)
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
        for valuesLocation in [sdqlpy_tree.outputDict.keys, sdqlpy_tree.outputDict.values]:
            for idx, val in enumerate(valuesLocation):
                if isinstance(val, (CountDistinctAggrOperator)):
                    # If we have a countDistinctAggrOperator,
                    # We need to set somethings to support this
                    assert isinstance(sdqlpy_tree, (SDQLpyGroupNode, SDQLpyConcatNode))
                    
                    if isinstance(sdqlpy_tree, SDQLpyGroupNode):
                        sdqlpy_tree.output_dict_value_sr_dict = True
                    elif isinstance(sdqlpy_tree, SDQLpyConcatNode):
                        sdqlpy_tree.output_dict_value_dict_size = True
                    else:
                        raise Exception("Unexpected node")
                    
                    # Turn off duplicateUser and counter
                    sdqlpy_tree.outputDict.duplicateUser = False
                    sdqlpy_tree.child.outputDict.duplicateCounter = False
                    
                    sdqlpy_tree.set_output_dict()
                
        return sdqlpy_tree, replacementDict
    
    def solveRecordNodeImportantTableColumns(sdqlpy_tree):
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            solveRecordNodeImportantTableColumns(sdqlpy_tree.left)
            solveRecordNodeImportantTableColumns(sdqlpy_tree.right)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            solveRecordNodeImportantTableColumns(sdqlpy_tree.child)
        else:
            # A leaf node
            pass
        
        if isinstance(sdqlpy_tree, SDQLpyRecordNode):
            sdqlpy_tree.filterTableColumns()
            
    def solveConcatNodeForTopGroupNode(sdqlpy_tree):
        if isinstance(sdqlpy_tree, SDQLpyGroupNode):
            concat_node = SDQLpyConcatNode()
            concat_node.primaryKey = sdqlpy_tree.primaryKey
            concat_node.foreignKeys = sdqlpy_tree.foreignKeys
            concat_node.addChild(sdqlpy_tree)
            sdqlpy_tree = concat_node
            
        return sdqlpy_tree
    
    # def solveRetrieveNode(sdqlpy_tree):
    #     # Traverse to gather nodeIDs
    #     nodeIDs = dict()
        
    #     def traverse_to_set_node_ids(sdqlpy_tree):
    #         if isinstance(sdqlpy_tree, BinarySDQLpyNode):
    #             traverse_to_set_node_ids(sdqlpy_tree.left)
    #             traverse_to_set_node_ids(sdqlpy_tree.right)
    #         elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
    #             traverse_to_set_node_ids(sdqlpy_tree.child)
    #         else:
    #             # A leaf node
    #             pass
            
    #         if sdqlpy_tree.nodeID not in nodeIDs:
    #             nodeIDs[sdqlpy_tree.nodeID] = sdqlpy_tree
    #         elif sdqlpy_tree.nodeID == None:
    #             # NodeID is none, this is something we've created then - and is not going to be used by a retrieveNode
    #             pass
    #         else:
    #             raise Exception()
        
    #     def configureRetrieveNodes(sdqlpy_tree):
    #         if isinstance(sdqlpy_tree, BinarySDQLpyNode):
    #             configureRetrieveNodes(sdqlpy_tree.left)
    #             configureRetrieveNodes(sdqlpy_tree.right)
    #         elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
    #             configureRetrieveNodes(sdqlpy_tree.child)
    #         else:
    #             # A leaf node
    #             pass
            
    #         if isinstance(sdqlpy_tree, SDQLpyRetrieveNode):
    #             assert sdqlpy_tree.targetID in nodeIDs
    #             retrievedNode = nodeIDs[sdqlpy_tree.targetID]
                
    #             nodeTableColumnsCounter = Counter([type(x) for x in sdqlpy_tree.outputDict.flatCols()])
    #             retrievedNodeColumnsCounter = Counter([type(x) for x in retrievedNode.outputDict.flatCols()])
                
    #             assert len(sdqlpy_tree.outputDict.flatCols()) == len(retrievedNode.outputDict.flatCols())
    #             assert nodeTableColumnsCounter == retrievedNodeColumnsCounter
    #             assert len(set(nodeTableColumnsCounter.values())) <= 1, "All should have the same value"
    #             assert all(1 == x for x in nodeTableColumnsCounter.values()), "All should be 1"
    #             # All should have same names and types
    #             for idx, val in enumerate(sdqlpy_tree.outputDict.flatCols()):
    #                 retrievedDictItem = list(retrievedNode.outputDict.flatCols())[idx]
    #                 assert type(val) == type(retrievedDictItem)
    #                 if val.codeName != retrievedDictItem.codeName:
    #                     retrievedDictItem.codeName = val.codeName
    #                 assert val.codeName == retrievedDictItem.codeName
        
    #     # Build a dictionary of Node IDs
    #     traverse_to_set_node_ids(sdqlpy_tree)
    #     # Configure each RetrieveNode to be correct
    #     configureRetrieveNodes(sdqlpy_tree)
    
    def solveComplexAggrHandling(sdqlpy_tree):
        def is_complex_aggr_in_node(sdqlpy_node) -> bool:
            complex_aggrs = [AvgAggrOperator, MinAggrOperator, MaxAggrOperator]
            numberComplex = list(filter(lambda x: type(x) in complex_aggrs, sdqlpy_node.outputDict.flatCols()))
            return len(numberComplex) > 0
            
        # Post Order traversal: Visit Children
        leftNode, rightNode, childNode = None, None, None
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = solveComplexAggrHandling(sdqlpy_tree.left)
            rightNode = solveComplexAggrHandling(sdqlpy_tree.right)
            
            sdqlpy_tree.left = leftNode
            sdqlpy_tree.right = rightNode
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = solveComplexAggrHandling(sdqlpy_tree.child)
            
            sdqlpy_tree.child = childNode
        else:
            # A leaf node
            pass
        
        # A AVG/MIN/MAX should only happen in a Group/Or be used, once created
        if isinstance(sdqlpy_tree, UnarySDQLpyNode) and isinstance(sdqlpy_tree.child, SDQLpyGroupNode) and is_complex_aggr_in_node(sdqlpy_tree.child):
            # A complex Aggr has been detected,
            if isinstance(sdqlpy_tree, SDQLpyConcatNode):
                sdqlpy_tree.promote_to_float = True
            else:
                raise Exception(f"A groupNode with complex aggrs (avg/max/min) has an unrecognised parent: {sdqlpy_tree}")
        elif isinstance(sdqlpy_tree, BinarySDQLpyNode) and isinstance(sdqlpy_tree.left, SDQLpyGroupNode) and is_complex_aggr_in_node(sdqlpy_tree.left):
            # We need to insert a SDQLpyPromoteToFloat node above the Group on the LHS
            pass
        elif isinstance(sdqlpy_tree, BinarySDQLpyNode) and isinstance(sdqlpy_tree.right, SDQLpyGroupNode) and is_complex_aggr_in_node(sdqlpy_tree.right):
            # We need to insert a SDQLpyPromoteToFloat node above the Group on the LHS
            promote = SDQLpyPromoteToFloatNode()
            promote.addChild(sdqlpy_tree.right)
            promote.primaryKey = sdqlpy_tree.right.primaryKey
            promote.foreignKeys = sdqlpy_tree.right.foreignKeys
            promote.removeColumnIDs = sdqlpy_tree.right.removeColumnIDs
            promote.set_output_dict()
            sdqlpy_tree.right = promote
        elif isinstance(sdqlpy_tree, BinarySDQLpyNode) and isinstance(sdqlpy_tree.left, SDQLpyAggrNode) and is_complex_aggr_in_node(sdqlpy_tree.left):
            # SDQLpyPromoteToFloat for an AggrNode
            promote = SDQLpyPromoteToFloatNode()
            promote.addChild(sdqlpy_tree.left)
            promote.primaryKey = sdqlpy_tree.left.primaryKey
            promote.foreignKeys = sdqlpy_tree.left.foreignKeys
            promote.removeColumnIDs = sdqlpy_tree.left.removeColumnIDs
            promote.set_output_dict()
            sdqlpy_tree.left = promote
        elif isinstance(sdqlpy_tree, BinarySDQLpyNode) and isinstance(sdqlpy_tree.right, SDQLpyAggrNode) and is_complex_aggr_in_node(sdqlpy_tree.right):
            # SDQLpyPromoteToFloat for an AggrNode
            pass
        
        return sdqlpy_tree
    
    def solveRemoveColumnIDs(sdqlpy_tree):
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = solveRemoveColumnIDs(sdqlpy_tree.left)
            rightNode = solveRemoveColumnIDs(sdqlpy_tree.right)
            
            sdqlpy_tree.left = leftNode
            sdqlpy_tree.right = rightNode
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = solveRemoveColumnIDs(sdqlpy_tree.child)
            
            sdqlpy_tree.child = childNode
        else:
            # A leaf node
            pass
        
        # Current node
        if len(sdqlpy_tree.removeColumnIDs) > 0:
            # We have had an optimisation applied
            # Delete from SDQLpySRDict
            sdqlpy_tree.outputDict.deleteFromSROnIDs(sdqlpy_tree.removeColumnIDs)
            # Check Primary still in SRDict
            outputDictIDs = [id(x) for x in sdqlpy_tree.outputDict.flatCols()]
            assert all([id(x) in outputDictIDs for x in sdqlpy_tree.primaryKey]) or isinstance(sdqlpy_tree, SDQLpyAggrNode)
            # Check no removeIds in outputDict
            assert all([not (x in outputDictIDs) for x in sdqlpy_tree.removeColumnIDs])
        
        return sdqlpy_tree
    
    def solveEqNeqJoins(sdqlpy_tree):
        def gatherJoinCondTypes(join_cond):
            current_types = []
            if isinstance(join_cond, BinaryExpressionOperator):
                leftTypes = gatherJoinCondTypes(join_cond.left)
                rightTypes = gatherJoinCondTypes(join_cond.right)

                current_types.extend(leftTypes)
                current_types.extend(rightTypes)
            elif isinstance(join_cond, UnaryExpressionOperator):
                childTypes = gatherJoinCondTypes(join_cond.child)
                
                current_types.extend(childTypes)
            else:
                pass
            
            current_types.append(join_cond)
            return current_types
            
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = solveEqNeqJoins(sdqlpy_tree.left)
            rightNode = solveEqNeqJoins(sdqlpy_tree.right)
            
            sdqlpy_tree.left = leftNode
            sdqlpy_tree.right = rightNode
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = solveEqNeqJoins(sdqlpy_tree.child)
            
            sdqlpy_tree.child = childNode
        else:
            # A leaf node
            pass
        
        if isinstance(sdqlpy_tree, SDQLpyJoinNode):
            joinTypes = []
            for x in sdqlpy_tree.joinCondition:
                joinTypes.extend(gatherJoinCondTypes(x))
            joinTypeCounter = Counter([type(x) for x in joinTypes])
            
            if (joinTypeCounter[EqualsOperator] >= 1) and (joinTypeCounter[NotEqualsOperator] >= 1):
                assert joinTypeCounter[EqualsOperator] == 1
                equalCond = list(filter(lambda x: type(x) == EqualsOperator, joinTypes))
                assert len(equalCond) == 1
                equalCond = equalCond[0]
                
                # Make the inner join
                
                innerJoin = SDQLpyJoinNode(sdqlpy_tree.joinMethod, "inner", sdqlpy_tree.joinCondition[0],
                                           sdqlpy_tree.rightKeys, sdqlpy_tree.leftKeys)
                
                # Left should be a record (that is a vector)
                leftVectored = SDQLpyRecordNode(sdqlpy_tree.right.tableName, sdqlpy_tree.rightKeys)
                leftVectored.addFilterContent(sdqlpy_tree.right.filterContent)
                leftVectored.vectorValue = True
                leftVectored.setCardinality(-1)
                leftVectored.primaryKey = (equalCond.left, )
                leftVectored.incomingDict = sdqlpy_tree.right.outputDict
                
                innerJoin.addLeft(leftVectored)
                innerJoin.primaryKey = (equalCond.left, )
                
                # Right should be a probe over a Retrieve for the original join
                if sdqlpy_tree.left.nodeID == None:
                    # Create a new NodeID, that is likely to be unique
                    sdqlpy_tree.left.nodeID = random.randint(50, 100) 
                rightRetrieve = SDQLpyRetrieveNode(sdqlpy_tree.outputDict.flatCols(), sdqlpy_tree.left.nodeID)
                rightRetrieve.outputDict = sdqlpy_tree.outputDict
                rightRetrieve.primaryKey = sdqlpy_tree.primaryKey
                innerJoin.addRight(rightRetrieve)
                
                innerJoin.set_output_dict()
                innerJoin.primaryKey = innerJoin.left.primaryKey
                innerJoin.setCardinality(-1)
                
                # Overall Join
                if "left" in sdqlpy_tree.joinType:
                    newJoinType = str(sdqlpy_tree.joinType).replace("left", "right")
                else:
                    raise Exception(f"Expected it to be 'left...' but instead was: {sdqlpy_tree.joinType}")
                
                overallJoin = SDQLpyJoinNode(sdqlpy_tree.joinMethod, newJoinType, equalCond,
                                             [equalCond.right], [equalCond.left])
                overallJoin.addLeft(innerJoin)
                overallJoin.addRight(sdqlpy_tree.left)
                
                overallJoin.set_output_dict()
                overallJoin.primaryKey = sdqlpy_tree.primaryKey
                overallJoin.foreignKeys = sdqlpy_tree.foreignKeys
                overallJoin.removeColumnIDs = sdqlpy_tree.removeColumnIDs
                overallJoin.setCardinality(sdqlpy_tree.cardinality)
                
                sdqlpy_tree = overallJoin
        
        return sdqlpy_tree
    
    # Set the code names
    set_codeNames(universal_tree)
    output_cols_order = universal_tree.outputNames
    # Convert BNL to Inner with condition
    # convert_join_bnl_to_inner(universal_tree)
    # Call convert trees
    sdqlpy_tree = convert_trees(universal_tree)
    # Add concat if top node is a GroupNode
    sdqlpy_tree = solveConcatNodeForTopGroupNode(sdqlpy_tree)
    # Wire up incoming/output Dicts
    wire_up_incoming_output_dicts(sdqlpy_tree, no_sumaggr_warn=True)
    # Convert SumAggrOperator to ColumnNode
    parserCreatedColumns, _ = convertSumAggrOperatorToColumnNode(sdqlpy_tree)
    # Add PromoteToFloat handing for AVG/MIN/MAX
    sdqlpy_tree = solveComplexAggrHandling(sdqlpy_tree)
    # Solve empty outputDict codenames
    solveOutputDictEmptyCodename(sdqlpy_tree, parserCreatedColumns)
    # Solve RecordNodes so that they're more efficient
    solveRecordNodeImportantTableColumns(sdqlpy_tree)
    # Solve Equal/Neq Joins
    sdqlpy_tree = solveEqNeqJoins(sdqlpy_tree)
    # Order joins, in the specific SDQLpy Manner
    sdqlpy_tree = solveSDQLpySpecificOrderJoins(sdqlpy_tree)
    # Insert JoinProbe Nodes
    sdqlpy_tree = insert_join_probe_nodes(sdqlpy_tree)
    # move postJoinFilterNodes into a separate FilterNode
    sdqlpy_tree = insertPostJoinFilterNodes(sdqlpy_tree)
    # Wire up incoming/output Dicts
    wire_up_incoming_output_dicts(sdqlpy_tree)
    # solve Duplicates by multiplying by a counter
    duplicateFixTopGroupJoin(sdqlpy_tree)
    # solve CountDistinctOperator
    sdqlpy_tree, _ = solveCountDistinctOperator(sdqlpy_tree)
    # solve CountAllOperator
    solveCountAllOperator(sdqlpy_tree)
    # Solve empty outputDict codenames
    solveOutputDictEmptyCodename(sdqlpy_tree, parserCreatedColumns)
    # Solve aggr after an aggr
    solveRepeatedAggrs(sdqlpy_tree)
    # Solve non-primary filters
    leftOverFilter = solveNonPrimaryFilters(sdqlpy_tree)
    assert leftOverFilter == None
    # Wire up incoming/output Dicts
    wire_up_incoming_output_dicts(sdqlpy_tree)
    # # Solve RetrieveNode
    # solveRetrieveNode(sdqlpy_tree)
    # Use removeColumnIDs information
    sdqlpy_tree = solveRemoveColumnIDs(sdqlpy_tree)
    
    # Order the topNode correctly
    orderTopNode(sdqlpy_tree, output_cols_order)
    
    return sdqlpy_tree
