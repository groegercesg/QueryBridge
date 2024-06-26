from enum import Enum

from sdqlpy_classes import *
from sdqlpy_helpers import *

class SupportedOptimisations(Enum):
    PIPE_BREAK = "PipelineBreaker"
    V_FOLD = "VerticalFolding"
    UPDATE_SUM = "UpdateSum"
    DENSE = "Dense"
    
    @staticmethod
    def has_value(item):
        return item in [v.value for v in SupportedOptimisations.__members__.values()]

OPTIMISATIONS_ORDER = [
    SupportedOptimisations.PIPE_BREAK,
    SupportedOptimisations.V_FOLD,
    SupportedOptimisations.UPDATE_SUM,
    SupportedOptimisations.DENSE
]

def optimisation_runner(sdqlpy_tree, inOpt, o3_value):
    match inOpt:
        case SupportedOptimisations.PIPE_BREAK.value:
            sdqlpy_tree = opt_pipe_break(sdqlpy_tree)
        case SupportedOptimisations.V_FOLD.value:
            sdqlpy_tree = opt_v_fold(sdqlpy_tree)
        case SupportedOptimisations.UPDATE_SUM.value:
            sdqlpy_tree = opt_update_sum(sdqlpy_tree)
        case SupportedOptimisations.DENSE.value:
            sdqlpy_tree = opt_dense(sdqlpy_tree, o3_value)
        case _:
            raise Exception(f"The optimisation: {inOpt} was not recognised.")
        
    return sdqlpy_tree

def sdqlpy_tree_preprocess(sdqlpy_tree):
    # Reset columnRemoveIDs
    #   As the removal has already happened
    if isinstance(sdqlpy_tree, BinarySDQLpyNode):
        leftNode = sdqlpy_tree_preprocess(sdqlpy_tree.left)
        rightNode = sdqlpy_tree_preprocess(sdqlpy_tree.right)
    
        sdqlpy_tree.left = leftNode
        sdqlpy_tree.right = rightNode
    elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
        childNode = sdqlpy_tree_preprocess(sdqlpy_tree.child)
        
        sdqlpy_tree.child = childNode
    else:
        pass
    
    sdqlpy_tree.removeColumnIDs = set()

    return sdqlpy_tree

def sdqlpy_apply_optimisations(sdqlpy_tree, inOptimisations, sdqlpy_o3_value):
    if inOptimisations == [""]:
        return sdqlpy_tree
    
    # Check optimisations are all okay
    assert isinstance(inOptimisations, list)
    assert all([SupportedOptimisations.has_value(x) for x in inOptimisations])
    assert len(inOptimisations) <= len(OPTIMISATIONS_ORDER)
    
    # We should sort the optimisations
    order = {v.value:i for i,v in enumerate(OPTIMISATIONS_ORDER)}
    inOptimisations = sorted(inOptimisations, key=lambda x: order[x])
    
    # sdqlpy_tree = sdqlpy_tree_preprocess(sdqlpy_tree)
    
    for opt in inOptimisations:
        sdqlpy_tree = optimisation_runner(sdqlpy_tree, opt, sdqlpy_o3_value)
        
    return sdqlpy_tree

PIPELINE_BREAKERS = [
    SDQLpyGroupNode,
    SDQLpyJoinNode,
    SDQLpyConcatNode,
    SDQLpyAggrNode,
    SDQLpyJoinBuildNode,
    SDQLpyPromoteToFloatNode,
    SDQLpyRetrieveNode
]
        
def opt_pipe_break(sdqlpy_tree):
    """
    Flow filters forward and eliminate nodes that are not "Pipeline Breakers"
    """
    
    def pipe_break_run(sdqlpy_tree):
        def trim_non_breakers(sdqlpy_tree):
            def get_highest_breaker(sdqlpy_tree):
                
                if type(sdqlpy_tree) in PIPELINE_BREAKERS:
                    return sdqlpy_tree
                elif isinstance(sdqlpy_tree, BinarySDQLpyNode):
                    leftBreaker = get_highest_breaker(sdqlpy_tree.left)
                    rightBreaker = get_highest_breaker(sdqlpy_tree.right)
                    
                    assert type(leftBreaker) in PIPELINE_BREAKERS
                    assert type(rightBreaker) in PIPELINE_BREAKERS
                    
                    sdqlpy_tree.left = leftBreaker
                    sdqlpy_tree.right = rightBreaker
                elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
                    childBreaker = get_highest_breaker(sdqlpy_tree.child)
                    
                    assert type(childBreaker) in PIPELINE_BREAKERS
                    assert type(sdqlpy_tree) not in PIPELINE_BREAKERS
                    assert sdqlpy_tree.filterContent == None
                    
                    sdqlpy_tree = childBreaker
                else:
                    assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
                    return sdqlpy_tree
                
                return sdqlpy_tree
            
            if isinstance(sdqlpy_tree, BinarySDQLpyNode):
                leftBreaker = get_highest_breaker(sdqlpy_tree.left)
                rightBreaker = get_highest_breaker(sdqlpy_tree.right)
                
                assert type(leftBreaker) in PIPELINE_BREAKERS
                assert type(rightBreaker) in PIPELINE_BREAKERS
                assert type(sdqlpy_tree) in PIPELINE_BREAKERS
                
                sdqlpy_tree.left = leftBreaker
                sdqlpy_tree.right = rightBreaker
            elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
                childBreaker = get_highest_breaker(sdqlpy_tree.child)
                
                assert type(childBreaker) in PIPELINE_BREAKERS
                assert type(sdqlpy_tree) in PIPELINE_BREAKERS
                
                sdqlpy_tree.child = childBreaker
            else:
                assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
                
            return sdqlpy_tree
        
        def children_are_breakers(sdqlpy_tree):
            # Return true is the "children" are all breakers
            if isinstance(sdqlpy_tree, UnarySDQLpyNode):
                return (type(sdqlpy_tree.child) in PIPELINE_BREAKERS) or (type(sdqlpy_tree.child) == SDQLpyRecordNode)
            elif isinstance(sdqlpy_tree, BinarySDQLpyNode):
                leftDecision = (type(sdqlpy_tree.left) in PIPELINE_BREAKERS)  or (type(sdqlpy_tree.left) == SDQLpyRecordNode)
                rightDecision = (type(sdqlpy_tree.right) in PIPELINE_BREAKERS)  or (type(sdqlpy_tree.right) == SDQLpyRecordNode)
                return leftDecision and rightDecision
            else:
                assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
                # A leaf should be considered a breaker as well
                return True
        
        def add_filter_to_joinNode(sdqlpy_tree, addFilter):
            # Add filter to current, have to be aware of the fact that it might already have a filter
            if addFilter != None:
                if sdqlpy_tree.comparingTree == None:
                    sdqlpy_tree.comparingTree = addFilter
                else:
                    newCompTree = AndOperator()
                    newCompTree.addLeft(sdqlpy_tree.comparingTree)
                    newCompTree.addRight(addFilter)
                    sdqlpy_tree.comparingTree = newCompTree
        
        def add_filter_to_current(sdqlpy_tree, addFilter):
            # Add filter to current, have to be aware of the fact that it might already have a filter
            
            if sdqlpy_tree.filterContent == None and addFilter == None:
                # Don't add anything
                pass
            elif sdqlpy_tree.filterContent != None and addFilter == None:
                # Don't add anything
                pass
            elif sdqlpy_tree.filterContent == None and addFilter != None:
                # Overwrite
                sdqlpy_tree.filterContent = addFilter
            else:
                assert sdqlpy_tree.filterContent != None and addFilter != None
                # Use an AndOperator
                newFilter = AndOperator()
                newFilter.addLeft(sdqlpy_tree.filterContent)
                newFilter.addRight(addFilter)
                sdqlpy_tree.filterContent = newFilter
        
        def gather_filters_below(sdqlpy_tree):
            # We want to return filters from below, iterate until we reach a Pipeline Breaker
            
            if type(sdqlpy_tree) in PIPELINE_BREAKERS:
                # We don't need anything from this node
                return None
            else:
                lowerFilter = None
                if isinstance(sdqlpy_tree, UnarySDQLpyNode):
                    assert hasattr(sdqlpy_tree, "child")
                    lowerFilter = gather_filters_below(sdqlpy_tree.child)
                else:
                    assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
                    assert not isinstance(sdqlpy_tree, BinarySDQLpyNode)
            
            # Get a current filter, from not a pipeline breaker
            assert type(sdqlpy_tree) not in PIPELINE_BREAKERS
            currentFilter = None
            if sdqlpy_tree.filterContent != None:
                currentFilter = sdqlpy_tree.filterContent
                sdqlpy_tree.filterContent = None
            else:
                # No filter to gather
                pass
            
            if (currentFilter == None) and (lowerFilter == None):
                return None
            elif (currentFilter != None) and (lowerFilter == None):
                return currentFilter
            elif (currentFilter == None) and (lowerFilter != None):
                return lowerFilter
            else:
                assert (currentFilter != None) and (lowerFilter != None)
                # Join with AND
                newCombinedFilter = AndOperator()
                newCombinedFilter.addLeft(currentFilter)
                newCombinedFilter.addRight(lowerFilter)
                return newCombinedFilter
        
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = pipe_break_run(sdqlpy_tree.left)
            rightNode = pipe_break_run(sdqlpy_tree.right)
            
            sdqlpy_tree.left = leftNode
            sdqlpy_tree.right = rightNode
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = pipe_break_run(sdqlpy_tree.child)
            
            sdqlpy_tree.child = childNode
        else:
            assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
        
        if type(sdqlpy_tree) in PIPELINE_BREAKERS:
            # Gather filters below
            if isinstance(sdqlpy_tree, BinarySDQLpyNode):
                filter_from_left = gather_filters_below(sdqlpy_tree.left)
                add_filter_to_joinNode(sdqlpy_tree, filter_from_left)
                filter_from_right = gather_filters_below(sdqlpy_tree.right)
                add_filter_to_joinNode(sdqlpy_tree, filter_from_right)
                # Handle removeColumnIDs
                if filter_from_left != None:
                    sdqlpy_tree.removeColumnIDs = set(sdqlpy_tree.left.removeColumnIDs).union(sdqlpy_tree.removeColumnIDs)
                    sdqlpy_tree.left.removeColumnIDs = set()
                if filter_from_right != None:
                    sdqlpy_tree.removeColumnIDs = set(sdqlpy_tree.right.removeColumnIDs).union(sdqlpy_tree.removeColumnIDs)
                    sdqlpy_tree.right.removeColumnIDs = set()
            elif isinstance(sdqlpy_tree, SDQLpyAggrNode) and Counter([type(x) for x in sdqlpy_tree.outputDict.flatVals()])[AvgAggrOperator] >= 1:
                pass
            elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
                filter_from_below = gather_filters_below(sdqlpy_tree.child)
                add_filter_to_current(sdqlpy_tree, filter_from_below)
                # Handle removeColumnIDs
                if filter_from_below != None:
                    sdqlpy_tree.removeColumnIDs = set(sdqlpy_tree.child.removeColumnIDs).union(sdqlpy_tree.removeColumnIDs)
                    sdqlpy_tree.child.removeColumnIDs = set()
            elif isinstance(sdqlpy_tree, SDQLpyRetrieveNode):
                # SDQLpyRetrieveNode is a quasi-pipeline breaker
                pass
            else:
                raise Exception("A Pipeline breaker shouldn't be a LeafNode")
            
            # Trim the children to be the next closest Pipeline Breaker
            # as that's all the tree should consist of
            if children_are_breakers(sdqlpy_tree):
                # The child is a Pipeline Breaker, there's no issue here
                pass
            else:
                # Trim the non-breakers
                sdqlpy_tree = trim_non_breakers(sdqlpy_tree)
                assert children_are_breakers(sdqlpy_tree)
            
        # The returned tree should be composed solely of Pipeline Breakers
        return sdqlpy_tree
    
    def surface_all_nodes(sdqlpy_tree):
        currentNodes = []
    
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNodes = surface_all_nodes(sdqlpy_tree.left)
            rightNodes = surface_all_nodes(sdqlpy_tree.right)
            
            currentNodes.extend(leftNodes)
            currentNodes.extend(rightNodes)
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNodes = surface_all_nodes(sdqlpy_tree.child)
            
            currentNodes.extend(childNodes)
        else:
            assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
        
        # Add current
        currentNodes.append(type(sdqlpy_tree))
        
        return currentNodes
    
    def resolve_incoming_dicts(sdqlpy_tree):
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = resolve_incoming_dicts(sdqlpy_tree.left)
            rightNode = resolve_incoming_dicts(sdqlpy_tree.right)
            
            sdqlpy_tree.left = leftNode
            sdqlpy_tree.right = rightNode
            
            sdqlpy_tree.incomingDicts = [sdqlpy_tree.left.outputDict, sdqlpy_tree.right.outputDict]
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = resolve_incoming_dicts(sdqlpy_tree.child)
            
            sdqlpy_tree.child = childNode
            
            sdqlpy_tree.incomingDict = sdqlpy_tree.child.outputDict
        else:
            assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
            
        return sdqlpy_tree
    
    pipe_break_run(sdqlpy_tree)
    allNodesInTree = surface_all_nodes(sdqlpy_tree)
    nonPipelineBreakers = list(filter(lambda x: (x not in PIPELINE_BREAKERS) and not (x == SDQLpyRecordNode), allNodesInTree))
    assert len(nonPipelineBreakers) == 0
    
    sdqlpy_tree = resolve_incoming_dicts(sdqlpy_tree)
    
    return sdqlpy_tree

def opt_v_fold(sdqlpy_tree):
    """Folding subsequent operations into each other

    SDQLpyFilterNode -> SDQLpyJoinBuildNode
    SDQLpyJoinNode -> SDQLpyGroupNode
    SDQLpyJoinNode -> SDQLpyJoinBuildNode
    SDQLpyJoinNode -> SDQLpyAggrNode
    
    NOT:
    SDQLpyJoinNode -> SDQLpyJoinNode
    
    Notes: 
        - Have to carry over 'output_dict_value_sr_dict' for Query 16
    
    """
    
    def v_fold_run(sdqlpy_tree):
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = v_fold_run(sdqlpy_tree.left)
            rightNode = v_fold_run(sdqlpy_tree.right)
            
            sdqlpy_tree.left = leftNode
            sdqlpy_tree.right = rightNode
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = v_fold_run(sdqlpy_tree.child)
            
            sdqlpy_tree.child = childNode
        else:
            assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
            
        # We can do vertical folding on a Unary Node
        if isinstance(sdqlpy_tree, UnarySDQLpyNode):
            # And we can fold it into either a JoinNode or a Filter Node
            if isinstance(sdqlpy_tree.child, SDQLpyJoinNode) and sdqlpy_tree.child.foldedInto == False:
                casesInOutputDict = list(filter(lambda x: type(x) == CaseOperator, sdqlpy_tree.outputDict.flatCols()))
                if len(casesInOutputDict) >= 1 and isinstance(sdqlpy_tree, (SDQLpyGroupNode, SDQLpyAggrNode)):
                    # We should not do folding if there is a CaseOperator in the outputDict
                    pass
                elif sdqlpy_tree.child.joinType == "outer":
                    # We can't fold an Outer Join
                    pass
                elif isinstance(sdqlpy_tree, (SDQLpyGroupNode, SDQLpyJoinBuildNode, SDQLpyAggrNode)):
                    # Folding Opportunity
                    if isinstance(sdqlpy_tree, SDQLpyGroupNode):
                        # Carry down the output_dict_value condition
                        sdqlpy_tree.child.output_dict_value_sr_dict = sdqlpy_tree.output_dict_value_sr_dict
                    elif isinstance(sdqlpy_tree, SDQLpyJoinBuildNode):
                        # JoinBuild Folding
                        pass
                    elif isinstance(sdqlpy_tree, SDQLpyAggrNode):
                        # Aggr Mode
                        assert sdqlpy_tree.repeated_aggr == False
                    else:
                        # Unrecognised Node
                        raise Exception(f"Unrecognised supposedly foldable Node: {sdqlpy_tree}")
                    
                    assert sdqlpy_tree.filterContent == None
                    
                    # Set the outputDict
                    sdqlpy_tree.child.outputDict = sdqlpy_tree.outputDict
                    # Set the incomingDict
                    if isinstance(sdqlpy_tree.child, BinarySDQLpyNode):
                        sdqlpy_tree.child.incomingDicts = [sdqlpy_tree.child.left.outputDict, sdqlpy_tree.child.right.outputDict]
                    else:
                        raise Exception("Must be a Binary SDQLpy Node")
                    
                    # Carry the primary and foreign
                    sdqlpy_tree.child.primaryKey = sdqlpy_tree.primaryKey
                    sdqlpy_tree.child.foreignKeys = sdqlpy_tree.foreignKeys
                    # Carry the removeColumnIDs
                    sdqlpy_tree.child.removeColumnIDs = set(sdqlpy_tree.child.removeColumnIDs).union(sdqlpy_tree.removeColumnIDs)
                    
                    sdqlpy_tree = sdqlpy_tree.child
                    sdqlpy_tree.foldedInto = True
                elif isinstance(sdqlpy_tree, (SDQLpyJoinNode, SDQLpyConcatNode)):
                    # No Folding
                    pass
                else:
                    raise Exception(f"Unexpected Node Detected: {sdqlpy_tree}")
            elif isinstance(sdqlpy_tree.child, SDQLpyFilterNode) and sdqlpy_tree.child.foldedInto == False:
                if isinstance(sdqlpy_tree, (SDQLpyJoinBuildNode)):
                    # Folding Opportunity
                    assert sdqlpy_tree.filterContent == None
                    
                    # Set the outputDict
                    sdqlpy_tree.child.outputDict = sdqlpy_tree.outputDict
                    # Set the incomingDict
                    if isinstance(sdqlpy_tree.child, UnarySDQLpyNode):
                        sdqlpy_tree.child.incomingDict = sdqlpy_tree.child.child.outputDict
                    else:
                        raise Exception("Must be a Unary SDQLpy Node")
                
                    # Carry the primary and foreign
                    sdqlpy_tree.child.primaryKey = sdqlpy_tree.primaryKey
                    sdqlpy_tree.child.foreignKeys = sdqlpy_tree.foreignKeys
                    # Carry the removeColumnIDs
                    sdqlpy_tree.child.removeColumnIDs = set(sdqlpy_tree.child.removeColumnIDs).union(sdqlpy_tree.removeColumnIDs)
                                        
                    sdqlpy_tree = sdqlpy_tree.child
                    sdqlpy_tree.foldedInto = True
                else:
                    raise Exception(f"Unexpected Node Detected: {sdqlpy_tree}")
            
        return sdqlpy_tree
    
    sdqlpy_tree = v_fold_run(sdqlpy_tree)
    return sdqlpy_tree

def opt_update_sum(sdqlpy_tree):
    def update_sum_run(sdqlpy_tree):
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = update_sum_run(sdqlpy_tree.left)
            rightNode = update_sum_run(sdqlpy_tree.right)
            
            sdqlpy_tree.left = leftNode
            sdqlpy_tree.right = rightNode
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = update_sum_run(sdqlpy_tree.child)
            
            sdqlpy_tree.child = childNode
        else:
            assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
            
        us_tracking = open("tpch_update_sum.csv", "a")  # append mode
            
        if (sdqlpy_tree.outputDict.duplicateCounter == False) and len(sdqlpy_tree.outputDict.flatVals()) == 0:
            # Is not an UpdateSum - we know that the key is unique
            # So we can pass in "False"
            sdqlpy_tree.outputDict.set_is_assignment_sum(True)
            
            us_tracking.write(f"1\n")
        elif isinstance(sdqlpy_tree, (SDQLpyJoinBuildNode, SDQLpyConcatNode)):
            # Build summations should always be true, also the concat 
            if isinstance(sdqlpy_tree, SDQLpyJoinBuildNode) and hasattr(sdqlpy_tree.child, "vectorValue") and sdqlpy_tree.child.vectorValue == True:
                pass
            else:
                sdqlpy_tree.outputDict.set_is_assignment_sum(True)
                
                us_tracking.write(f"1\n")
        elif isinstance(sdqlpy_tree, (SDQLpyGroupNode, SDQLpyAggrNode, SDQLpyPromoteToFloatNode)):
            # In a groupNode, PromoteToFloat or AggrNode, the key is not unique
            pass
        elif isinstance(sdqlpy_tree, SDQLpyJoinNode):
            if sdqlpy_tree.joinMethod == "bnl":
                pass
            else:
                valueTypeCounter = Counter([type(x) for x in sdqlpy_tree.outputDict.flatVals()])
                if valueTypeCounter[ColumnValue] == len(sdqlpy_tree.outputDict.flatVals()) and len(sdqlpy_tree.outputDict.flatKeys()) >= 1:
                    # Column Types
                    columnNames = set([x.codeName for x in sdqlpy_tree.outputDict.flatVals()])
                    if "sum" in columnNames:
                        pass
                    else:
                        # The value section is just ColumnValues, so we can make it an assignment sum
                        sdqlpy_tree.outputDict.set_is_assignment_sum(True)
                        
                        us_tracking.write(f"1\n")
                else:
                    pass
        else:
            pass
        
        us_tracking.close()
        
        return sdqlpy_tree
    
    sdqlpy_tree = update_sum_run(sdqlpy_tree)    
    return sdqlpy_tree

def opt_dense(sdqlpy_tree, o3_value):
    def run_dense(sdqlpy_tree, dense_value):
        """
        And I'll take a parameter, like 0.9, for when the filtered is 90% the cardinality of the original, then I'll insert dense
        """
        
        if isinstance(sdqlpy_tree, BinarySDQLpyNode):
            leftNode = run_dense(sdqlpy_tree.left, dense_value)
            rightNode = run_dense(sdqlpy_tree.right, dense_value)
            
            sdqlpy_tree.left = leftNode
            sdqlpy_tree.right = rightNode
        elif isinstance(sdqlpy_tree, UnarySDQLpyNode):
            childNode = run_dense(sdqlpy_tree.child, dense_value)
            
            sdqlpy_tree.child = childNode
        else:
            assert isinstance(sdqlpy_tree, LeafSDQLpyNode)
            
        if (isinstance(sdqlpy_tree, SDQLpyJoinBuildNode) and isinstance(sdqlpy_tree.child, SDQLpyRecordNode)
            and sdqlpy_tree.child.filterContent == None
            and len(sdqlpy_tree.outputDict.flatKeys()) == 1
            and sdqlpy_tree.outputDict.keys[0].type == "Integer" and sdqlpy_tree.cardinality != None
            and sdqlpy_tree.child.vectorValue == False):
            # In a JoinBuild, with a Record below
            # The join should only have one key and it should be an integer key
            
            # Get cardinality from defaults
            tpch_cardinality = {
                'region': 5, 
                'part': 200000, 
                'nation': 25, 
                'lineitem': 6001215, 
                'partsupp': 800000, 
                'supplier': 10000, 
                'orders': 1500000, 
                'customer': 150000
            }
            before_card = tpch_cardinality.get(sdqlpy_tree.child.tableName, None)
        
            after_card = sdqlpy_tree.cardinality
            
            assert (before_card != None) and (after_card != None)
            
            card_ratio = after_card / before_card
            
            card_tracking = open("tpch_cardinality_tracking.csv", "a")  # append mode
            card_tracking.write(f"{card_ratio}\n")
            card_tracking.close()
            
            # Check we can do is dense
            if (card_ratio >= dense_value):
                assert isinstance(sdqlpy_tree, SDQLpyJoinBuildNode)
                sdqlpy_tree.is_dense = True
                
                # Set it for orders
                if sdqlpy_tree.child.tableName == "orders":
                    sdqlpy_tree.cardinality = "6000000"
                elif sdqlpy_tree.child.tableName == "region":
                    sdqlpy_tree.cardinality = "5"
                elif sdqlpy_tree.child.tableName == "customer":
                    sdqlpy_tree.cardinality = "150000"
                elif sdqlpy_tree.child.tableName == "lineitem":
                    sdqlpy_tree.cardinality = "6000000"
                elif sdqlpy_tree.child.tableName == "part":
                    sdqlpy_tree.cardinality = "200000"
                elif sdqlpy_tree.child.tableName == "partsupp":
                    sdqlpy_tree.cardinality = "200000"
                elif sdqlpy_tree.child.tableName == "nation":
                    sdqlpy_tree.cardinality = "25"
                elif sdqlpy_tree.child.tableName == "supplier":
                    sdqlpy_tree.cardinality = "10000"
                
        else:
            pass
        
        return sdqlpy_tree
    
    dense_value = float(o3_value)
    sdqlpy_tree = run_dense(sdqlpy_tree, dense_value)
    return sdqlpy_tree
