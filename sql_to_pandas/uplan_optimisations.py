from enum import Enum

from uplan_nodes import *

class SupportedOptimisations(Enum):
    COL_ELIM = "ColumnElimination"
    
    @staticmethod
    def has_value(item):
        return item in [v.value for v in SupportedOptimisations.__members__.values()]

OPTIMISATIONS_ORDER = [
    SupportedOptimisations.COL_ELIM
]

def optimisation_runner(uplan_tree, inOpt):
    match inOpt:
        case SupportedOptimisations.COL_ELIM.value:
            uplan_tree = opt_col_elim(uplan_tree)
        case _:
            raise Exception(f"The optimisation: {inOpt} was not recognised.")
        
    return uplan_tree
    

def uplan_apply_optimisations(uplan_tree, inOptimisations):
    if (inOptimisations == [""]) or (inOptimisations == ""):
        return uplan_tree
    
    # Check optimisations are all okay
    assert isinstance(inOptimisations, list)
    assert all([SupportedOptimisations.has_value(x) for x in inOptimisations])
    assert len(inOptimisations) <= len(OPTIMISATIONS_ORDER)
    
    # We should sort the optimisations
    order = {v.value:i for i,v in enumerate(OPTIMISATIONS_ORDER)}
    inOptimisations = sorted(inOptimisations, key=lambda x: order[x])
    
    for opt in inOptimisations:
        uplan_tree = optimisation_runner(uplan_tree, opt)
        
    return uplan_tree

def opt_col_elim(uplan_tree):
    def makeAllList(elements):
        flat_list = []
        for row in elements:
            if isinstance(row, tuple):
                flat_list.extend(list(row))
            else:
                flat_list.append(row)
        return flat_list
        
    def requiredColumns(uplan_tree):
        requiredColumns = []
        match uplan_tree:
            case OutputNode():
                requiredColumns.extend(uplan_tree.outputColumns)
            case GroupNode():
                requiredColumns.extend(uplan_tree.keyExpressions + uplan_tree.postAggregateOperations + uplan_tree.preAggregateExpressions)
            case JoinNode():
                requiredColumns.extend(uplan_tree.leftKeys + uplan_tree.rightKeys + [uplan_tree.joinCondition])
            case ScanNode():
                if uplan_tree.tableFilters != []:
                    requiredColumns.extend([uplan_tree.tableFilters])
                if uplan_tree.tableRestrictions != []:
                    requiredColumns.extend([uplan_tree.tableRestrictions])
                requiredColumns.extend(uplan_tree.tableColumns)
            case _:
                raise Exception(f"Unrecognised Universal Node: {uplan_tree}")
        
        # Add Primary
        assert len(uplan_tree.primaryKey) > 0
        requiredColumns.extend(makeAllList(uplan_tree.primaryKey))
        # TODO: Do we need to include Foreign Keys in this traversal
        return requiredColumns
    
    def getBelowColumns(uplan_tree) -> list:
        belowColumns = []
        if isinstance(uplan_tree, BinaryBaseNode):
            belowColumns.extend(uplan_tree.left.flowColumns)
            belowColumns.extend(uplan_tree.right.flowColumns)
        elif isinstance(uplan_tree, UnaryBaseNode):
            belowColumns.extend(uplan_tree.child.flowColumns)
        else:
            assert isinstance(uplan_tree, LeafBaseNode)
            pass
        
        return belowColumns
    
    def calculateNewlySeenNodes(uplan_tree) -> list:
        belowColumns = getBelowColumns(uplan_tree)
        currentFlowColumns = uplan_tree.flowColumns
        
        newlySeenColumnIDs = set(getIDsForList(currentFlowColumns)) - set(getIDsForList(belowColumns))
        current_flow_id_dict = build_flow_id_dict(currentFlowColumns)
        
        newlySeenColumns = [current_flow_id_dict[x] for x in newlySeenColumnIDs]
        
        return newlySeenColumns
    
    def getIDsForList(columns):
        return [id(x) for x in columns]
    
    def getAllColumns(expr: ExpressionBaseNode):
        all_cols = []
        if isinstance(expr, BinaryExpressionOperator):
            all_cols.extend(getAllColumns(expr.left))
            all_cols.extend(getAllColumns(expr.right))
        elif isinstance(expr, UnaryExpressionOperator):
            all_cols.extend(getAllColumns(expr.child))
        else:
            pass
        
        # Do on current
        all_cols.append(expr)
        
        return all_cols
    
    def getContributingColumns(expr: ExpressionBaseNode, belowColumns: list):
        contribs = []
        if isinstance(expr, BinaryExpressionOperator):
            contribs.extend(getContributingColumns(expr.left, belowColumns))
            contribs.extend(getContributingColumns(expr.right, belowColumns))
        elif isinstance(expr, UnaryExpressionOperator):
            contribs.extend(getContributingColumns(expr.child, belowColumns))
        else:
            pass
        
        if id(expr) in getIDsForList(belowColumns):
            contribs.append(expr)
        
        return contribs
        
    
    def requiredIDs(uplan_tree):
        reqCols = requiredColumns(uplan_tree)
        allReqIDs = getIDsForList(reqCols)
        dedupedReqIDs = list(set(allReqIDs))
        
        # Do Seen here
        removePositions = set()
        addColumns = []
        newlySeenColumns = calculateNewlySeenNodes(uplan_tree)
        if len(newlySeenColumns) > 0:
            for newCol in newlySeenColumns:
                allExprNodes = getAllColumns(newCol)
                contibutingColumns = getContributingColumns(newCol, getBelowColumns(uplan_tree))
                tryRemoveCols = set(getIDsForList(allExprNodes)) - set(getIDsForList(contibutingColumns))
                for remCol in tryRemoveCols:
                    if remCol in dedupedReqIDs:
                        removePositions.add(dedupedReqIDs.index(remCol))
                addColumns.extend(contibutingColumns)
        
        if len(removePositions) > 0:
            removePositions = list(removePositions)
            removePositions = sorted(removePositions, reverse=True)
            # Carry out deletes
            for rem in removePositions:
                dedupedReqIDs.pop(rem)
                
        if len(addColumns) > 0:
            for addCol in addColumns:
                dedupedReqIDs.append(id(addCol))
            
            # # # Testing
            # reqCols_id_dict = build_flow_id_dict(reqCols)
            # for ddID in dedupedReqIDs:
            #     print(f"{reqCols_id_dict[ddID].codeName}")
        
        return list(set(dedupedReqIDs))
    
    def build_flow_id_dict(flowColumns: list) -> dict:
        flow_id_dict = dict()
        for flowCol in flowColumns:
            flow_id_dict[id(flowCol)] = flowCol
            
        return flow_id_dict
    
    def get_columns_for_id(flowColumns, IDs):
        current_flow_id_dict = build_flow_id_dict(flowColumns)
        
        return [current_flow_id_dict[x] for x in IDs]
    
    def run_col_elim(uplan_tree, requiredForAboveIds = None):
        # Preorder Traversal
        # Run on current node
        
        # Run if we're at the root node
        if requiredForAboveIds == None:
            assert isinstance(uplan_tree, OutputNode)
            requiredForAboveIds = requiredIDs(uplan_tree)
        
        # Compare With FlowColumns
        flowIDs = [id(x) for x in uplan_tree.flowColumns]
        primaryKeyIDs = getIDsForList(makeAllList(uplan_tree.primaryKey))
        removeIDs = set(flowIDs) - (set(requiredForAboveIds).union(set(primaryKeyIDs)))
        uplan_tree.removeColumnIDs = removeIDs
        # if len(removeIDs) > 0:
        #     print(f"We can remove {len(removeIDs)} columns from the {uplan_tree} node")
        #     # Show what they are
        #     flow_id_dict = build_flow_id_dict(uplan_tree.flowColumns)
        #     for removeID in removeIDs:
        #         print(f"{flow_id_dict[removeID].codeName}")
        
        # Calculate Required For Above, for this current Node
        requiredForCurrentNode = requiredIDs(uplan_tree)
        # requiredForCurrentNodeColumns = get_columns_for_id(uplan_tree.flowColumns, requiredForCurrentNode)
        requiredForAboveIds = list(set(requiredForCurrentNode).union(requiredForAboveIds))
        
        if isinstance(uplan_tree, BinaryBaseNode):
            run_col_elim(uplan_tree.left, requiredForAboveIds)
            run_col_elim(uplan_tree.right, requiredForAboveIds)
        elif isinstance(uplan_tree, UnaryBaseNode):
            run_col_elim(uplan_tree.child, requiredForAboveIds)
        else:
            assert isinstance(uplan_tree, LeafBaseNode)
            pass
    
    run_col_elim(uplan_tree)
    return uplan_tree
    