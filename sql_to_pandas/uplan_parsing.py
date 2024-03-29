from uplan_nodes import *
from uplan_optimisations import *

from sdqlpy_unparser import *
from sdqlpy_transformer import *
from sdqlpy_optimisations import *

from pandas_unparser_v2 import *

from collections import Counter

def audit_universal_plan_tree_outputnode(op_tree: UniversalBaseNode) -> bool:
    return isinstance(op_tree, OutputNode)

def audit_universal_plan_tree_scannode(op_tree: UniversalBaseNode) -> bool:
    def get_leaf_nodes(op_tree: UniversalBaseNode) -> list[UniversalBaseNode]:
        leafs = []
        def _get_leaf_nodes(op_node: UniversalBaseNode):
            match op_node:
                case BinaryBaseNode():
                    _get_leaf_nodes(op_node.left)
                    _get_leaf_nodes(op_node.right)
                case UnaryBaseNode():
                    _get_leaf_nodes(op_node.child)
                case UniversalBaseNode():
                    leafs.append(op_node)
                case _:
                    raise Exception(f"We are auditing a universal plan tree, all nodes should be at minimum a UniversalBaseNode, not: {op_node.__class__}") 
        _get_leaf_nodes(op_tree)
        return leafs
    
    # Get all leaves, make sure they're all ScanNode
    all_leaves = get_leaf_nodes(op_tree)
    return all(isinstance(leaf, ScanNode) for leaf in all_leaves)

def set_flowColumns(uplan_tree):
    # Post Order traversal: Visit Children
    leftNode, rightNode, childNode = None, None, None
    if isinstance(uplan_tree, BinaryBaseNode):
        leftNode = set_flowColumns(uplan_tree.left)
        rightNode = set_flowColumns(uplan_tree.right)
    elif isinstance(uplan_tree, UnaryBaseNode):
        childNode = set_flowColumns(uplan_tree.child)
    else:
        # A leaf node
        pass
    
    # Assign previous changes
    if (leftNode != None) and (rightNode != None):
        uplan_tree.left = leftNode
        uplan_tree.right = rightNode
    elif (childNode != None):
        uplan_tree.child = childNode
    else:
        # A leaf node
        pass
    
    # Run on current node (sdqlpy_tree)
    if isinstance(uplan_tree, ScanNode):
        if uplan_tree.primaryKey == None:
            uplan_tree.flowColumns = uplan_tree.tableColumns
        else:
            # We apply the partial column limiting optimisation here
            uplan_tree.flowColumns = list(set([x for x in uplan_tree.tableColumns if x.essential == True] + list(uplan_tree.primaryKey)))
            uplan_tree.tableColumns = uplan_tree.flowColumns
    elif isinstance(uplan_tree, JoinNode):
        if uplan_tree.joinType in ["inner", "outer"]:
            uplan_tree.flowColumns = uplan_tree.left.flowColumns + uplan_tree.right.flowColumns
        elif "left" in uplan_tree.joinType:
            uplan_tree.flowColumns = uplan_tree.left.flowColumns
        elif "right" in uplan_tree.joinType:
            uplan_tree.flowColumns = uplan_tree.right.flowColumns
        else:
            raise Exception(f"Unrecognised Join Type for JoinNode: {uplan_tree.joinType}")
    elif isinstance(uplan_tree, GroupNode):
        if uplan_tree.keyExpressions == []:
            uplan_tree.flowColumns = uplan_tree.postAggregateOperations
        else:
            uplan_tree.flowColumns = uplan_tree.keyExpressions + uplan_tree.postAggregateOperations
    elif isinstance(uplan_tree, NewColumnNode):
        uplan_tree.flowColumns = uplan_tree.values + uplan_tree.child.flowColumns
    elif isinstance(uplan_tree, (SortNode, LimitNode, OutputNode, FilterNode, RetrieveNode)):
        uplan_tree.flowColumns = uplan_tree.child.flowColumns
    else:
        raise Exception(f"Unrecognised node: {uplan_tree}")

    return uplan_tree

def fix_orderJoinsForPrimaryForeignKeys(uplan_tree: UniversalBaseNode, table_schema: dict) -> UniversalBaseNode:
    def whatTypeAreKeys(joinKeys: list, childNode):
        assert len(joinKeys) > 0
        nextJoinKeysIDs = [id(x) for x in joinKeys]
                
        outcomes = []
        primaryKeyIDs = [id(x) for x in childNode.primaryKey]
        foreignKeyIDs = [id(x) for x in childNode.foreignKeys]
        
        primaryKeyCounter = 0
        
        # Check for primary first
        for jKey in joinKeys:
            if id(jKey) in primaryKeyIDs:
                primaryKeyCounter += 1
                if primaryKeyCounter == len(childNode.primaryKey):
                    outcomes.append("P")
                    for pkey in childNode.primaryKey:
                        nextJoinKeysIDs.remove(id(pkey))
        
        # Check for foreign keys after
        for jKey in nextJoinKeysIDs:
            if jKey in foreignKeyIDs:
                outcomes.append("F")
            else:
                # Joining on keys that are neither Primary nor Foreign
                outcomes.append("N")
        
        return outcomes
    
    # Post Order traversal: Visit Children
    leftNode, rightNode, childNode = None, None, None
    if isinstance(uplan_tree, BinaryBaseNode):
        leftNode = fix_orderJoinsForPrimaryForeignKeys(uplan_tree.left, table_schema)
        rightNode = fix_orderJoinsForPrimaryForeignKeys(uplan_tree.right, table_schema)
    elif isinstance(uplan_tree, UnaryBaseNode):
        childNode = fix_orderJoinsForPrimaryForeignKeys(uplan_tree.child, table_schema)
    else:
        # A leaf node
        pass
    
    # Assign previous changes
    if (leftNode != None) and (rightNode != None):
        uplan_tree.left = leftNode
        uplan_tree.right = rightNode
    elif (childNode != None):
        uplan_tree.child = childNode
    else:
        # A leaf node
        pass
    
    # Run on current node (sdqlpy_tree)
    if isinstance(uplan_tree, JoinNode):
        # Check the cardinality information for left and right exists
        assert uplan_tree.left.cardinality != None
        assert uplan_tree.right.cardinality != None
        
        assert len(uplan_tree.leftKeys) > 0
        assert len(uplan_tree.rightKeys) > 0
        
        # Check which is primary and foreign, to set primary/foreign for current node
        leftType = whatTypeAreKeys(uplan_tree.leftKeys, uplan_tree.left)
        rightType = whatTypeAreKeys(uplan_tree.rightKeys, uplan_tree.right)
        
        leftKeys_str = [x.codeName for x in uplan_tree.leftKeys]
        rightKeys_str = [x.codeName for x in uplan_tree.rightKeys]
        
        joinType = "basic"
        
        if leftType == ["N"] and rightType == ["N"]:
            # Joining on keys that are neither Primary nor Foreign
            if isinstance(leftNode, ScanNode) and isinstance(rightNode, ScanNode):
                # We expect them both to be record nodes and that they're of the same table
                assert leftNode.tableName == rightNode.tableName
                # Double check that cardinalities are the same and leave it as we found it
                assert leftNode.cardinality == rightNode.cardinality
            elif isinstance(leftNode, GroupNode) and isinstance(rightNode, RetrieveNode):
                # All good
                pass
            else:
                # That it's a non-equi join, verify that it is - by looking at the operators
                if isinstance(uplan_tree.joinCondition, list):
                    joinConditionTypes = set([type(x) for x in uplan_tree.joinCondition])
                else:
                    joinConditionTypes = set([type(uplan_tree.joinCondition)])
                nonEquiJoinTypes = set([type(x) for x in [GreaterThanEqOperator(), GreaterThanOperator(), LessThanEqOperator(), LessThanOperator()]])
                # Subset - all items of A are present in B
                joinType = "non-equi"
                assert joinConditionTypes.issubset(nonEquiJoinTypes)
        elif "P" in leftType and "P" in rightType:
            # Both left and right are primary
            # Prefer the one with lower cardinality
            if uplan_tree.left.cardinality <= uplan_tree.right.cardinality:
                # We should index on Left
                # No swap required
                pass
            else:
                # We should index on Right
                # Swap required
                uplan_tree.swapLeftAndRight()
                assert uplan_tree.left.cardinality <= uplan_tree.right.cardinality
        elif "P" in leftType:
            # Check that all of right are "F" or "N"
            assert len(set(rightType)) > 0 and Counter(rightType)["P"] == 0
            # That there's only 1 P in left and the rest are F or N
            assert Counter(leftType)["P"] == 1 and (Counter(leftType)["F"] + Counter(leftType)["N"] == len(leftType) - 1)
            
            # Index on Left
            # We should index on Left
            # No swap required
            pass
        elif "P" in rightType:
            # Check that all of left are "F" or "N"
            leftCounter = Counter(leftType)
            assert (leftCounter["F"] > 0 or leftCounter["N"] > 0) and (leftCounter["P"] == 0)
            # And, that there's only 1 P in right and the rest are F or N
            assert Counter(rightType)["P"] == 1 and (Counter(rightType)["F"] + Counter(rightType)["N"] == len(rightType) - 1)
            
            # We should index on Right
            # Swap required
            uplan_tree.swapLeftAndRight()
        elif leftKeys_str == rightKeys_str:
            # They are entirely foreign
            assert set(leftType) == set("F") and set(rightType) == set("F")
            # If we are joining on identical keys, even if they're not primary, it's still okay
            # We'll use cardinality to decide
            # Prefer the one with lower cardinality
            if uplan_tree.left.cardinality <= uplan_tree.right.cardinality:
                # We should index on Left
                # No swap required
                pass
            else:
                # We should index on Right
                # Swap required
                uplan_tree.swapLeftAndRight()
                assert uplan_tree.left.cardinality <= uplan_tree.right.cardinality
        else:
            raise Exception(f"Unrecognised format for the Join Condition or unexpected Primary/Foreign Key configuration")
        
        # Set the primary/foreignKeys
        # The primary will be the primary of right
        if uplan_tree.joinType == "leftantijoin":
            flowColumnIDs = [id(x) for x in uplan_tree.flowColumns]
            leftPrimIDs = [id(x) for x in uplan_tree.left.primaryKey]
            assert all([x in flowColumnIDs for x in leftPrimIDs])
            uplan_tree.setPrimary(uplan_tree.left.primaryKey)
        elif uplan_tree.joinType == "leftsemijoin":
            flowColumnIDs = [id(x) for x in uplan_tree.flowColumns]
            leftPrimIDs = [id(x) for x in uplan_tree.left.primaryKey]
            assert all([x in flowColumnIDs for x in leftPrimIDs])
            uplan_tree.setPrimary(uplan_tree.left.primaryKey)
        elif uplan_tree.joinType == "rightantijoin":
            flowColumnIDs = [id(x) for x in uplan_tree.flowColumns]
            rightPrimIDs = [id(x) for x in uplan_tree.right.primaryKey]
            assert all([x in flowColumnIDs for x in rightPrimIDs])
            uplan_tree.setPrimary(uplan_tree.right.primaryKey)
        elif uplan_tree.joinType == "rightsemijoin":
            flowColumnIDs = [id(x) for x in uplan_tree.flowColumns]
            rightPrimIDs = [id(x) for x in uplan_tree.right.primaryKey]
            assert all([x in flowColumnIDs for x in rightPrimIDs])
            uplan_tree.setPrimary(uplan_tree.right.primaryKey)
        else:
            uplan_tree.setPrimary(uplan_tree.right.primaryKey)
            
        # Set foreign keys
        uplan_tree.addForeign(uplan_tree.left.foreignKeys)
        uplan_tree.addForeign(uplan_tree.left.waitingForeignKeys)
        uplan_tree.addForeign(uplan_tree.right.foreignKeys)
        uplan_tree.addForeign(uplan_tree.right.waitingForeignKeys)
        uplan_tree.resolveForeignKeys()
            
    else:
        # Not a join, we should set/propagate Primary/Foreign Keys
        if isinstance(uplan_tree, ScanNode):
            # Add primary key
            getPrimary = table_schema[uplan_tree.tableName][0]
            assert len(getPrimary) == 1
            primaryKeyList = []
            for primary_key in list(list(getPrimary)[0]):
                primaryKeyList.append(
                    returnFromFlowColumns(
                        primary_key, uplan_tree.flowColumns
                    )
                )
            uplan_tree.setPrimary(tuple(primaryKeyList))
            # Add foreign key
            getForeign = table_schema[uplan_tree.tableName][1]
            reformatedForeign = dict()
            for foreign_key, value in getForeign.items():
                reformatedForeign[
                    returnFromFlowColumns(
                        foreign_key, uplan_tree.flowColumns
                    )
                ] = value
            uplan_tree.addForeign(reformatedForeign)
        elif isinstance(uplan_tree, GroupNode) and len(uplan_tree.keyExpressions) > 0:
            # Set the key as primary
            uplan_tree.setPrimary(tuple(uplan_tree.keyExpressions))
            uplan_tree.addForeign(uplan_tree.child.foreignKeys)
            uplan_tree.addForeign(uplan_tree.child.waitingForeignKeys)
        else:
            # Propagate Forwards
            assert len(uplan_tree.child.primaryKey) > 0
            uplan_tree.setPrimary(uplan_tree.child.primaryKey)
            uplan_tree.addForeign(uplan_tree.child.foreignKeys)
            uplan_tree.addForeign(uplan_tree.child.waitingForeignKeys)
           
    return uplan_tree

def fix_flowColumnsEmptyCodeName(uplan_tree, parserCreatedColumns):
    # Post Order traversal: Visit Children
    if isinstance(uplan_tree, BinaryBaseNode):
        fix_flowColumnsEmptyCodeName(uplan_tree.left, parserCreatedColumns)
        fix_flowColumnsEmptyCodeName(uplan_tree.right, parserCreatedColumns)
    elif isinstance(uplan_tree, UnaryBaseNode):
        fix_flowColumnsEmptyCodeName(uplan_tree.child, parserCreatedColumns)
    else:
        # A leaf node
        pass
    
    # Run on current node (sdqlpy_tree)
    for col in uplan_tree.flowColumns:
        if col.codeName == "":
            handleEmptyCodeName(col, parserCreatedColumns)
            assert col.codeName != ""
            parserCreatedColumns.add(col.codeName)
            
def fix_solveDuplicateColumnsNames(uplan_tree):
    def getProblemCodeNamesFromFlowColumns(flowColumns: list) -> list:
        # Check that the codeNames are unique
        nameDict = defaultdict(int)
        for col in flowColumns:
            nameDict[col.codeName] += 1
            
        problemCodeNames = [k for k, v in nameDict.items() if v > 1]
        return problemCodeNames, nameDict
    
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

    # Post Order traversal: Visit Children
    leftNode, rightNode, childNode = None, None, None
    if isinstance(uplan_tree, BinaryBaseNode):
        leftNode = fix_solveDuplicateColumnsNames(uplan_tree.left)
        rightNode = fix_solveDuplicateColumnsNames(uplan_tree.right)
    elif isinstance(uplan_tree, UnaryBaseNode):
        childNode = fix_solveDuplicateColumnsNames(uplan_tree.child)
    else:
        # A leaf node
        pass
    
    # Assign previous changes
    if (leftNode != None) and (rightNode != None):
        uplan_tree.left = leftNode
        uplan_tree.right = rightNode
    elif (childNode != None):
        uplan_tree.child = childNode
    else:
        # A leaf node
        pass
    
    # Run on current node (sdqlpy_tree)
    if isinstance(uplan_tree, JoinNode):
        assert len(uplan_tree.flowColumns) > 0
        
        # Any values that are > 1, we have a problem and need to resolve duplicates 
        # in the output flowColumns
        problemCodeNames, problemCodeNameDict = getProblemCodeNamesFromFlowColumns(uplan_tree.flowColumns)
        if len(problemCodeNames) > 0:
            for badCodeName in problemCodeNames:
                    filtered_FlowColumns = [x for x in uplan_tree.flowColumns if x.codeName == badCodeName]
                    assert len(filtered_FlowColumns) == problemCodeNameDict[badCodeName] and problemCodeNameDict[badCodeName] == 2
                    
                    # Change the codenames, for those in filtered
                    nameAffixes = ["_x", "_y"]
                    for idx, affix in enumerate(nameAffixes):
                        filtered_FlowColumns[idx].codeName = f"{filtered_FlowColumns[idx].codeName}{affix}"
                
            # Check this has resolved things
            problemCodeNames, _ = getProblemCodeNamesFromFlowColumns(uplan_tree.flowColumns)
            assert len(problemCodeNames) == 0
            
        # Check the Join Keys for Duplicates as well
        assert (len(uplan_tree.leftKeys) > 0) and (len(uplan_tree.rightKeys) > 0)
        problemCodeNames, problemCodeNameDict = getProblemCodeNamesFromJoinKeys(uplan_tree.leftKeys, uplan_tree.rightKeys)
        if len(problemCodeNames) > 0:
            for badCodeName in problemCodeNames:
                filtered_FlowColumns = [x for x in (uplan_tree.leftKeys + uplan_tree.rightKeys) if x.codeName == badCodeName]
                assert len(filtered_FlowColumns) == problemCodeNameDict[badCodeName] and problemCodeNameDict[badCodeName] == 2
                
                # Change the codenames, for those in filtered
                nameAffixes = ["_x", "_y"]
                for idx, affix in enumerate(nameAffixes):
                    filtered_FlowColumns[idx].codeName = f"{filtered_FlowColumns[idx].codeName}{affix}"
            
            # Check this has resolved things
            problemCodeNames, _ = getProblemCodeNamesFromJoinKeys(uplan_tree.leftKeys, uplan_tree.rightKeys)
            assert len(problemCodeNames) == 0

    return uplan_tree

def uplan_to_exec_format(op_tree, output_format, table_schema, uplan_opts, query_name):
    # We have a Universal Plan tree at this point, we need to traverse it to fix some items
    # Flow Columns through tree for later
    op_tree = set_flowColumns(op_tree)
    # Solve flowColumns with empty CodeName
    parserCreatedColumns = set()
    fix_flowColumnsEmptyCodeName(op_tree, parserCreatedColumns)
    # Order Joins - we need to do this before duplicate renaming
    op_tree = fix_orderJoinsForPrimaryForeignKeys(op_tree, table_schema)
    op_tree = set_flowColumns(op_tree)
    # Solve duplicate column names in the tree
    op_tree = fix_solveDuplicateColumnsNames(op_tree)
    
    # And, also apply optimisations
    op_tree = uplan_apply_optimisations(op_tree, uplan_opts)
    
    # Test 1: top node should be OutputNode
    #assert audit_universal_plan_tree_outputnode(op_tree)
    # Test 2: all leaf nodes should be ScanNode
    assert audit_universal_plan_tree_scannode(op_tree)
    
    unparse_content = None
    if output_format == "pandas":
        # Convert Universal Plan Tree to Pandas Tree
        pd_tree = convert_universal_to_pandas(op_tree)
        print(f"Converted Universal Plan Tree of {query_name} into Pandas Tree")

        # Unparse Pandas Tree
        # try:
        unparse_content = UnparsePandasTree(pd_tree)
        # except:
        #     print(f"Pandas Generation for Query '{query_name}' Failed.")
        #     raise Exception("Failed Pandas Generation")
    elif output_format == "sdqlpy":
        # Convert Universal Plan Tree to SDQLpy Tree
        sdqlpy_tree = convert_universal_to_sdqlpy(op_tree)
        
        # Test: All leaf nodes should be SDQLpyRecordNode
        assert audit_sdqlpy_tree_leafnode(sdqlpy_tree)
        print(f"Converted Universal Plan Tree of {query_name} into SDQLpy Tree")
        
        # Unparse SDQLpy Tree
        # try:
        unparse_content = UnparseSDQLpyTree(sdqlpy_tree)
        # except:
        #     print(f"SDQLpy Generation for Query '{query_name}' Failed.")
        #     raise Exception("Failed SDQLpy Generation")
    else:
        raise Exception(f"Unexpected format for output_format: {output_format}")
        
    assert unparse_content != None
    return unparse_content
