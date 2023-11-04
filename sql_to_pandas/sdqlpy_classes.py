from collections import Counter

from universal_plan_nodes import *
from expression_operators import *

from sdqlpy_helpers import *
TAB = "    "

# Classes for the SDQLpy Tree
class SDQLpyBaseNode():
    def __init__(self):
        # The columns, as strs
        self.incomingColumns = set()
        self.outputColumns = set()
        self.nodeID = None
        self.sdqlrepr = None
        self.topNode = False
        self.filterContent = None
    
    def addFilterContent(self, filterContent):
        assert self.filterContent == None
        self.filterContent = filterContent
        
    def addID(self, value):
        assert self.nodeID == None
        self.nodeID = value
    
    def getTableName(self, unparser, not_output=False):
        assert self.sdqlrepr != None
        if self.topNode == True and not_output == False:
            # Default SDQL name for top level output
            self.tableName =  "results"
        else:
            nodeNumber = unparser.nodesCounter[self.__class__.__name__]
            self.tableName = f"{self.sdqlrepr}_{str(nodeNumber)}"
            
        return self.tableName
        
class LeafSDQLpyNode(SDQLpyBaseNode):
    def __init__(self):
        super().__init__()
        
class PipelineBreakerNode(SDQLpyBaseNode):
    def __init__(self):
        super().__init__()
        
class UnarySDQLpyNode(PipelineBreakerNode):
    def __init__(self):
        super().__init__()
        self.child = None
        self.incomingColumns = set()
        
    def addChild(self, child: SDQLpyBaseNode):
        assert self.child == None
        self.child = child
        self.incomingColumns = self.child.outputColumns
        
    def getChildName(self, unparser):
        childTableList = unparser.getChildTableNames(self)
        assert len(childTableList) == 1
        return childTableList[0]
        
class BinarySDQLpyNode(PipelineBreakerNode):
    def __init__(self):
        super().__init__()
        self.left = None
        self.right = None
        self.incomingColumns = set()
        
    def addLeft(self, left: SDQLpyBaseNode):
        assert self.left == None
        self.left = left
        self.incomingColumns = self.incomingColumns.union(self.left.outputColumns)
        
    def addRight(self, right: SDQLpyBaseNode):
        assert self.right == None
        self.right = right
        self.incomingColumns = self.incomingColumns.union(self.right.outputColumns)
        
    def getChildNames(self, unparser):
        childTableList = unparser.getChildTableNames(self)
        assert len(childTableList) == 2
        return childTableList

# Classes for Nodes
class SDQLpyRecordNode(LeafSDQLpyNode):
    def __init__(self, tableName, tableColumns):
        super().__init__()
        self.tableName = tableName
        self.sdqlrepr = tableName
        # Filter for only essential columns
        self.tableColumns = [x for x in tableColumns if x.essential == True]
        self.incomingColumns = set(self.tableColumns)
        self.outputColumns = set(self.tableColumns)
        
    def getTableName(self, unparser):
        return self.sdqlrepr

class SDQLpyJoinBuildNode(UnarySDQLpyNode):
    def __init__(self, tableName, tableKeys, additionalColumns):
        super().__init__()
        self.tableName = tableName
        assert isinstance(tableKeys, list) and len(tableKeys) == 1
        self.tableKey = tableKeys[0]
        assert isinstance(additionalColumns, list)
        # Filter for additional columns not equal to the key column
        self.additionalColumns = additionalColumns
        self.sdqlrepr = "indexed"
        self.outputColumns = set([self.tableKey]).union(set(self.additionalColumns))

class SDQLpyAggrNode(UnarySDQLpyNode):
    def __init__(self, aggregateOperations):
        super().__init__()
        self.outputRecord = SDQLpyRecordOutput(
            [],
            aggregateOperations
        )
        self.sdqlrepr = "aggr"
        self.outputColumns = set(aggregateOperations)
        
class SDQLpyConcatNode(UnarySDQLpyNode):
    def __init__(self, outputColumns):
        super().__init__()
        self.sdqlrepr = "concat"
        self.outputColumns = outputColumns
        
class SDQLpyGroupNode(UnarySDQLpyNode):
    def __init__(self, keyExpressions, aggregateOperations):
        super().__init__()
        self.outputRecord = SDQLpyRecordOutput(
            keyExpressions,
            aggregateOperations
        )
        self.sdqlrepr = "group"
        self.outputColumns = set(aggregateOperations).union(set(keyExpressions))
        
class SDQLpyJoinNode(BinarySDQLpyNode):
    KNOWN_JOIN_METHODS = set([
        'hash'
    ])
    KNOWN_JOIN_TYPES = set([
        'inner'
    ])
    
    def __init__(self, joinMethod, joinType, joinCondition):
        super().__init__()
        assert joinMethod in self.KNOWN_JOIN_METHODS, f"{joinMethod} is not in the known join methods"
        self.joinMethod = joinMethod
        assert joinType in self.KNOWN_JOIN_TYPES, f"{joinType} is not in the known join types"
        self.joinType = joinType
        joinCondition = self.__splitConditionsIntoList(joinCondition)
        assert isinstance(joinCondition, list)
        self.joinCondition = joinCondition
        self.outputRecord = None
        self.sdqlrepr = "join"
        self.third_node = None
        self.is_update_sum = False
        
    def set_output_record(self, incomingRecord):
        assert isinstance(incomingRecord, SDQLpyRecordOutput)
        self.outputRecord = incomingRecord
        
    def update_update_sum(self, newValue):
        self.is_update_sum = newValue
        
    def set_columns_variable(self):
        match self.joinType:
            case "inner":
                assert (self.left != None) and (self.right != None)
                self.outputColumns = self.left.outputColumns.union(self.right.outputColumns)
            case _:
                raise Exception(f"No columns variable set for joinType: {self.joinType}")
        
    def __splitConditionsIntoList(self, joinCondition: ExpressionBaseNode) -> list[ExpressionBaseNode]:
        newConditions = []
        joiningNodes = [AndOperator]
        currentJoinCondition = joinCondition
        while any(isinstance(currentJoinCondition, x) for x in joiningNodes):
            newConditions.append(currentJoinCondition.left)
            currentJoinCondition = currentJoinCondition.right
        newConditions.append(currentJoinCondition)
        # At this stage, we should scoop up any postJoinFilters
        self.postJoinFilters = list(filter(lambda x: isinstance(x, OrOperator), newConditions))
        realNewConditions = list(filter(lambda x: not isinstance(x, OrOperator), newConditions))
        
        # Extract a 'LessThanOperator' from a list of >= 1 EqualsOperator
        operatorCount = Counter(realNewConditions)
        non_equi_join_operators = [LessThanOperator(), LessThanEqOperator(), GreaterThanOperator()]
        non_equi_join_operator_types = [LessThanOperator, LessThanEqOperator, GreaterThanOperator]
        if (operatorCount[EqualsOperator()] > 0) and any(operatorCount[x] > 0 for x in non_equi_join_operators):
            self.postJoinFilters.extend(list(filter(lambda x: any(isinstance(x, op) for op in non_equi_join_operator_types), realNewConditions)))
            realNewConditions = list(filter(lambda x: not any(isinstance(x, op) for op in non_equi_join_operator_types), realNewConditions))
            assert len(realNewConditions) > 0
        
        return realNewConditions
    
    def do_join_key_separation(self):
        self.leftKeys, self.rightKeys = [], []
        for x in self.joinCondition:
            if id(x.left) in [id(col) for col in self.left.outputColumns]:
                self.leftKeys.append(x.left)
            elif id(x.left) in [id(col) for col in self.right.outputColumns]:
                self.rightKeys.append(x.left)
            else:
                raise Exception(f"Couldn't find the x.left value in either of the left and right tables!")
            
            if id(x.right) in [id(col) for col in self.left.outputColumns]:
                self.leftKeys.append(x.right)
            elif id(x.right) in [id(col) for col in self.right.outputColumns]:
                self.rightKeys.append(x.right)
            else:
                raise Exception(f"Couldn't find the x.right value in either of the left and right tables!")
            
        assert len(self.leftKeys) == len(self.rightKeys) == len(self.joinCondition)

    def add_third_node(self, node):
        assert self.third_node == None
        self.third_node = node

# Classes for SDQLpy Constructs
class SDQLpyRecordOutput():
    def __init__(self, keys, columns):
        assert isinstance(keys, list)
        self.keys = keys
        assert isinstance(columns, list)
        self.columns = columns
        self.third_wrap_counter = 0
        
    def wrapColumns(self, col, third_node, third_cols, target_key):
        left_col, right_col, child_col = None, None, None
        if isinstance(col, BinaryExpressionOperator):
            left_col = self.wrapColumns(col.left, third_node, third_cols, target_key)
            right_col = self.wrapColumns(col.right, third_node, third_cols, target_key)
        elif isinstance(col, UnaryExpressionOperator):
            child_col = self.wrapColumns(col.child, third_node, third_cols, target_key)
        else:
            # A value node
            assert isinstance(col, LeafNode)
            pass
        
        # Assign previous changes
        if (left_col != None) and (right_col != None):
            col.left = left_col
            col.right = right_col
        elif (child_col != None):
            col.child = child_col
        else:
            # A leaf node
            pass
        
        match col:
            case ColumnValue():
                if col.codeName in third_cols != None:
                    new_col = SDQLpyThirdNodeWrapper(col, third_node, target_key)
                    self.third_wrap_counter += 1
                    return new_col
            
        return col
        
    def checkForThirdNodeColumns(self, third_node, target_keys):
        self.third_wrap_counter = 0
        # Checks the keys and columns
        # To see if theyre a part of the third node
        # If they are, it wraps them up in a third node wrapper
        # Increment the counter, so we know how many we've found
        
        assert len(target_keys) == 1
        target_key = target_keys[0]
        
        third_cols_str = [x.codeName for x in third_node.outputColumns]
        
        for idx, key in enumerate(self.keys):
            self.keys[idx] = self.wrapColumns(
                key, third_node, third_cols_str, target_key
            )
        for idx, col in enumerate(self.columns):
            self.columns[idx] = self.wrapColumns(
                col, third_node, third_cols_str, target_key
            )
        
        return self.third_wrap_counter
        
        
    def generateSDQLpyTwoLambda(self, l_lambda_idx, r_lambda_idx, l_columns, r_columns):
        # Assign sourceNode to the Column Values
        for key in self.keys:
            setSourceNodeColumnValues(key, l_lambda_idx, l_columns, r_lambda_idx, r_columns)
        for col in self.columns:
            setSourceNodeColumnValues(col, l_lambda_idx, l_columns, r_lambda_idx, r_columns)
        
        output_content = self.generateSDQLpyContent()
        
        for key in self.keys:
            resetColumnValues(key)
        for col in self.columns:
            resetColumnValues(col)
            
        return output_content
    
    def generateSDQLpyOneLambda(self, lambda_idx, columns):
        # Assign sourceNode to the Column Values
        for key in self.keys:
            setSourceNodeColumnValues(key, lambda_idx, columns)
        for col in self.columns:
            setSourceNodeColumnValues(col, lambda_idx, columns)
        
        output_content = self.generateSDQLpyContent()
        
        for key in self.keys:
            resetColumnValues(key)
        for col in self.columns:
            resetColumnValues(col)
            
        return output_content
        
    def generateSDQLpyContent(self):
        output_content = []
        
        if self.keys == []:
            # If there are no keys, this should be an aggr output
            assert len(self.columns) == 1
            assert isinstance(self.columns[0], SumAggrOperator)
            output_content.append(
                f"{TAB}{convert_expression_operator_to_sdqlpy(self.columns[0].child)}"
            )
            return output_content
        
        output_content.append(
            f"{{"
        )
        
        # Process: Keys
        if len(self.keys) == 1:
            keyFormatted = convert_expression_operator_to_sdqlpy(self.keys[0])
        else:
            keyContent = []
            for key in self.keys:
                expr = convert_expression_operator_to_sdqlpy(key)
                keyContent.append(
                    f'"{key.codeName}": {expr}'
                )
            keyFormatted = f"record({{{', '.join(keyContent)}}})"
        output_content.append(
            f"{TAB}{keyFormatted}:"
        )
        # Process: Columns
        colContent = []
        for col in self.columns:
            if isinstance(col, (ColumnValue, CountAllOperator)):
                expr = convert_expression_operator_to_sdqlpy(col)
            else:
                expr = convert_expression_operator_to_sdqlpy(col.child)
            colContent.append(
                f'"{col.codeName}": {expr}'
            )
        columnFormatted = f"record({{{', '.join(colContent)}}})"
        output_content.append(
            f"{TAB}{columnFormatted}"
        )
        
        output_content.append(
            f"}}"
        )
        
        return output_content
