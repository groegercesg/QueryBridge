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
        self.cardinality = None
    
    def setCardinality(self, card):
        assert self.cardinality == None and isinstance(card, int)
        self.cardinality = card
    
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
    
    def refreshNode(self):
        pass
        
class LeafSDQLpyNode(SDQLpyBaseNode):
    def __init__(self):
        super().__init__()
        
class PipelineBreakerNode(SDQLpyBaseNode):
    def __init__(self):
        super().__init__()
        
    def set_output_record(self, incomingRecord):
        assert isinstance(incomingRecord, SDQLpyRecordOutput)
        self.outputRecord = incomingRecord
        
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
    
    def refreshNode(self):
        self.incomingColumns = self.child.outputColumns
        
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
    
    def refreshNode(self):
        assert isinstance(self, SDQLpyJoinNode)
        self.set_columns_variable()

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
        self.outputRecord = SDQLpyRecordOutput(
            self.tableColumns,
            list() 
        )
        
    def getTableName(self, unparser):
        if self.filterContent != None:
            nodeNumber = unparser.nodesCounter[self.__class__.__name__]
            self.tableName = f"filter_{str(nodeNumber)}"
            return self.tableName
        else:
            return self.sdqlrepr

class SDQLpyJoinBuildNode(UnarySDQLpyNode):
    def __init__(self, tableKeys, additionalColumns):
        super().__init__()
        assert isinstance(tableKeys, list)
        self.tableKeys = tableKeys
        assert isinstance(additionalColumns, list)
        # Filter for additional columns not equal to the key column
        self.additionalColumns = additionalColumns
        self.sdqlrepr = "indexed"
        self.outputColumns = set(self.tableKeys).union(set(self.additionalColumns))
        self.outputRecord = SDQLpyRecordOutput(
            tableKeys,
            list(self.outputColumns) 
        )

class SDQLpyFilterNode(UnarySDQLpyNode):
    def __init__(self):
        super().__init__()
        self.outputRecord = SDQLpyRecordOutput(
            [],
            []
        )
        self.sdqlrepr = "filter"
        self.outputColumns = self.incomingColumns
        
    def set_output_record(self, incomingRecord):
        assert isinstance(incomingRecord, SDQLpyRecordOutput)
        # A filter node should only have keys, no values
        self.outputRecord = SDQLpyRecordOutput(
            incomingRecord.keys + incomingRecord.columns,
            []
        )

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
        'inner', 'rightsemijoin'
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
        
    def get_output_record(self):
        self.outputRecord = SDQLpyRecordOutput(
            list(self.outputColumns),
            list()
        )
        assert self.outputRecord != None
        return self.outputRecord
        
    def update_update_sum(self, newValue):
        self.is_update_sum = newValue
        
    def set_columns_variable(self):
        match self.joinType:
            case "inner":
                assert (self.left != None) and (self.right != None)
                self.outputColumns = self.left.outputColumns.union(self.right.outputColumns)
            case "rightsemijoin":
                self.outputColumns = self.right.outputColumns
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
            
        # Fix postJoinFilters
        if len(self.postJoinFilters) == 0:
            # Skip if no 'postJoinFilters', set as None
            self.postJoinFilters = None
        elif len(self.postJoinFilters) == 1:
            self.postJoinFilters = self.postJoinFilters[0]
        else:
            raise Exception(f"Length of postJoinFilters wasn't 1, we should join it with and/or. Examine context to decide which")
        
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
        
    def make_leftTableRef(self, unparser, lambda_index):
        leftTable, rightTable = self.getChildNames(unparser)
        # {'{leftKey}': {lambda_index}[0].{rightKey}}
        lr_pairs = []
        for idx, l_key in enumerate(self.leftKeys):
            lr_pairs.append(
                f"'{l_key}': {lambda_index}[0].{self.rightKeys[idx]}"
            )
        innerRecord = f"{{{', '.join(lr_pairs)}}}"
        return f"{leftTable}[record({innerRecord})]"

# Classes for SDQLpy Constructs
class SDQLpyNKeyJoin():
    def __init__(self, leftNode, leftKeys, rightKeys):
        self.leftNode = leftNode
        assert (len(leftKeys) > 1)
        self.leftKeys = leftKeys
        assert (len(rightKeys) > 1)
        self.rightKeys = rightKeys
        self.tableName = None
        
    def setTableName(self, value):
        assert self.tableName == None
        self.tableName = value

class SDQLpyRecordOutput():
    def __init__(self, keys, columns):
        assert isinstance(keys, list)
        self.keys = keys
        assert isinstance(columns, list)
        self.columns = columns
        self.third_wrap_counter = 0
        self.unique = False
        
    def setUnique(self, value):
        self.unique = value
        
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
        
        
    def generateSDQLpyTwoLambda(self, unparser, l_lambda_idx, r_lambda_idx, l_columns, r_columns):
        # Assign sourceNode to the Column Values
        for key in self.keys:
            setSourceNodeColumnValues(key, l_lambda_idx, l_columns, r_lambda_idx, r_columns)
        for col in self.columns:
            setSourceNodeColumnValues(col, l_lambda_idx, l_columns, r_lambda_idx, r_columns)
        
        output_content = self.generateSDQLpyContent(unparser)
        
        for key in self.keys:
            resetColumnValues(key)
        for col in self.columns:
            resetColumnValues(col)
            
        return output_content
    
    def generateSDQLpyOneLambda(self, unparser, lambda_idx, columns):
        # Assign sourceNode to the Column Values
        for key in self.keys:
            setSourceNodeColumnValues(key, lambda_idx, columns)
        for col in self.columns:
            setSourceNodeColumnValues(col, lambda_idx, columns)
        
        output_content = self.generateSDQLpyContent(unparser)
        
        for key in self.keys:
            resetColumnValues(key)
        for col in self.columns:
            resetColumnValues(col)
            
        return output_content
        
    def generateSDQLpyContent(self, unparser):
        output_content = []
        
        if self.keys == []:
            # If there are no keys, this should be an aggr output
            assert len(self.columns) == 1
            assert isinstance(self.columns[0], SumAggrOperator)
            output_content.append(
                f"{TAB}{unparser._UnparseSDQLpyTree__convert_expression_operator_to_sdqlpy(self.columns[0].child)}"
            )
            return output_content
        
        output_content.append(
            f"{{"
        )
        
        # Process: Keys
        keyContent = []
        for key in self.keys:
            expr = unparser._UnparseSDQLpyTree__convert_expression_operator_to_sdqlpy(key)
            # Set codeName if None
            if key.codeName == "":
                unparser.handleEmptyCodeName(key)
            
            assert key.codeName != ''
            keyContent.append(
                f'"{key.codeName}": {expr}'
            )
        keyFormatted = f"record({{{', '.join(keyContent)}}})"
        if self.unique == True:
            keyFormatted = f"unique({keyFormatted})"
        output_content.append(
            f"{TAB}{keyFormatted}:"
        )
        
        # Process: Columns
        if self.columns == []:
            # If no columns, then write True
            output_content.append(
                f"{TAB}{True}"
            )
        else:
            colContent = []
            for col in self.columns:
                if isinstance(col, (ColumnValue, CountAllOperator, SDQLpyThirdNodeWrapper, SumAggrOperator)):
                    expr = unparser._UnparseSDQLpyTree__convert_expression_operator_to_sdqlpy(col)
                else:
                    expr = unparser._UnparseSDQLpyTree__convert_expression_operator_to_sdqlpy(col.child)
                # Set codeName if None
                if col.codeName == "":
                    unparser.handleEmptyCodeName(col)
                
                assert col.codeName != ''
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
