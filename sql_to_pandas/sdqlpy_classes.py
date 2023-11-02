from collections import Counter

from universal_plan_nodes import *
from expression_operators import *

from sdqlpy_helpers import *
TAB = "    "

# Classes for the SDQLpy Tree
class SDQLpyBaseNode():
    def __init__(self):
        # The columns, as strs
        self.columns = set()
        self.nodeID = None
        self.sdqlrepr = None
        self.topNode = False
        
    def addID(self, value):
        assert self.nodeID == None
        self.nodeID = value
        
    def addToTableColumns(self, incomingColumns):
        localColumns = set()
        if isinstance(incomingColumns, list):
            assert all(isinstance(x, ExpressionBaseNode) for x in incomingColumns)
            localColumns.update(incomingColumns)
        elif isinstance(incomingColumns, ExpressionBaseNode):
            localColumns.add(incomingColumns)
        else:
            raise Exception(f"Unexpected format of incoming columns, {type(incomingColumns)}")
    
        self.columns.update(localColumns)
        
    def __updateTableColumns(self):
        if len(self.columns) > 0:
            pass
        else:
            # get from children
            if hasattr(self, "left"):
                #if self.left.columns:
                #    self.columns.update(self.left.columns)
                #else:
                self.columns.update(self.left.getTableColumnsInternal())
            if hasattr(self, "right"):
                self.columns.update(self.right.getTableColumnsInternal())
            if hasattr(self, "child"):
                self.columns.update(self.child.getTableColumnsInternal())
        
    def getTableColumnsString(self) -> set():
        self.__updateTableColumns()
        outputNames = set()
        for col in self.columns:
            if col.codeName != '':
                outputNames.add(col.codeName)
            else:
                outputNames.add(col.value)
        return outputNames

    def getTableColumnsInternal(self) -> set():
        self.__updateTableColumns()
        return self.columns
    
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
        self.filterContent = None
    
    def addFilterContent(self, filterContent):
        assert self.filterContent == None
        self.filterContent = filterContent
        
class UnarySDQLpyNode(PipelineBreakerNode):
    def __init__(self):
        super().__init__()
        self.child = None
        
    def addChild(self, child: SDQLpyBaseNode):
        assert self.child == None
        self.child = child
        
    def getChildName(self, unparser):
        childTableList = unparser.getChildTableNames(self)
        assert len(childTableList) == 1
        return childTableList[0]
        
class BinarySDQLpyNode(PipelineBreakerNode):
    def __init__(self):
        super().__init__()
        self.left = None
        self.right = None
        
    def addLeft(self, left: SDQLpyBaseNode):
        assert self.left == None
        self.left = left
        
    def addRight(self, right: SDQLpyBaseNode):
        assert self.right == None
        self.right = right
        
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
        self.tableColumns = tableColumns
        self.columns = set(self.tableColumns)
        
    def getTableName(self, unparser):
        return self.sdqlrepr

class SDQLpyJoinBuildNode(UnarySDQLpyNode):
    def __init__(self, tableName, tableKeys, additionalColumns):
        super().__init__()
        self.tableName = tableName
        assert isinstance(tableKeys, list) and len(tableKeys) == 1
        self.tableKey = tableKeys[0]
        assert isinstance(additionalColumns, list)
        self.additionalColumns = additionalColumns
        self.sdqlrepr = "indexed"
        self.columns = set([self.tableKey]).union(set(self.additionalColumns))

class SDQLpyAggrNode(UnarySDQLpyNode):
    def __init__(self, aggregateOperations):
        super().__init__()
        self.aggregateOperations = aggregateOperations
        self.sdqlrepr = "aggr"
        self.columns = set(self.aggregateOperations)
        
class SDQLpyConcatNode(UnarySDQLpyNode):
    def __init__(self, columns):
        super().__init__()
        self.sdqlrepr = "concat"
        self.columns = columns
        
class SDQLpyGroupNode(UnarySDQLpyNode):
    def __init__(self, keyExpressions, aggregateOperations):
        super().__init__()
        self.keyExpressions = keyExpressions
        self.aggregateOperations = aggregateOperations
        self.sdqlrepr = "group"
        self.columns = set(self.aggregateOperations).union(set(self.keyExpressions))
        
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
        
    def set_output_record(self, incomingRecord):
        assert isinstance(incomingRecord, SDQLpyRecordOutput)
        self.outputRecord = incomingRecord
        
    def set_columns_variable(self):
        match self.joinType:
            case "inner":
                assert (self.left != None) and (self.right != None)
                self.columns = self.left.columns.union(self.right.columns)
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
            if id(x.left) in [id(col) for col in self.left.columns]:
                self.leftKeys.append(x.left)
            elif id(x.left) in [id(col) for col in self.right.columns]:
                self.rightKeys.append(x.left)
            else:
                raise Exception(f"Couldn't find the x.left value in either of the left and right tables!")
            
            if id(x.right) in [id(col) for col in self.left.columns]:
                self.leftKeys.append(x.right)
            elif id(x.right) in [id(col) for col in self.right.columns]:
                self.rightKeys.append(x.right)
            else:
                raise Exception(f"Couldn't find the x.right value in either of the left and right tables!")
            
        assert len(self.leftKeys) == len(self.rightKeys) == len(self.joinCondition)

# Classes for SDQLpy Constructs
class SDQLpyRecordOutput():
    def __init__(self, keys, columns):
        assert isinstance(keys, list)
        self.keys = keys
        assert isinstance(columns, list)
        self.columns = columns
        
    def generateSDQLpyTwoLambda(self, l_lambda_idx, r_lambda_idx, l_columns, r_columns):
        # Assign sourceNode to the Column Values
        def setSourceNodeColumnValues(value, l_lambda_idx, r_lambda_idx, l_columns, r_columns):
            if isinstance(value, BinaryExpressionOperator):
                setSourceNodeColumnValues(value.left, l_lambda_idx, r_lambda_idx, l_columns, r_columns)
                setSourceNodeColumnValues(value.right, l_lambda_idx, r_lambda_idx, l_columns, r_columns)
            elif isinstance(value, UnaryExpressionOperator):
                setSourceNodeColumnValues(value.child, l_lambda_idx, r_lambda_idx, l_columns, r_columns)
            else:
                # A value node
                assert isinstance(value, LeafNode)
                pass
            
            match value:
                case ColumnValue():
                    decidedSourceValue = None
                    if value.codeName in l_columns:
                        decidedSourceValue = l_lambda_idx
                    elif value.codeName in r_columns:
                        decidedSourceValue = r_lambda_idx
                    else:
                        raise Exception(f"Value ({value.codeName}) wasn't in either left or right")
                    
                    assert decidedSourceValue != None
                    if value.sourceNode == None:
                        value.sourceNode = decidedSourceValue
                    else:
                        assert value.sourceNode == decidedSourceValue
           
        def resetColumnValues(value):
            if isinstance(value, BinaryExpressionOperator):
                resetColumnValues(value.left)
                resetColumnValues(value.right)
            elif isinstance(value, UnaryExpressionOperator):
                resetColumnValues(value.child)
            else:
                # A value node
                assert isinstance(value, LeafNode)
                pass
            
            match value:
                case ColumnValue():
                    if value.sourceNode != None:
                        value.sourceNode = None
                  
                    
        l_columns_str = set([x.codeName for x in l_columns])
        r_columns_str = set([x.codeName for x in r_columns])
        
        # Assert none are empty string
        assert not any([True for x in l_columns_str if x == ""])
        assert not any([True for x in r_columns_str if x == ""])
        
        for key in self.keys:
            setSourceNodeColumnValues(key, l_lambda_idx, r_lambda_idx, l_columns_str, r_columns_str)
        for col in self.columns:
            setSourceNodeColumnValues(col, l_lambda_idx, r_lambda_idx, l_columns_str, r_columns_str)
        
        output_content = self.generateSDQLpy("")
        
        for key in self.keys:
            resetColumnValues(key)
        for col in self.columns:
            resetColumnValues(col)
            
        return output_content
        
    def generateSDQLpy(self, lambda_index):
        output_content = []
        
        output_content.append(
            f"{{"
        )
        
        # Process: Keys
        if len(self.keys) == 1:
            keyFormatted = convert_expression_operator_to_sdqlpy(self.keys[0], lambda_index)
        else:
            keyContent = []
            for key in self.keys:
                expr = convert_expression_operator_to_sdqlpy(key, lambda_index)
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
            if isinstance(col, ColumnValue):
                expr = convert_expression_operator_to_sdqlpy(col, lambda_index)
            else:
                expr = convert_expression_operator_to_sdqlpy(col.child, lambda_index)
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
