from collections import Counter

from universal_plan_nodes import *
from expression_operators import *

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

class SDQLpyJoinBuildNode(LeafSDQLpyNode):
    def __init__(self, tableName, tableKeys, additionalColumns, filterCondition = None):
        super().__init__()
        self.tableName = tableName
        assert isinstance(tableKeys, list) and len(tableKeys) == 1
        self.tableKey = tableKeys[0]
        assert isinstance(additionalColumns, list)
        self.additionalColumns = additionalColumns
        self.filterCondition = filterCondition
        self.sdqlrepr = "indexed"
        self.columns = set([self.tableKey]).union(set(self.additionalColumns))

class SDQLpyAggrNode(UnarySDQLpyNode):
    def __init__(self, aggregateOperations):
        super().__init__()
        self.aggregateOperations = aggregateOperations
        self.sdqlrepr = "aggr"
        self.columns = set(self.aggregateOperations)
        
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
        self.sdqlrepr = "join"
        
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
