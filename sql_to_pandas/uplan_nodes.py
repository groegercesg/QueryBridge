from expression_operators import *
from uplan_helpers import *

class UniversalBaseNode():
    def __init__(self):
        self.nodeID = None
        self.cardinality = None
        
        self.primaryKey = None
        self.foreignKeys = set()
        self.waitingForeignKeys = dict()
        
    def setPrimary(self, inPrimary):
        assert isinstance(inPrimary, tuple)
        self.primaryKey = inPrimary
        
    def addForeign(self, inForeign):
        assert isinstance(inForeign, (set, dict))
        if isinstance(inForeign, set):
            self.foreignKeys.update(inForeign)
        else:
            for fkey in inForeign.keys():
                self.foreignKeys.add(fkey)
            self.waitingForeignKeys.update(inForeign)
    
    def addID(self, value):
        assert self.nodeID == None
        self.nodeID = value
    
    def setCardinality(self, card):
        assert self.cardinality == None and isinstance(card, int)
        self.cardinality = card
        
class LeafBaseNode(UniversalBaseNode):
    def __init__(self):
        super().__init__()
    
class UnaryBaseNode(UniversalBaseNode):
    def __init__(self):
        super().__init__()
        self.child = None
        
    def addChild(self, child):
        assert self.child == None
        self.child = child
        
class BinaryBaseNode(UniversalBaseNode):
    def __init__(self):
        super().__init__()
        self.left = None
        self.right = None
        
    def addLeft(self, left: UniversalBaseNode):
        assert self.left == None
        self.left = left
        
    def addRight(self, right: UniversalBaseNode):
        assert self.right == None
        self.right = right

# Classes for Nodes

class JoinNode(BinaryBaseNode):
    KNOWN_JOIN_METHODS = set(['hash', None, 'bnl'])
    KNOWN_JOIN_TYPES = set([
        'inner', 'outer', 
        'leftsemijoin', 'rightsemijoin',
        'leftantijoin', 'rightantijoin'
    ])
    def __init__(self, joinMethod, joinType, joinCondition, leftKeys, rightKeys):
        super().__init__()
        assert joinMethod in self.KNOWN_JOIN_METHODS, f"{joinMethod} is not in the known join methods"
        self.joinMethod = joinMethod
        assert joinType in self.KNOWN_JOIN_TYPES, f"{joinType} is not in the known join types"
        self.joinType = joinType
        self.joinCondition = joinCondition
        assert isinstance(leftKeys, list) and isinstance(rightKeys, list)
        self.leftKeys = leftKeys
        self.rightKeys = rightKeys
        
    def swapLeftAndRight(self):
        # Swap left and right
        new_right = self.left
        new_left = self.right
        
        self.left = new_left
        self.right = new_right
        
        # Swap the keys as well
        oldLeftKeys = self.leftKeys
        oldRightKeys = self.rightKeys
        
        self.leftKeys = oldRightKeys
        self.rightKeys = oldLeftKeys
        
    def resolveForeignKeys(self):
        toPopKeys = []
        assert len(self.flowColumns) > 0
        
        for key in self.waitingForeignKeys.keys():
            if len(list(filter(lambda x: x.codeName == self.waitingForeignKeys[key][0], self.flowColumns))) > 0:
                self.foreignKeys.add(
                    returnFromFlowColumns(
                        self.waitingForeignKeys[key][0], self.flowColumns
                    )
                )
                toPopKeys.append(key)
            
        for tpKey in toPopKeys:
            self.waitingForeignKeys.pop(tpKey)
        
class SortNode(UnaryBaseNode):
    def __init__(self, sortCriteria):
        super().__init__()
        assert isinstance(sortCriteria, list)
        assert all(isinstance(x, SortOperator) for x in sortCriteria)
        self.sortCriteria = sortCriteria
        
class LimitNode(UnaryBaseNode):
    def __init__(self, limitValue):
        super().__init__()
        assert isinstance(limitValue, int)
        self.limitValue = limitValue
        
class GroupNode(UnaryBaseNode):
    def __init__(self, keyExpressions, preAggregateExpressions, postAggregateOperations):
        super().__init__()
        self.keyExpressions = keyExpressions
        self.preAggregateExpressions = preAggregateExpressions
        self.postAggregateOperations = postAggregateOperations

class ScanNode(LeafBaseNode):
    def __init__(self, tableName, tableColumns, tableRestrictions, tableFilters):
        super().__init__()
        self.tableName = tableName
        self.tableColumns = tableColumns
        self.tableRestrictions = tableRestrictions
        self.tableFilters = tableFilters

class OutputNode(UnaryBaseNode):
    def __init__(self, outputColumns, outputNames):
        super().__init__()
        self.outputColumns = outputColumns
        self.outputNames = outputNames
        
class NewColumnNode(UnaryBaseNode):
    def __init__(self, valuesToCreate):
        super().__init__()
        self.values = valuesToCreate

class FilterNode(UnaryBaseNode):
    def __init__(self, condition):
        super().__init__()
        self.condition = condition

class RetrieveNode(UnaryBaseNode):
    def __init__(self, tableColumns, retrieveTargetID):
        super().__init__()
        self.tableColumns = tableColumns
        self.retrieveTargetID = retrieveTargetID
        