from expression_operators import *

class UniversalBaseNode():
    def __init__(self):
        pass
    
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
    def __init__(self, joinMethod, joinType, joinCondition):
        super().__init__()
        assert joinMethod in self.KNOWN_JOIN_METHODS, f"{joinMethod} is not in the known join methods"
        self.joinMethod = joinMethod
        assert joinType in self.KNOWN_JOIN_TYPES, f"{joinType} is not in the known join types"
        self.joinType = joinType
        self.joinCondition = joinCondition
        
class SortNode(UnaryBaseNode):
    def __init__(self, sortCriteria):
        super().__init__()
        assert isinstance(sortCriteria, list)
        assert all(isinstance(x, SortOperator) for x in sortCriteria)
        self.sortCriteria = sortCriteria
        
class GroupNode(UnaryBaseNode):
    def __init__(self, keyExpressions, preAggregateExpressions, postAggregateOperations):
        super().__init__()
        self.keyExpressions = keyExpressions
        self.preAggregateExpressions = preAggregateExpressions
        self.postAggregateOperations = postAggregateOperations

class ScanNode(UniversalBaseNode):
    def __init__(self, tableName, tableColumns, tableRestrictions):
        super().__init__()
        self.tableName = tableName
        self.tableColumns = tableColumns
        self.tableRestrictions = tableRestrictions

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
