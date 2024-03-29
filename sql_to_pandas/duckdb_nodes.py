

class DuckNode():
    def __init__(self):
        self.nodeID = None
        self.cardinality = None
    
    def addID(self, value):
        assert self.nodeID == None
        self.nodeID = value
    
    def setCardinality(self, card):
        assert self.cardinality == None and isinstance(card, int)
        self.cardinality = card
        
class LeafDuckNode(DuckNode):
    def __init__(self):
        super().__init__()
    
class UnaryDuckNode(DuckNode):
    def __init__(self):
        super().__init__()
        self.child = None
        
    def addChild(self, child):
        assert self.child == None
        self.child = child
        
class BinaryDuckNode(DuckNode):
    def __init__(self):
        super().__init__()
        self.left = None
        self.right = None
        
    def addLeft(self, left: DuckNode):
        assert self.left == None
        self.left = left
        
    def addRight(self, right: DuckNode):
        assert self.right == None
        self.right = right
        
class TernaryDuckNode(DuckNode):
    def __init__(self):
        super().__init__()
        self.left = None
        self.middle = None
        self.right = None
        
    def addLeft(self, left: DuckNode):
        assert self.left == None
        self.left = left
    
    def addMiddle(self, middle: DuckNode):
        assert self.middle == None
        self.middle = middle
        
    def addRight(self, right: DuckNode):
        assert self.right == None
        self.right = right

# Classes for Nodes

class DHashJoinNode(BinaryDuckNode):
    def __init__(self, joinMethod, joinType, joinCondition, leftKeys, rightKeys):
        super().__init__()
        self.joinMethod = joinMethod
        self.joinType = joinType
        self.joinCondition = joinCondition
        self.leftKeys = leftKeys
        self.rightKeys = rightKeys

class DSeqScan(LeafDuckNode):
    def __init__(self, tableName, tableColumns, condition):
        super().__init__()
        self.tableName = tableName
        self.tableColumns = tableColumns
        self.condition = condition
        
class DChunkScan(LeafDuckNode):
    def __init__(self):
        super().__init__()
        
class DSimpleAggregate(UnaryDuckNode):
    def __init__(self, aggregateOperations):
        super().__init__()
        self.aggregateOperations = aggregateOperations

class DDelimJoin(TernaryDuckNode):
    def __init__(self, joinType, leftKeys, rightKeys):
        super().__init__()
        self.joinType = joinType
        self.leftKeys = leftKeys
        self.rightKeys = rightKeys

class DHashGroupBy(UnaryDuckNode):
    def __init__(self, keys, aggregateOperations):
        super().__init__()
        self.keys = keys
        self.aggregateOperations = aggregateOperations

# DProjection

class DPiecewiseMergeJoin(BinaryDuckNode):
    def __init__(self, joinType, joinCondition, leftKeys, rightKeys):
        super().__init__()
        self.joinType = joinType
        self.joinCondition = joinCondition
        self.leftKeys = leftKeys
        self.rightKeys = rightKeys

class DFilter(UnaryDuckNode):
    def __init__(self, condition):
        super().__init__()
        self.condition = condition
