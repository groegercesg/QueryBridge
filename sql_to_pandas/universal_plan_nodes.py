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
    def __init__(self):
        super().__init__()
        
class SortNode(UnaryBaseNode):
    def __init__(self):
        super().__init__()
        
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
        
class FilterNode(UnaryBaseNode):
    def __init__(self):
        super().__init__()
