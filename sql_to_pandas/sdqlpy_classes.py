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
    
    def getTableName(self, unparser):
        assert self.sdqlrepr != None
        if self.topNode == True:
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
    def __init__(self, tableName):
        super().__init__()
        self.tableName = tableName
        self.sdqlrepr = tableName
        
    def getTableName(self, unparser):
        return self.sdqlrepr

class SDQLpyAggrNode(UnarySDQLpyNode):
    def __init__(self, preAggregateExpressions, postAggregateOperations):
        super().__init__()
        self.preAggregateExpressions = preAggregateExpressions
        self.postAggregateOperations = postAggregateOperations
        self.sdqlrepr = "aggr"
