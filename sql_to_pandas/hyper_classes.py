# Classes for HyperDB
# {'leftsemijoin', 'map', 'sort', 'join', 'groupby', 'leftantijoin', 'groupjoin', 'rightsemijoin', 'executiontarget', 'select', 'rightantijoin', 'tablescan'}

class BaseNode():
    def __init__(self):
        self.child = None
    
    def addChild(self, child):
        assert self.child == None
        self.child = child
        
class mapNode(BaseNode):
    def __init__(self, mapValues):
        super().__init__()
        self.mapValues = mapValues
        
class sortNode(BaseNode):
    def __init__(self, sortCriteria):
        super().__init__()
        self.sortCriteria = sortCriteria
        
class groupbyNode(BaseNode):
    def __init__(self, keyExpressions, aggregateExpressions, aggregateOperations):
        super().__init__()
        self.keyExpressions = keyExpressions
        self.aggregateExpressions = aggregateExpressions
        self.aggregateOperations = aggregateOperations

class executiontargetNode(BaseNode):
    def __init__(self, output_columns, output_names):
        super().__init__()
        self.output_columns = output_columns
        self.output_names = output_names

class tablescanNode(BaseNode):
    def __init__(self, table_name, table_columns, tableRestrictions):
        super().__init__()
        self.table_name = table_name
        self.table_columns = table_columns
        self.tableRestrictions = tableRestrictions

class selectNode(BaseNode):
    def __init__(self, selectCondition):
        super().__init__()
        self.selectCondition = selectCondition
  
class JoinNode(BaseNode):
    def __init__(self):
        super().__init__()
        self.isJoinNode = True
        self.left = None
        self.right = None
    
    def addLeft(self, left: BaseNode):
        assert self.left == None
        self.left = left
        
    def addRight(self, right: BaseNode):
        assert self.right == None
        self.right = right

class groupjoinNode(JoinNode):
    def __init__(self, joinType, leftKey, rightKey, leftExpressions, leftAggregates, rightExpressions, rightAggregates):
        super().__init__()
        self.joinType = joinType
        self.leftKey = leftKey
        self.rightKey = rightKey
        self.leftExpressions = leftExpressions
        self.leftAggregates = leftAggregates
        self.rightExpressions = rightExpressions
        self.rightAggregates = rightAggregates

class joinNode(JoinNode):
    def __init__(self, joinMethod, joinCondition):
        super().__init__()
        self.joinMethod = joinMethod
        self.joinCondition = joinCondition

class leftsemijoinNode(JoinNode):
    def __init__(self, joinMethod, joinCondition):
        super().__init__()
        self.joinMethod = joinMethod
        self.joinCondition = joinCondition

class leftantijoinNode(JoinNode):
    def __init__(self, joinMethod, joinCondition):
        super().__init__()
        self.joinMethod = joinMethod
        self.joinCondition = joinCondition

class rightsemijoinNode(JoinNode):
    def __init__(self, joinMethod, joinCondition):
        super().__init__()
        self.joinMethod = joinMethod
        self.joinCondition = joinCondition 

class rightantijoinNode(JoinNode):
    def __init__(self, joinMethod, joinCondition):
        super().__init__()
        self.joinMethod = joinMethod
        self.joinCondition = joinCondition 
