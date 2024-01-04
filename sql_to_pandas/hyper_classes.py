# Classes for HyperDB
SUPPORTED_HYPER_OPERATORS = {'leftsemijoin', 'map', 'sort', 'join', 'groupby', 'leftantijoin', 'groupjoin', 'rightsemijoin', 'executiontarget', 'select', 'rightantijoin', 'tablescan', 'explicitscan', 'leftsinglejoin'}

class HyperBaseNode():
    def __init__(self):
        self.child = None
        self.hyperID = None
        self.cardinality = None
    
    def addChild(self, child):
        assert self.child == None
        self.child = child
        
    def setHyperID(self, id):
        assert self.hyperID == None
        self.hyperID = id
        
    def setCardinality(self, card):
        if isinstance(card, float):
            card = int(card)
        
        assert self.cardinality == None and isinstance(card, int)
        self.cardinality = card
        
class mapNode(HyperBaseNode):
    def __init__(self, mapValues):
        super().__init__()
        self.mapValues = mapValues
        
class sortNode(HyperBaseNode):
    def __init__(self, sortCriteria, limitValue):
        super().__init__()
        self.sortCriteria = sortCriteria
        self.limitValue = limitValue
        
class groupbyNode(HyperBaseNode):
    def __init__(self, keyExpressions, aggregateExpressions, aggregateOperations):
        super().__init__()
        self.keyExpressions = keyExpressions
        self.aggregateExpressions = aggregateExpressions
        self.aggregateOperations = aggregateOperations

class executiontargetNode(HyperBaseNode):
    def __init__(self, output_columns, output_names):
        super().__init__()
        self.output_columns = output_columns
        self.output_names = output_names
        
class explicitscanNode(HyperBaseNode):
    def __init__(self, mapping):
        super().__init__()
        self.mapping = mapping
        self.isRetrieve = False
        self.table_columns = []
        
    def setRetrieve(self, value, retrieveOperatorID):
        self.isRetrieve = value
        self.targetOperator = retrieveOperatorID

class tablescanNode(HyperBaseNode):
    def __init__(self, table_name, table_columns, tableRestrictions, tableFilters):
        super().__init__()
        self.table_name = table_name
        self.table_columns = table_columns
        self.tableRestrictions = tableRestrictions
        self.tableFilters = tableFilters

class selectNode(HyperBaseNode):
    def __init__(self, selectCondition):
        super().__init__()
        self.selectCondition = selectCondition
  
class JoinBaseNode(HyperBaseNode):
    def __init__(self, joinType):
        super().__init__()
        self.isJoinNode = True
        self.left = None
        self.right = None
        self.joinType = joinType
        self.leftKeys = []
        self.rightKeys = []
        
    def setLeftKeys(self, incomingKeys: list) -> None:
        assert self.leftKeys == []
        self.leftKeys = incomingKeys
        
    def setRightKeys(self, incomingKeys: list) -> None:
        assert self.rightKeys == []
        self.rightKeys = incomingKeys
    
    def addLeft(self, left: HyperBaseNode):
        assert self.left == None
        self.left = left
        
    def addRight(self, right: HyperBaseNode):
        assert self.right == None
        self.right = right

class groupjoinNode(JoinBaseNode):
    def __init__(self, joinType, leftKey, rightKey, leftExpressions, leftAggregates, rightExpressions, rightAggregates):
        super().__init__("groupjoin")
        self.joinType = joinType
        self.leftKey = leftKey
        self.rightKey = rightKey
        self.leftExpressions = leftExpressions
        self.leftAggregates = leftAggregates
        self.rightExpressions = rightExpressions
        self.rightAggregates = rightAggregates
        
        # To be added to later
        self.groupKeys = []

class joinNode(JoinBaseNode):
    def __init__(self, joinType, joinMethod, joinCondition):
        super().__init__(joinType)
        self.joinMethod = joinMethod
        self.joinCondition = joinCondition
