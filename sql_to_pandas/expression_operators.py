import sys

class ExpressionBaseNode():
    def __init__(self):
        self.codeName = ""
        self.created = False
        self.no_source = False
        
    def setCreated(self):
        self.created = True
        
    def setCodeName(self, inName):
        assert self.codeName == ""
        self.codeName = inName
        
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.codeName == other.codeName
        else:
            return NotImplemented
    
    def __hash__(self):
        return hash(self.codeName)
    
    def __str__(self):
        return self.codeName

class UnaryExpressionOperator(ExpressionBaseNode):
    def __init__(self):
        super().__init__()
        self.child = None
    
    def addChild(self, child):
        assert self.child == None
        self.child = child
        
class BinaryExpressionOperator(ExpressionBaseNode):
    def __init__(self):
        super().__init__()
        self.left = None
        self.right = None
        
    def addLeft(self, left: ExpressionBaseNode):
        assert self.left == None
        self.left = left
        
    def addRight(self, right: ExpressionBaseNode):
        assert self.right == None
        self.right = right

class LeafNode(ExpressionBaseNode):
    # Node has no children
    def __init__(self):
        super().__init__()

# Values
class ValueNode(LeafNode):
    def __init__(self, value):
        super().__init__()
        self.value = value
        
    def __eq__(self, other):
        if not isinstance(other, ValueNode):
            # don't attempt to compare against unrelated objects
            return NotImplemented
        
        return isinstance(other, self.__class__) and (self.value == other.value) and (self.codeName == other.codeName)
    
    def __hash__(self):
        return hash(self.value)
    
    def __str__(self):
        return self.value

class ColumnValue(ValueNode):
    SUPPORTED_TYPES = ["Integer", "Double", "Varchar", "Date", "Char"]
    def __init__(self, value, incomingType):
        super().__init__(value)
        self.essential = False
        self.codeName = value
        self.handleTypes(incomingType)
        self.sourceNode = None
    
    def handleTypes(self, incomingType):
        # If the type is already good, we skip
        if incomingType in self.SUPPORTED_TYPES:
            self.type = incomingType
            self.typeSize = None
            return
        
        detectedType = None
        if isinstance(incomingType, list):
            if len(incomingType) == 1:
                detectedType = incomingType[0]
            elif len(incomingType) == 2:
                detectedType = incomingType[0]
                # Store type size
                self.typeSize = incomingType[1]
            else:
                raise Exception(f"Unknown list type format, with {len(incomingType)} elements")
        else:
            raise Exception(f"Unexpected type format: {incomingType}")
        
        assert detectedType in self.SUPPORTED_TYPES
        self.type = detectedType
    
    def setEssential(self, target):
        self.essential = target

class ConstantValue(ValueNode):
    SUPPORTED_TYPES = ["Integer", "Datetime", "Float", "String", "Bool"]
    def __init__(self, value, type):
        super().__init__(value)
        assert type in self.SUPPORTED_TYPES
        self.type = type
        self.forceInteger = False
        
    def __eq__(self, other): 
        if not isinstance(other, ConstantValue):
            # don't attempt to compare against unrelated objects
            return NotImplemented

        return (isinstance(other, self.__class__) and self.value == other.value 
                and self.type == other.type and self.codeName == other.codeName)
        
    def setForceInteger(self, value):
        self.forceInteger = value

# Unary Operators
class NotOperator(UnaryExpressionOperator):
    def __init__(self):
        super().__init__()

class ExtractYearOperator(UnaryExpressionOperator):
    def __init__(self):
        super().__init__()

# Binary Operators
class OrOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()

class AndOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()

class SubOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        self.type = "Double"
        
class AddOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        self.type = "Double"

class MulOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        self.type = "Double"
        
class DivOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        self.type = "Double"
        
class NotEqualsOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
       
class EqualsOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
class LessThanOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
class LessThanEqOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
class GreaterThanOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
class GreaterThanEqOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
class IntervalNotionOperator(BinaryExpressionOperator):
    """
    Inequality  | Interval Notion
    a <= x <= b | [a, b]
    a <  x <  b | (a, b)
    a <= x <  b | [a, b)
    a <  x <= b | (a, b]
    """
    SUPPORTED_MODES = ["[]", "()", "[)", "(]"]
    def __init__(self, mode, value):
        super().__init__()
        assert mode in self.SUPPORTED_MODES
        self.mode = mode
        # The item that we want to compare to left and right
        self.value = value

# Aggregation Operators
class AggregationOperators(UnaryExpressionOperator):
    def __init__(self):
        super().__init__()
        self.sourceNode = None
        
class MaxAggrOperator(AggregationOperators):
    def __init__(self):
        super().__init__()

class SumAggrOperator(AggregationOperators):
    def __init__(self):
        super().__init__()
        
class MinAggrOperator(AggregationOperators):
    def __init__(self):
        super().__init__()

class AvgAggrOperator(AggregationOperators):
    def __init__(self):
        super().__init__()
        
class CountAggrOperator(UnaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
class CountDistinctAggrOperator(UnaryExpressionOperator):
    def __init__(self):
        super().__init__()

class CountAllOperator(LeafNode):
    def __init__(self):
        super().__init__()

class InSetOperator(UnaryExpressionOperator):
    def __init__(self):
        super().__init__()
        self.set = []
        
    def addToSet(self, toAdd: ExpressionBaseNode):
        self.set.append(toAdd)

class CaseInstance():
    def __init__(self):
        self.case = None
        self.outputValue = None
        
    def setOutputValue(self, outputValue):
        assert self.outputValue is None
        self.outputValue = outputValue
        
    def setCase(self, inExpr):
        assert self.case is None
        self.case = inExpr
        
class CaseOperator(LeafNode):
    def __init__(self):
        super().__init__()
        self.caseInstances = []
        self.elseExpr = []
        self.type = "Integer"
        
    def addToCase(self, toAdd: CaseInstance):
        self.caseInstances.append(toAdd)
        
    def addElse(self, elseExpr):
        assert self.elseExpr == []
        self.elseExpr = elseExpr

class SubstringOperator(LeafNode):
    def __init__(self, value, startPosition, length):
        super().__init__()
        self.value = value
        self.startPosition = startPosition
        self.length = length
   
class LikeOperator(LeafNode):
    def __init__(self, value, comparator):
        super().__init__()
        self.value = value
        self.comparator = comparator
    
class LookupOperator(LeafNode):
    def __init__(self, values, comparisons, modes):
        super().__init__()
        self.values = values
        self.comparisons = comparisons
        self.modes = modes

# Sort Operator
class SortOperator(LeafNode):
    def __init__(self, value, descending):
        super().__init__()
        self.value = value
        assert descending == True or descending == False
        self.descending = descending
        
### Functions for handling these

def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)

def join_statements_with_operator(statements: list[ExpressionBaseNode], join_operator: BinaryExpressionOperator) -> ExpressionBaseNode:
    assert len(statements) >= 2
    assert join_operator in ["OrOperator", "AndOperator"]
    current_op = str_to_class(join_operator)()
    current_node = current_op
    
    while len(statements) > 2:
        current_node.addLeft(statements.pop())
        # Decide operator to add
        current_node.addRight(str_to_class(join_operator)())
        current_node = current_node.right
    current_node.addLeft(statements.pop())
    current_node.addRight(statements.pop())
    return current_op

### SDQL specific ones

class SDQLpyFirstIndex(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
class SDQLpyStartsWith(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
class SDQLpyEndsWith(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()

class SDQLpyLambdaReference(ValueNode):
    def __init__(self, value):
        super().__init__(value)

class SDQLpyThirdNodeWrapper(LeafNode):
    def __init__(self, col, third_node, target_key):
        super().__init__()
        self.col = col
        self.third_node = third_node
        assert isinstance(target_key, ColumnValue)
        self.target_key = target_key
        self.sourceNode = None
        self.codeName = col.codeName
        
    def set_sourceValue(self, sourceValue):
        assert self.sourceNode == None
        self.sourceNode = sourceValue
        
class SDQLpyColumnValue(LeafNode):
    def __init__(self):
        super().__init__()
        