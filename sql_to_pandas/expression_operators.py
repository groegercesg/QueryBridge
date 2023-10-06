class ExpressionBaseNode():
    def __init__(self):
        self.codeName = ""
        
    def setCodeName(self, inName):
        assert self.codeName == ""
        self.codeName = inName

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
    def __init__(self,):
        super().__init__()

# Values
class ValueNode(LeafNode):
    def __init__(self, value):
        super().__init__()
        self.value = value

class ColumnValue(ValueNode):
    def __init__(self, value):
        super().__init__(value)

class ConstantValue(ValueNode):
    SUPPORTED_TYPES = ["Integer", "Datetime", "Float", "String"]
    def __init__(self, value, type):
        super().__init__(value)
        assert type in self.SUPPORTED_TYPES
        self.type = type
        
    def __eq__(self, other): 
        if not isinstance(other, ConstantValue):
            # don't attempt to compare against unrelated objects
            return NotImplemented

        return self.value == other.value and self.type == other.type

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
        
class AddOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()

class MulOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
class DivOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
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
        self.caseInstances = []
        self.elseExpr = []
        
    def addToCase(self, toAdd: CaseInstance):
        self.caseInstances.append(toAdd)
        
    def addElse(self, elseExpr):
        assert self.elseExpr == []
        self.elseExpr = elseExpr

class SubstringOperator(LeafNode):
    def __init__(self, value, startPosition, length):
        self.value = value
        self.startPosition = startPosition
        self.length = length
   
class LikeOperator(LeafNode):
    def __init__(self, value, comparator):
        self.value = value
        self.comparator = comparator
    
class LookupOperator(LeafNode):
    def __init__(self, values, comparisons, modes):
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
        