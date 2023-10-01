class ExpressionBaseNode():
    def __init__(self):
        pass

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

# Values
class ValueNode(ExpressionBaseNode):
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

# Unary Operators

# Binary Operators
class SubOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
class AddOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()

class MulOperator(BinaryExpressionOperator):
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
class SumAggrOperator(UnaryExpressionOperator):
    def __init__(self):
        super().__init__()

class AvgAggrOperator(UnaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
class CountAggrOperator(UnaryExpressionOperator):
    def __init__(self):
        super().__init__()
        self.countAll = False
        
    def setCountAll(self):
        self.countAll = True

class InSetOperator(UnaryExpressionOperator):
    def __init__(self):
        super().__init__()
        self.set = []
        
    def addToSet(self, toAdd: ExpressionBaseNode):
        self.set.append(toAdd)

class CaseInstance():
    def __init__(self):
        self.subCases = []
        self.outputValue = None
        
    def setOutputValue(self, outputValue):
        assert self.outputValue is None
        self.outputValue = outputValue
        
    def addToCaseInstance(self, inExpr):
        self.subCases.append(inExpr)
        
class CaseOperator(UnaryExpressionOperator):
    def __init__(self):
        super().__init__()
        self.caseInstances = []
        self.elseExpr = []
        
    def addToCases(self, toAdd: CaseInstance):
        self.caseInstances.append(toAdd)
        
    def addElse(self, elseExpr):
        assert self.elseExpr == []
        self.elseExpr = elseExpr
        

# Sort Operator
class SortOperator(ExpressionBaseNode):
    def __init__(self, value, descending):
        super().__init__()
        self.value = value
        assert descending == True or descending == False
        self.descending = descending
        