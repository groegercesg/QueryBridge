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
    def __init__(self, value):
        super().__init__(value)

# Unary Operators

# Binary Operators
class SubOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()

class MulOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
class EqualsOperator(BinaryExpressionOperator):
    def __init__(self):
        super().__init__()
        
# Aggregation Operators
class SumAggrOperator(UnaryExpressionOperator):
    def __init__(self):
        super().__init__()

# Sort Operator
class SortOperator(ExpressionBaseNode):
    def __init__(self, value, descending):
        super().__init__()
        self.value = value
        assert descending == True or descending == False
        self.descending = descending
        