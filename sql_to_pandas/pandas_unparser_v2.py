from collections import defaultdict, Counter
import copy
import random

from universal_plan_nodes import *
from expression_operators import *

TAB = "    "

def flatten(l):
    return [item for sublist in l for item in sublist]

def convert_aggregation_tree_to_pandas(aggr_tree: ExpressionBaseNode, dataFrameName: str) -> str:
    # Visit Children
    if isinstance(aggr_tree, AggregationOperators):
        childNode = convert_aggregation_tree_to_pandas(aggr_tree.child, dataFrameName)
    else:
        # Should have a completed Code Name
        assert aggr_tree.codeName != ""
        return f"{dataFrameName}.{aggr_tree.codeName}"
        
    if aggr_tree.codeName != "":
        return f"{dataFrameName}.{aggr_tree.codeName}"
    
    expression_output = ""
    match aggr_tree:
        case SumAggrOperator():
            expression_output = f"{childNode}.sum()"
        case AvgAggrOperator():
            expression_output = f"{childNode}.mean()"
        case MaxAggrOperator():
            expression_output = f"{childNode}.max()"
        case _:
            raise Exception(f"Unknown Aggregation Operator: {aggr_tree}")
        
    return expression_output

def convert_expression_operator_to_aggr(expr_tree: ExpressionBaseNode) -> str:
    expression_output = ""
    match expr_tree:
        case SumAggrOperator():
            expression_output = "sum"
        case MinAggrOperator():
            expression_output = "min"
        case AvgAggrOperator():
            expression_output = "mean"
        case CountAggrOperator() | CountAllOperator():
            expression_output = "count"
        case AggregationOperators():
            raise Exception(f"We have an aggregation operator, but don't have a case for it: {expr_tree}")
    
    return expression_output

def convert_expression_operator_to_column_name(expr_tree: ExpressionBaseNode):
    # Visit Children
    expression_output = []
    if isinstance(expr_tree, BinaryExpressionOperator):
        expression_output.extend(convert_expression_operator_to_column_name(expr_tree.left))
        expression_output.extend(convert_expression_operator_to_column_name(expr_tree.right))
    elif isinstance(expr_tree, UnaryExpressionOperator):
        expression_output.extend(convert_expression_operator_to_column_name(expr_tree.child))
    else:
        # A value node
        assert isinstance(expr_tree, LeafNode)
        pass
    
    match expr_tree:
        case ColumnValue():
            expression_output.append(f"{expr_tree.value}")
        case SumAggrOperator():
            expression_output.append("sum")
        case MinAggrOperator():
            expression_output.append("min")
        case AvgAggrOperator():
            expression_output.append("mean")
        case CountAggrOperator() | CountAllOperator():
            expression_output.append("count")
        case CaseOperator():
            expression_output.append("case")
        case SubstringOperator():
            expression_output.append("substr")
        case MaxAggrOperator():
            expression_output.append("max")
        case AggregationOperators():
            raise Exception(f"We have an aggregation operator, but don't have a case for it: {expr_tree}")
    
    return expression_output

def convert_expression_operator_to_pandas(expr_tree: ExpressionBaseNode, dataFrameName: str, columnName: str = None) -> str:
    def handleConstantValue(expr: ConstantValue):
        if expr.type == "Datetime":
            return f"'{expr.value.strftime('%Y-%m-%d')}'"
        if expr.type == "String":
            return f"'{expr.value}'"
        else:
            return expr.value
        
    def handleIntervalNotion(expr: IntervalNotionOperator, leftNode, rightNode, dataFrameName):
        pandasValue = convert_expression_operator_to_pandas(expr.value, dataFrameName)
        inclusiveOption = None
        match expr.mode:
            case "[]":
                inclusiveOption = "both"
            case "()":
                inclusiveOption = "neither"
            case "[)":
                inclusiveOption = "left"
            case "(]":
                inclusiveOption = "right"
            case _:
                raise Exception(f"Unknown Internal Notion operator: {expr.mode}")
        
        assert inclusiveOption in ["both", "neither", "left", "right"]
        convertedExpression = f"{pandasValue}.between({leftNode}, {rightNode}, inclusive='{inclusiveOption}')"
        return convertedExpression
    
    def handleInSetOperator(expr: InSetOperator, childNode: str) -> str:
        listElements = []
        for element in expr.set:
            assert isinstance(element, ConstantValue)
            listElements.append(element.value)
        return f"{childNode}.isin({listElements})"
    
    def handleCaseOperator(expr: CaseOperator, dataFrameName: str, columnName: str) -> list[str]:
        assert columnName != None
        # Use '.loc' for the case expression
        # Return a list of the lines required
        caseLines = []
        # Default
        defaultValue = convert_expression_operator_to_pandas(expr.elseExpr, dataFrameName)
        caseLines.append(
            f"{dataFrameName}['{columnName}'] = {defaultValue}"
        )
        for caseInst in expr.caseInstances:
            caseValue = convert_expression_operator_to_pandas(caseInst.case, dataFrameName)
            outputValue = convert_expression_operator_to_pandas(caseInst.outputValue, dataFrameName)
            caseLines.append(
                f"{dataFrameName}.loc[({caseValue}), '{columnName}'] = {outputValue}"
            )
        return caseLines
    
    def handleLikeOperator(expr: LikeOperator, dataFrameName: str) -> str:
        _special_regex_chars = {
            ch : '\\'+ch
            for ch in '.^$*+?{}[]|()\\'
        }

        def sql_like_fragment_to_regex_string(fragment):
            safe_fragment = ''.join([
                _special_regex_chars.get(ch, ch)
                for ch in fragment
            ])
            return '^' + safe_fragment.replace('%', '.*?').replace('_', '.') + '$'
        
        targetColumn = convert_expression_operator_to_pandas(expr.value, dataFrameName)
        assert isinstance(expr.comparator, ConstantValue)
        regex_cmd = sql_like_fragment_to_regex_string(expr.comparator.value)
        
        # Assemble the line
        return f"{targetColumn}.str.contains('{regex_cmd}', regex=True)"
    
    def handleLookupOperator(expr: LookupOperator, dataFrameName: str) -> str:
        assert len(expr.comparisons) / len(expr.values) % 2 == 0
        assert all(isinstance(x, EqualsOperator) for x in expr.modes)
        leftEquals = []
        for i in range(len(expr.values)):
            newEq = EqualsOperator()
            newEq.addLeft(expr.values[i])
            newEq.addRight(expr.comparisons[i])
            leftEquals.append(newEq)
        rightEquals = []
        for i in range(len(expr.values)):
            newEq = EqualsOperator()
            newEq.addLeft(expr.values[i])
            newEq.addRight(expr.comparisons[i+len(expr.values)])
            rightEquals.append(newEq)
        leftAnd = AndOperator()
        leftAnd.addLeft(leftEquals[0])
        leftAnd.addRight(leftEquals[1])
        rightAnd = AndOperator()
        rightAnd.addLeft(rightEquals[0])
        rightAnd.addRight(rightEquals[1])
        lookup = OrOperator()
        lookup.addLeft(leftAnd)
        lookup.addRight(rightAnd)
        return convert_expression_operator_to_pandas(lookup, dataFrameName)
    
    def handleSubstringOperator(expr_tree: SubstringOperator, dataFrameName: str) -> str:
        column = convert_expression_operator_to_pandas(expr_tree.value, dataFrameName)
        startPos = convert_expression_operator_to_pandas(expr_tree.startPosition, dataFrameName)
        endPos = convert_expression_operator_to_pandas(expr_tree.length, dataFrameName)
        return f"{column}.str.slice({startPos}, {startPos + endPos})"
    
    # Visit Children
    if isinstance(expr_tree, BinaryExpressionOperator):
        leftNode = convert_expression_operator_to_pandas(expr_tree.left, dataFrameName)
        rightNode = convert_expression_operator_to_pandas(expr_tree.right, dataFrameName)
    elif isinstance(expr_tree, UnaryExpressionOperator):
        childNode = convert_expression_operator_to_pandas(expr_tree.child, dataFrameName)
    else:
        # A value node
        assert isinstance(expr_tree, LeafNode)
        pass
    
    # If an expression has a non-blank codeName, then
    # it has already been created and so should be done
    # again.
    if expr_tree.codeName != '':
        return f"{dataFrameName}.{expr_tree.codeName}"
    
    expression_output = None
    match expr_tree:
        case ColumnValue():
            expression_output = f"{dataFrameName}.{expr_tree.value}"
        case ConstantValue():
            expression_output = handleConstantValue(expr_tree)
        case AndOperator():
            expression_output = f"{leftNode} & {rightNode}"
        case MulOperator():
            expression_output = f"{leftNode} * {rightNode}"
        case LessThanOperator():
            expression_output = f"({leftNode} < {rightNode})"
        case LessThanEqOperator():
            expression_output = f"({leftNode} <= {rightNode})"
        case EqualsOperator():
            expression_output = f"({leftNode} == {rightNode})"
        case NotEqualsOperator():
            expression_output = f"({leftNode} != {rightNode})"
        case GreaterThanOperator():
            expression_output = f"({leftNode} > {rightNode})"
        case IntervalNotionOperator():
            expression_output = handleIntervalNotion(expr_tree, leftNode, rightNode, dataFrameName)
        case SubOperator():
            expression_output = f"({leftNode} - {rightNode})"
        case AddOperator():
            expression_output = f"({leftNode} + {rightNode})"
        case DivOperator():
            expression_output = f"({leftNode} / {rightNode})"
        case InSetOperator():
            expression_output = handleInSetOperator(expr_tree, childNode)
        case OrOperator():
            expression_output = f"{leftNode} | {rightNode}"
        case CaseOperator():
            expression_output = handleCaseOperator(expr_tree, dataFrameName, columnName)
        case LikeOperator():
            expression_output = handleLikeOperator(expr_tree, dataFrameName)
        case NotOperator():
            expression_output = f"({childNode} == False)"
        case ExtractYearOperator():
            expression_output = f"{childNode}.dt.year"
        case LookupOperator():
            expression_output = handleLookupOperator(expr_tree, dataFrameName)
        case SubstringOperator():
            expression_output = handleSubstringOperator(expr_tree, dataFrameName)
        case _: 
            raise Exception(f"Unrecognised expression operator: {expr_tree}")
    
    return expression_output
    

def convert_universal_to_pandas(op_tree: UniversalBaseNode):
    # Visit Children
    if isinstance(op_tree, BinaryBaseNode):
        leftNode = convert_universal_to_pandas(op_tree.left)
        rightNode = convert_universal_to_pandas(op_tree.right)
    elif isinstance(op_tree, UnaryBaseNode):
        childNode = convert_universal_to_pandas(op_tree.child)
    else:
        # A leaf node
        pass
    
    # Create a 'new_op_tree' from an existing 'op_tree'
    match op_tree:
        case ScanNode():
            new_op_tree = PandasScanNode(
                op_tree.tableName,
                op_tree.tableColumns,
                op_tree.tableRestrictions
            )
            # Handle scan, filters
            if op_tree.tableFilters != []:
                scan_node = new_op_tree
                # We need to insert a filter node above this one
                new_op_tree = PandasFilterNode(
                    op_tree.tableFilters
                )
                new_op_tree.addChild(scan_node)
        case GroupNode():
            if op_tree.keyExpressions == []:
                new_op_tree = PandasAggrNode(
                    op_tree.preAggregateExpressions,
                    op_tree.postAggregateOperations
                )
            else:
                new_op_tree = PandasGroupNode(
                    op_tree.keyExpressions,
                    op_tree.preAggregateExpressions,
                    op_tree.postAggregateOperations
                )
        case OutputNode():
            new_op_tree = PandasOutputNode(
                op_tree.outputColumns,
                op_tree.outputNames
            )
        case JoinNode():
            updateJoinMethod = op_tree.joinMethod
            if updateJoinMethod == None:
                updateJoinMethod = "merge"
            
            new_op_tree = PandasJoinNode(
                updateJoinMethod,
                op_tree.joinType,
                op_tree.joinCondition
            )
            
            # Handle join, filters
            if new_op_tree.postJoinFilters != []:
                assert len(new_op_tree.postJoinFilters) == 1
                join_node = new_op_tree
                # We need to insert a filter node above this one
                new_op_tree = PandasFilterNode(
                    join_node.postJoinFilters[0]
                )
                # Reset postJoinFilters
                join_node.postJoinFilters = []
                new_op_tree.addChild(join_node)
        case SortNode():
            new_op_tree = PandasSortNode(
                op_tree.sortCriteria
            )
        case FilterNode():
            new_op_tree = PandasFilterNode(
                op_tree.condition
            )
        case NewColumnNode():
            new_op_tree = PandasAddColumnsNode(
                op_tree.values
            )
        case RetrieveNode():
            new_op_tree = PandasRetrieveNode(
                op_tree.tableColumns,
                op_tree.retrieveTargetID
            )
        case LimitNode():
            new_op_tree = PandasLimitNode(
                op_tree.limitValue
            )
        case _:
            raise Exception(f"Unexpected op_tree, it was of class: {op_tree.__class__}")

    # Overwrite the existing OpNode
    lowest_node_pointer = new_op_tree
     # Find the lowest node
    searching = True
    while searching == True:
        match lowest_node_pointer:
            case UnaryPandasNode():
                if lowest_node_pointer.child != None:
                    lowest_node_pointer = lowest_node_pointer.child
                else:
                    searching = False
            case _:
                searching = False
    
    # Add in the children
    match lowest_node_pointer:
        case UnaryPandasNode():
            lowest_node_pointer.addChild(childNode)
        case BinaryPandasNode():
            lowest_node_pointer.addLeft(leftNode)
            lowest_node_pointer.addRight(rightNode)
        case _:
            # LeafPandasNode
            assert isinstance(lowest_node_pointer, LeafPandasNode)
            
    # Add nodeID to new_op_node
    assert hasattr(op_tree, "nodeID")
    new_op_tree.addID(op_tree.nodeID)
    
    return new_op_tree

# Classes for the Pandas Tree
class PandasBaseNode():
    def __init__(self):
        # The columns, as strs
        self.columns = set()
        self.nodeID = None
        
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
    
class LeafPandasNode(PandasBaseNode):
    def __init__(self):
        super().__init__()
    
class UnaryPandasNode(PandasBaseNode):
    def __init__(self):
        super().__init__()
        self.child = None
        
    def addChild(self, child: PandasBaseNode):
        assert self.child == None
        self.child = child
        
class BinaryPandasNode(PandasBaseNode):
    def __init__(self):
        super().__init__()
        self.left = None
        self.right = None
        
    def addLeft(self, left: PandasBaseNode):
        assert self.left == None
        self.left = left
        
    def addRight(self, right: PandasBaseNode):
        assert self.right == None
        self.right = right
        
# Classes for Nodes
class PandasScanNode(LeafPandasNode):
    def __init__(self, tableName, tableColumns, tableRestriction):
        super().__init__()
        self.tableName = tableName
        # Filter for only essential columns
        self.tableColumns = [x for x in tableColumns if x.essential == True]
        self.tableRestriction = tableRestriction
        
    def getTableColumnsForDF(self) -> list[str]:
        outputColumns = []
        for col in self.tableColumns:
            assert isinstance(col, ColumnValue)
            outputColumns.append(col.value)
        return outputColumns
    
    def refreshTableColumns(self):
        outputColumns = []
        for col in self.tableColumns:
            assert isinstance(col, ColumnValue)
            if col.codeName != '':
                outputColumns.append(col.codeName)
            else:
                outputColumns.append(col.value)
        self.columns = set(outputColumns)
    
class PandasOutputNode(UnaryPandasNode):
    def __init__(self, outputColumns, outputNames):
        super().__init__()
        self.outputColumns = outputColumns
        self.outputNames = outputNames

class PandasFilterNode(UnaryPandasNode):
    def __init__(self, condition):
        super().__init__()
        self.condition = condition

class PandasLimitNode(UnaryPandasNode):
    def __init__(self, limitValue):
        super().__init__()
        self.limitValue = limitValue

class PandasAddColumnsNode(UnaryPandasNode):
    def __init__(self, columns):
        super().__init__()
        self.addColumns = columns
        
class PandasRetrieveNode(LeafPandasNode):
    def __init__(self, tableColumns, retrieveTargetID):
        super().__init__()
        self.tableColumns = tableColumns
        self.retrieveTargetID = retrieveTargetID

class PandasAggrNode(UnaryPandasNode):
    def __init__(self, preAggregateExpressions, postAggregateOperations):
        super().__init__()
        self.preAggregateExpressions = preAggregateExpressions
        self.postAggregateOperations = postAggregateOperations

class PandasGroupNode(UnaryPandasNode):
    def __init__(self, keyExpressions, preAggregateExpressions, postAggregateOperations):
        super().__init__()
        self.keyExpressions = keyExpressions
        self.preAggregateExpressions = preAggregateExpressions
        self.postAggregateOperations = postAggregateOperations
        
class PandasSortNode(UnaryPandasNode):
    def __init__(self, sortCriteria):
        super().__init__()
        self.sortCriteria = sortCriteria
     
class PandasJoinNode(BinaryPandasNode):
    KNOWN_JOIN_METHODS = set([
        'hash',
        'merge',
        'bnl'
    ])
    KNOWN_JOIN_TYPES = set([
        'inner', 'outer',
        'leftsemijoin', 'rightsemijoin', 
        'leftantijoin', 'rightantijoin'
    ])
    def __init__(self, joinMethod, joinType, joinCondition):
        super().__init__()
        if (joinMethod == 'bnl') and (joinType != 'inner'):
            raise Exception("Trying to do a BNL ('Block-nested loop) join that's not an inner")
        self.postJoinFilters = []
        assert joinMethod in self.KNOWN_JOIN_METHODS, f"{joinMethod} is not in the known join methods"
        self.joinMethod = joinMethod
        assert joinType in self.KNOWN_JOIN_TYPES, f"{joinType} is not in the known join types"
        self.joinType = joinType
        joinCondition = self.__splitConditionsIntoList(joinCondition)
        assert isinstance(joinCondition, list)
        self.joinCondition = joinCondition
        
    def __splitConditionsIntoList(self, joinCondition: ExpressionBaseNode) -> list[ExpressionBaseNode]:
        newConditions = []
        joiningNodes = [AndOperator]
        currentJoinCondition = joinCondition
        while any(isinstance(currentJoinCondition, x) for x in joiningNodes):
            newConditions.append(currentJoinCondition.left)
            currentJoinCondition = currentJoinCondition.right
        newConditions.append(currentJoinCondition)
        # At this stage, we should scoop up any postJoinFilters
        self.postJoinFilters = list(filter(lambda x: isinstance(x, OrOperator), newConditions))
        realNewConditions = list(filter(lambda x: not isinstance(x, OrOperator), newConditions))
        
        # Extract a 'LessThanOperator' from a list of >= 1 EqualsOperator
        operatorCount = Counter(realNewConditions)
        non_equi_join_operators = [LessThanOperator(), LessThanEqOperator(), GreaterThanOperator()]
        non_equi_join_operator_types = [LessThanOperator, LessThanEqOperator, GreaterThanOperator]
        if (operatorCount[EqualsOperator()] > 0) and any(operatorCount[x] > 0 for x in non_equi_join_operators):
            self.postJoinFilters.extend(list(filter(lambda x: any(isinstance(x, op) for op in non_equi_join_operator_types), realNewConditions)))
            realNewConditions = list(filter(lambda x: not any(isinstance(x, op) for op in non_equi_join_operator_types), realNewConditions))
            assert len(realNewConditions) > 0
        
        return realNewConditions
    
    def checkLeftRightKeysValid(self, left: list[str], right: list[str]) -> bool:
        allLeftValid = all(aLeft in self.left.getTableColumnsString() for aLeft in left)
        allRightValid = all(aRight in self.right.getTableColumnsString() for aRight in right)
        return allLeftValid and allRightValid

def join_overlap_column_renaming(node: JoinNode, columnOverlap: set):
    # There was column overlap between the tables
    # so '_x' and '_y' versions have been created.
    # Set them to be _x and _y
    for col in node.left.columns:
        if col.codeName in [x.codeName for x in columnOverlap]:
            col.codeName = f"{col.codeName}_x"
    for col in node.right.columns:
        if col.codeName in [x.codeName for x in columnOverlap]:
            col.codeName = f"{col.codeName}_y"
            
def join_overlap_column_renaming_list(options: list[ExpressionBaseNode], columnOverlap: set):
    # There was column overlap between the tables
    # so '_x' and '_y' versions have been created.
    xGivenCols = set()
    yGivenCols = set()
    # Set them to be _x and _y
    for col in options:
        if col.codeName in [x.codeName for x in columnOverlap] and not col.codeName in xGivenCols:
            xGivenCols.add(col.codeName)
            col.codeName = f"{col.codeName}_x"
    for col in options:
        if col.codeName in [x.codeName for x in columnOverlap] and not col.codeName in yGivenCols:
            yGivenCols.add(col.codeName)
            col.codeName = f"{col.codeName}_y"

def do_join_key_separation(node: JoinNode):
    leftValues = []
    rightValues = []
    for x in node.joinCondition:
        if id(x.left) in [id(col) for col in node.left.columns]:
            leftValues.append(x.left)
        elif id(x.left) in [id(col) for col in node.right.columns]:
            rightValues.append(x.left)
        else:
            raise Exception(f"Couldn't find the x.left value in either of the left and right tables!")
        
        if id(x.right) in [id(col) for col in node.left.columns]:
            leftValues.append(x.right)
        elif id(x.right) in [id(col) for col in node.right.columns]:
            rightValues.append(x.right)
        else:
            raise Exception(f"Couldn't find the x.right value in either of the left and right tables!")
        
    assert len(leftValues) == len(rightValues) == len(node.joinCondition)
    return (leftValues, rightValues)

# Unparser
class UnparsePandasTree():
    def __init__(self, pandas_tree: PandasBaseNode) -> None:
        self.pandas_content = []
        self.nodesCounter = defaultdict(int)
        self.pandas_tree = pandas_tree
        
        self.nodeDict = {}
        self.gatherNodeDict(self.pandas_tree)
        
        self.__walk_tree(self.pandas_tree)
        
    def getChildTableNames(self, current: PandasBaseNode) -> list[str]:
        childTables = []
        if isinstance(current, BinaryPandasNode):
            childTables.append(current.left.tableName)
            childTables.append(current.right.tableName)
        elif isinstance(current, UnaryPandasNode):
            childTables.append(current.child.tableName)
        else:
            # A leaf node
            pass
        
        return childTables
    
    """
    We need to track new columns, surface previous
    Create new ones from just ColumnValues, make sure isn't in set
        If is, add randomness and try again
    Add to set, return new name in method
    Store this in the preAggrExpr
    """
    def getNewColumnName(self, expr: ExpressionBaseNode, current: PandasBaseNode):
        currentNodeColumns = current.getTableColumnsString()
        processedName = "".join(convert_expression_operator_to_column_name(expr))
        while processedName in currentNodeColumns:
            processedName = f"{processedName}{random.randint(0,9)}"
        current.addToTableColumns(expr)
        return processedName
        
    def writeContent(self, content: str) -> None:
        self.pandas_content.append(content)
    
    def getPandasContent(self) -> list[str]:
        return self.pandas_content
    
    def gatherNodeDict(self, current_node):
        if isinstance(current_node, BinaryPandasNode):
            self.gatherNodeDict(current_node.left)
            self.gatherNodeDict(current_node.right)
        elif isinstance(current_node, UnaryPandasNode):
            self.gatherNodeDict(current_node.child)
        else:
            # A leaf node
            pass
        
        if current_node.nodeID != None:
            assert current_node.nodeID not in self.nodeDict
            self.nodeDict[current_node.nodeID] = current_node
    
    def __walk_tree(self, current_node):
        # Walk to children Children
        if isinstance(current_node, BinaryPandasNode):
            self.__walk_tree(current_node.left)
            self.__walk_tree(current_node.right)
        elif isinstance(current_node, UnaryPandasNode):
            self.__walk_tree(current_node.child)
        else:
            # A leaf node
            pass
        
        # Visit the current_node and add it to self.pandas_content
        targetVisitorMethod = f"visit{current_node.__class__.__name__}"
        if hasattr(self, targetVisitorMethod):
            getattr(self, targetVisitorMethod)(current_node)
        else:
            raise Exception(f"No visit method found for class name: {current_node.__class__.__name__}, was expected to find a: '{targetVisitorMethod}' method.")
        assert len(current_node.columns) > 0
    
    def __getPandasRepresentationForColumn(self, column: ColumnValue):
        if column.codeName == '':
            assert isinstance(column, ColumnValue)
            return column.value
        else:
            return column.codeName
        
    def visitPandasLimitNode(self, node):
        self.nodesCounter[PandasLimitNode] += 1
        nodeNumber = self.nodesCounter[PandasLimitNode]
        createdDataFrameName = f"df_limit_{nodeNumber}"
        
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 1
        childTable = childTableList[0]
        
        self.writeContent(
            f"{createdDataFrameName} = {childTable}.head({node.limitValue})"
        )
        
        # Set the tableName
        node.tableName = createdDataFrameName
        # Add to node.columns
        node.columns = set(node.child.columns)
        
    def visitPandasRetrieveNode(self, node):
        self.nodesCounter[PandasRetrieveNode] += 1
        nodeNumber = self.nodesCounter[PandasRetrieveNode]
        createdDataFrameName = f"df_retrieve_{nodeNumber}"
        
        assert node.retrieveTargetID in self.nodeDict
        retrievedNode = self.nodeDict[node.retrieveTargetID]
        
        nodeTableColumnsCounter = Counter([type(x) for x in node.tableColumns])
        retrievedNodeColumnsCounter = Counter([type(x) for x in retrievedNode.columns])
        
        assert len(node.tableColumns) == len(retrievedNode.columns)
        assert nodeTableColumnsCounter == retrievedNodeColumnsCounter
        assert len(set(nodeTableColumnsCounter.values())) <= 1, "All should have the same value"
        assert all(1 == x for x in nodeTableColumnsCounter.values()), "All should be 1"
        
        # If any of these fail, we might need to look at storing the hyper IU references
        # Or some other method, that could be hard/annoying to do 

        # Rewrite the codeNames
        for i in range(len(node.tableColumns)):
            targetType = type(node.tableColumns[i])
            fromRetrievedNode = list(filter(lambda x: type(x) == targetType, retrievedNode.columns))
            assert len(fromRetrievedNode) == 1
            fromRetrievedNode = fromRetrievedNode[0]
            
            # Assign CodeName
            node.tableColumns[i].codeName = fromRetrievedNode.codeName
        
        gatherColumnCodeNames = [x.codeName for x in node.tableColumns]

        self.writeContent(
            f"{createdDataFrameName} = {retrievedNode.tableName}[{gatherColumnCodeNames}]"
        )
        
        # Set the tableName
        node.tableName = createdDataFrameName
        # Add to node.columns
        node.columns = set(node.tableColumns)
        
    def visitPandasAddColumnsNode(self, node):
        # We will add columns into the childtable
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 1
        childTable = childTableList[0]
        
        for newColumn in node.addColumns:
            newColumnExpression = convert_expression_operator_to_pandas(newColumn, childTable)
            newColumnName = self.getNewColumnName(newColumn, node.child)
            # Set the new name
            newColumn.setCodeName(newColumnName)
            self.writeContent(
                f"{childTable}['{newColumnName}'] = {newColumnExpression}"
            )

        # Set the tableName, to the child table, as we never really existed!
        node.tableName = childTable
        # Add to node.columns
        node.columns = set(node.child.columns) | set(node.addColumns)
        
    def visitPandasFilterNode(self, node):
        self.nodesCounter[PandasFilterNode] += 1
        nodeNumber = self.nodesCounter[PandasFilterNode]
        createdDataFrameName = f"df_filter_{nodeNumber}"
        
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 1
        childTable = childTableList[0]

        filterConditions = convert_expression_operator_to_pandas(node.condition, childTable)
        self.writeContent(
            f"{createdDataFrameName} = {childTable}[{filterConditions}]"
        )

        # Set the tableName
        node.tableName = createdDataFrameName
        # Add to node.columns
        node.columns = set(node.child.columns)
    
    def visitPandasSortNode(self, node):
        self.nodesCounter[PandasSortNode] += 1
        nodeNumber = self.nodesCounter[PandasSortNode]
        createdDataFrameName = f"df_sort_{nodeNumber}"
        
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 1
        childTable = childTableList[0]
        
        assert len(node.sortCriteria) > 0
        processedSortKeys = [self.__getPandasRepresentationForColumn(x.value) for x in node.sortCriteria]
        processedSortDirections = [(not x.descending) for x in node.sortCriteria]
        self.writeContent(
            f"{createdDataFrameName} = {childTable}.sort_values(\n"
            f"{TAB}by={processedSortKeys},\n"
            f"{TAB}ascending={processedSortDirections}\n"
            f")"
        )
        
        # Set the tableName
        node.tableName = createdDataFrameName
        # Add to node.columns
        node.columns = set(node.child.columns)
    
    def visitPandasGroupNode(self, node):
        self.nodesCounter[PandasGroupNode] += 1
        nodeNumber = self.nodesCounter[PandasGroupNode]
        createdDataFrameName = f"df_group_{nodeNumber}"
        
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 1
        childTable = childTableList[0]
        
        # preAggregateExpressions, do these in the childTable
        for preAggrExpr in node.preAggregateExpressions:
            if isinstance(preAggrExpr, ColumnValue):
                # Check this column is in the child, for safety
                assert preAggrExpr in node.child.columns
            elif preAggrExpr.codeName != '':
                # "Already been created, don't do it again
                pass
            else:
                newColumnName = self.getNewColumnName(preAggrExpr, node.child)
                newColumnExpression = convert_expression_operator_to_pandas(preAggrExpr, childTable, newColumnName)
                # Set the new name
                preAggrExpr.setCodeName(newColumnName)
                if isinstance(newColumnExpression, str):
                    self.writeContent(
                        f"{childTable}['{newColumnName}'] = {newColumnExpression}"
                    )
                elif isinstance(newColumnExpression, list):
                    for line in newColumnExpression:
                        self.writeContent(line)
                else:
                    raise Exception(f"Unexpected format of newColumnExpression: {type(newColumnExpression)}")
        
        # keyExpressions
        assert len(node.keyExpressions) > 0
        processedKeys = [self.__getPandasRepresentationForColumn(x) for x in node.keyExpressions]
        self.writeContent(
            f"{createdDataFrameName} = {childTable} \\\n"
            f"{TAB}.groupby({processedKeys}, sort=False, as_index=False) \\\n"
            f"{TAB}.agg("
        )
        
        # postAggregateOperations
        for postAggrOp in node.postAggregateOperations:
            # Write each one
            newColumnName = self.getNewColumnName(postAggrOp, node)
            # Set the new name
            postAggrOp.setCodeName(newColumnName)
            if isinstance(postAggrOp, CountAllOperator):
                # Select column name from child columns, choose random (ish) column
                previousColumnName = list(node.child.getTableColumnsString() - set(processedKeys))[0]
                assert previousColumnName != ''
            else:
                assert not isinstance(postAggrOp.child, AggregationOperators), "There should be only 1 aggregation, as one can't nest them in SQL"
                previousColumnName = self.__getPandasRepresentationForColumn(postAggrOp.child)
            columnAggregation = convert_expression_operator_to_aggr(postAggrOp)
            assert not any(x == "" for x in [newColumnName, previousColumnName, columnAggregation])
            self.writeContent(
                f"{TAB}{TAB}{newColumnName}=('{previousColumnName}', '{columnAggregation}'),"
            )
            
        # Write the aggregation closing
        self.writeContent(
            f"{TAB})"
        )
        
        # Set the tableName
        node.tableName = createdDataFrameName
        # Set the table columns
        node.columns = set(node.keyExpressions) | set(node.postAggregateOperations)
        
    def visitPandasJoinNode(self, node):
        self.nodesCounter[PandasJoinNode] += 1
        nodeNumber = self.nodesCounter[PandasJoinNode]
        createdDataFrameName = f"df_join_{nodeNumber}"
        
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 2
        
        # Set leftTable and rightTable
        leftTable = childTableList[0]
        rightTable = childTableList[1]
        
        # Decide the joinMethod
        joinMethod = None
        match node.joinMethod:
            case "hash":
                # If you set 'merge(Sort=False)', then it's a Hash Join
                joinMethod = False
            case "merge":
                joinMethod = True
            case "bnl":
                # Doesn't matter, but initialise it for good form
                joinMethod = False 
            case _:
                raise Exception(f"Unknown Join Method provided: {node.joinMethod}")
        
        assert joinMethod != None
    
        if any(isinstance(x, LookupOperator) for x in node.joinCondition) and node.joinMethod == "bnl":
            # This is a batch-nested loop situation
            assert node.joinType == "inner"
            node.joinType = "bnl"
        elif not all(isinstance(x, EqualsOperator) for x in node.joinCondition):
            # This is a non-equi join situation
            operatorCount = Counter(node.joinCondition)
            if operatorCount[EqualsOperator()] == 1 and operatorCount[NotEqualsOperator()] >= 1:
                # Inner cond situation
                # The left and right of the equals operator should be the same code name
                # We refer to this henceforth as the filter_key
                innerTableName = f"inner_cond"
                equalOperator = list(filter(lambda x: isinstance(x, EqualsOperator), node.joinCondition))
                assert len(equalOperator) == 1
                equalOperator = equalOperator[0]
                assert equalOperator.left.codeName == equalOperator.right.codeName
                innerKey = equalOperator.left.codeName
                # Check the joinCondition didn't have things other than Equals and NotEquals
                assert len(set(operatorCount.keys()) - set([EqualsOperator(), NotEqualsOperator()])) == 0
                # Pull out the inner cond, use a deep copy, as we'll be editing some stuff
                inner_condition_operators = copy.deepcopy(list(filter(lambda x: isinstance(x, NotEqualsOperator), node.joinCondition)))
                assert len(inner_condition_operators) > 0
                # Do intersection thing with "_x" and "_y"; only with innerConditionColumns!
                innerConditionColumns = flatten([[x.left, x.right] for x in inner_condition_operators])
                columnOverlap = node.left.columns.intersection(node.right.columns) - set([equalOperator.left, equalOperator.right])
                if columnOverlap:
                    join_overlap_column_renaming_list(innerConditionColumns, columnOverlap)
                
                if len(inner_condition_operators) == 1:
                    innerCondition = convert_expression_operator_to_pandas(inner_condition_operators[0], innerTableName)#
                else:
                    # 2 or more inner conditions
                    and_join = join_statements_with_operator(inner_condition_operators, "AndOperator")
                    innerCondition = convert_expression_operator_to_pandas(and_join, innerTableName)#
               
                self.writeContent(
                    f"{innerTableName} = {leftTable}.merge({rightTable}, left_on='{innerKey}', right_on='{innerKey}', how='{'inner'}', sort={joinMethod})"
                )
                
                projectInnerCondWithKey = ""
                if node.joinType not in ['rightsemijoin', 'leftsemijoin']:
                    projectInnerCondWithKey = f"['{innerKey}']"
                    
                self.writeContent(
                    f"{innerTableName} = {innerTableName}[{innerCondition}]{projectInnerCondWithKey}"
                )
                
                # Set the values for the main join
                node.joinCondition = [equalOperator]
                rightTable = innerTableName
            else:
                assert node.joinType == "inner"
                node.joinType = "non-equi"
                
        if node.joinMethod != "bnl":                
            leftKeys, rightKeys = do_join_key_separation(node)
            
            leftKeysString = [self.__getPandasRepresentationForColumn(x) for x in leftKeys]
            rightKeysString = [self.__getPandasRepresentationForColumn(x) for x in rightKeys]
            
            assert node.checkLeftRightKeysValid(leftKeysString, rightKeysString)
        else:
            # Initialise as sets for proper form
            leftKeys, rightKeys = set(), set()
               
        # And set the table columns
        match node.joinType:
            case "inner":
                # Check for column overlap at the start
                columnOverlap = node.left.columns.intersection(node.right.columns) - set(set(leftKeys) | set(rightKeys))
                if columnOverlap:
                    join_overlap_column_renaming(node, columnOverlap)
                    assert len(node.left.columns.intersection(node.right.columns) - set(set(leftKeys) | set(rightKeys))) == 0
                    
                self.writeContent(
                    f"{createdDataFrameName} = {leftTable}.merge({rightTable}, left_on={leftKeysString}, right_on={rightKeysString}, how='{'inner'}', sort={joinMethod})"
                )
                node.columns = node.left.columns.union(node.right.columns)
            case "rightsemijoin":
                if len(leftKeysString) == 1 and len(rightKeysString) == 1:
                    self.writeContent(
                        f"{createdDataFrameName} = {rightTable}[{rightTable}['{rightKeysString[0]}'].isin({leftTable}['{leftKeysString[0]}'])]"
                    )
                else:
                    self.writeContent(
                        f"{createdDataFrameName} = {rightTable}[{rightTable}[{rightKeysString}].isin({leftTable}.set_index([{leftKeysString}]).index)]"
                    )
                node.columns = node.right.columns
            case "leftsemijoin":
                if len(leftKeysString) == 1 and len(rightKeysString) == 1:
                    self.writeContent(
                        # df_join_1 = df_scan_1[df_scan_1['o_orderkey'].isin(df_scan_2["l_orderkey"])]
                        f"{createdDataFrameName} = {leftTable}[{leftTable}['{leftKeysString[0]}'].isin({rightTable}['{rightKeysString[0]}'])]"
                    )
                else:
                    self.writeContent(
                        # df_join_1 = df_scan_1[df_scan_1[['o_orderkey']].isin(df_scan_2.set_index(["l_orderkey"]).index)]
                        f"{createdDataFrameName} = {leftTable}[{leftTable}[{leftKeysString}].isin({rightTable}.set_index([{rightKeysString}]).index)]"
                    )
                node.columns = node.left.columns
            case "leftantijoin" | "rightantijoin":
                self.writeContent(
                    f"{createdDataFrameName} = {leftTable}.merge({rightTable}, left_on={leftKeysString}, right_on={rightKeysString}, how='{'outer'}', sort={joinMethod}, indicator=True)"
                )
                
                # Set the joinKeep
                if node.joinType == "leftantijoin":
                    joinKeep = 'left_only'
                    node.columns = node.left.columns
                elif node.joinType == "rightantijoin":
                    joinKeep = 'right_only'
                    node.columns = node.right.columns
                else:
                    raise Exception(f"Unknown type of antijoin: {node.joinType}")
                
                self.writeContent(
                    f"{createdDataFrameName} = {createdDataFrameName}[{createdDataFrameName}._merge == '{joinKeep}'].drop('_merge', axis = 1)"
                )
            case "non-equi":
                assert len(node.joinCondition) == 1
                
                # Check for column overlap at the start
                columnOverlap = node.left.columns.intersection(node.right.columns) - set(set(leftKeys) | set(rightKeys))
                if columnOverlap:
                    join_overlap_column_renaming(node, columnOverlap)
                    assert len(node.left.columns.intersection(node.right.columns) - set(set(leftKeys) | set(rightKeys))) == 0
                
                joinCondition = convert_expression_operator_to_pandas(node.joinCondition[0], createdDataFrameName)
                self.writeContent(
                    f"{createdDataFrameName} = {leftTable}.merge({rightTable}, how='{'cross'}', sort={joinMethod})"
                )
                self.writeContent(
                    f"{createdDataFrameName} = {createdDataFrameName}[{joinCondition}]"
                )
                node.columns = node.left.columns.union(node.right.columns)
            case "outer":
                # Check for column overlap at the start
                columnOverlap = node.left.columns.intersection(node.right.columns) - set(set(leftKeys) | set(rightKeys))
                if columnOverlap:
                    join_overlap_column_renaming(node, columnOverlap)
                    assert len(node.left.columns.intersection(node.right.columns) - set(set(leftKeys) | set(rightKeys))) == 0
                
                self.writeContent(
                    f"{createdDataFrameName} = {leftTable}.merge({rightTable}, left_on={leftKeysString}, right_on={rightKeysString}, how='{'outer'}', sort={joinMethod})"
                )
                node.columns = node.left.columns.union(node.right.columns)
            case "bnl":
                # Check for column overlap at the start
                columnOverlap = node.left.columns.intersection(node.right.columns) - set(set(leftKeys) | set(rightKeys))
                if columnOverlap:
                    join_overlap_column_renaming(node, columnOverlap)
                    assert len(node.left.columns.intersection(node.right.columns) - set(set(leftKeys) | set(rightKeys))) == 0
                
                defaultBlockSize = 10
                intermediateDataframeName = "joined_block"
                joinCondition = convert_expression_operator_to_pandas(node.joinCondition[0], intermediateDataframeName)
                self.writeContent(
                    f"result_blocks = []\n"
                    f"for i in range(0, len({leftTable}), {defaultBlockSize}):\n"
                    f"{TAB}block_left = {leftTable}.iloc[i:i+{defaultBlockSize}]\n"
                    f"{TAB}for j in range(0, len({rightTable}), {defaultBlockSize}):\n"
                    f"{TAB}{TAB}block_right = {rightTable}.iloc[j:j+{defaultBlockSize}]\n"
                    f"{TAB}{TAB}{intermediateDataframeName} = pd.merge(block_left, block_right, how='{'cross'}')\n"
                    f"{TAB}{TAB}{intermediateDataframeName} = {intermediateDataframeName}[{joinCondition}]\n"
                    f"{TAB}{TAB}result_blocks.append({intermediateDataframeName})\n"
                    f"{createdDataFrameName} = pd.concat(result_blocks, ignore_index=True)"
                )
                
                # Set node columns
                node.columns = node.left.columns.union(node.right.columns)
            case _:
                raise Exception(f"Unexpected Join Type supplied: {node.joinType}")
        
        # Set the tableName
        node.tableName = createdDataFrameName

    def visitPandasScanNode(self, node):
        self.nodesCounter[PandasScanNode] += 1
        nodeNumber = self.nodesCounter[PandasScanNode]
        createdDataFrameName = f"df_scan_{nodeNumber}"
        previousTableName = node.tableName
        
        # Use restrictions
        if node.tableRestriction != []:
            # Convert tableRestriction
            tableRestriction = convert_expression_operator_to_pandas(node.tableRestriction, previousTableName)
            self.writeContent(
                f"{createdDataFrameName} = {previousTableName}[{tableRestriction}]"
            )
            # Update previousTableName
            previousTableName = createdDataFrameName
            
        # Limit by table columns
        self.writeContent(
            f"{createdDataFrameName} = {previousTableName}[{node.getTableColumnsForDF()}]"
        )
        
        # Set the tableName
        node.tableName = createdDataFrameName
        # Set the tableColumns
        node.addToTableColumns(node.tableColumns)
        
    def visitPandasAggrNode(self, node):        
        self.nodesCounter[PandasAggrNode] += 1
        nodeNumber = self.nodesCounter[PandasAggrNode]
        
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 1
        childTable = childTableList[0]
        
        # Do preAggregateExpressions first
        if node.preAggregateExpressions != []:
            # Create the new column(s) in the childTable
            for preAggrExpr in node.preAggregateExpressions:
                if isinstance(preAggrExpr, ColumnValue):
                    # Check this column is in the child, for safety
                    assert preAggrExpr in node.child.columns
                elif preAggrExpr.codeName != '':
                    # Already created
                    pass
                else:
                    newColumnName = self.getNewColumnName(preAggrExpr, node.child)
                    newColumnExpression = convert_expression_operator_to_pandas(preAggrExpr, childTable, newColumnName)
                    # Set the new name
                    preAggrExpr.setCodeName(newColumnName)
                    if isinstance(newColumnExpression, str):
                        self.writeContent(
                            f"{childTable}['{newColumnName}'] = {newColumnExpression}"
                        )
                    elif isinstance(newColumnExpression, list):
                        for line in newColumnExpression:
                            self.writeContent(line)
                    else:
                        raise Exception(f"Unexpected format of newColumnExpression: {type(newColumnExpression)}")

        # Create the new dataFrame, do the postAggregateOperations here
        createdDataFrameName = f"df_aggr_{nodeNumber}"
        self.writeContent(
            f"{createdDataFrameName} = pd.DataFrame()"
        )
        
        # Post Aggregate Expressions
        if node.postAggregateOperations != []:
            for postAggrOp in node.postAggregateOperations:
                newColumnExpression = convert_aggregation_tree_to_pandas(postAggrOp, childTable)
                newColumnName = self.getNewColumnName(postAggrOp, node)
                # Set the new name
                postAggrOp.setCodeName(newColumnName)
                self.writeContent(
                    f"{createdDataFrameName}['{newColumnName}'] = [{newColumnExpression}]"
                )
        
        # Set the tableName
        node.tableName = createdDataFrameName
        # Set node.columns
        node.columns = set(node.postAggregateOperations)
        
    def visitPandasOutputNode(self, node):
        self.nodesCounter[PandasOutputNode] += 1
        nodeNumber = self.nodesCounter[PandasOutputNode]
        
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 1
        childTable = childTableList[0]
        
        # Create the new dataFrame
        createdDataFrameName = f"df_output_{nodeNumber}"
        
        # Rename the columns
        assert len(node.outputNames) == len(node.outputColumns)
        # Only create new names, if they'd be different
        currentNewNameDict = {
            self.__getPandasRepresentationForColumn(node.outputColumns[i]): node.outputNames[i]
            for i in range(len(node.outputNames))
            if self.__getPandasRepresentationForColumn(node.outputColumns[i]) != node.outputNames[i]
        }
        
        previousTableName = childTable
        
        if currentNewNameDict != {}:
            self.writeContent(
                f"{createdDataFrameName} = {previousTableName}.rename(columns={currentNewNameDict})"
            )
            # Update previousTableName
            previousTableName = createdDataFrameName
            
        # Limit by node.outputNames
        # Limit by table columns
        self.writeContent(
            f"{createdDataFrameName} = {previousTableName}[{node.outputNames}]"
        )
        
        # Set the tableName
        node.tableName = createdDataFrameName
        # Add to node.columns
        node.columns = set(node.outputColumns)
