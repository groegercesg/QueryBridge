from collections import defaultdict
import random

from universal_plan_nodes import *
from expression_operators import *

TAB = "    "

def convert_aggregation_tree_to_pandas(aggr_tree: ExpressionBaseNode, dataFrameName: str) -> str:
    # Visit Children
    if isinstance(aggr_tree, AggregationOperators):
        childNode = convert_aggregation_tree_to_pandas(aggr_tree.child, dataFrameName)
    else:
        # Should have a completed Code Name
        assert aggr_tree.codeName != ""
        return f"{dataFrameName}.{aggr_tree.codeName}"
    
    expression_output = ""
    match aggr_tree:
        case SumAggrOperator():
            expression_output = f"{childNode}.sum()"
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
            # PandasScanNode
            assert isinstance(lowest_node_pointer, PandasScanNode)
    
    return new_op_tree

# Classes for the Pandas Tree
class PandasBaseNode():
    def __init__(self):
        # The columns, as strs
        self.columns = set()
        
    def addToTableColumns(self, incomingColumns):
        localColumns = set()
        if isinstance(incomingColumns, list):
            localColumns.update(incomingColumns)
        elif isinstance(incomingColumns, str):
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
                self.columns.update(self.left.getTableColumns())
            if hasattr(self, "right"):
                # if self.right.columns:
                #     self.columns.update(self.right.columns)
                # else:
                self.columns.update(self.right.getTableColumns())
            if hasattr(self, "child"):
                # if self.child.columns:
                #     self.columns.update(self.child.columns)
                # else:
                self.columns.update(self.child.getTableColumns())
        
    def getTableColumns(self) -> set():
        self.__updateTableColumns()
        return self.columns
    
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
class PandasScanNode(PandasBaseNode):
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
    
class PandasOutputNode(UnaryPandasNode):
    def __init__(self, outputColumns, outputNames):
        super().__init__()
        self.outputColumns = outputColumns
        self.outputNames = outputNames

class PandasFilterNode(UnaryPandasNode):
    def __init__(self, condition):
        super().__init__()
        self.condition = condition

class PandasAddColumnsNode(UnaryPandasNode):
    def __init__(self, columns):
        super().__init__()
        self.addColumns = columns

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
        return realNewConditions
    
    def checkLeftRightKeysValid(self, left: list[str], right: list[str]) -> bool:
        allLeftValid = all(aLeft in self.left.getTableColumns() for aLeft in left)
        allRightValid = all(aRight in self.right.getTableColumns() for aRight in right)
        return allLeftValid and allRightValid

# Unparser
class UnparsePandasTree():
    def __init__(self, pandas_tree: PandasBaseNode) -> None:
        self.pandas_content = []
        self.nodesCounter = defaultdict(int)
        self.pandas_tree = pandas_tree
        self.usedColumns = set()
        
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
        currentNodeColumns = current.getTableColumns()
        processedName = "".join(convert_expression_operator_to_column_name(expr))
        while processedName in currentNodeColumns:
            processedName = f"{processedName}{random.randint(0,9)}"
        while processedName in self.usedColumns:
            processedName = f"{processedName}{random.randint(0,9)}"
        current.addToTableColumns(processedName)
        self.usedColumns.add(processedName)
        return processedName
        
    def writeContent(self, content: str) -> None:
        self.pandas_content.append(content)
    
    def getPandasContent(self) -> list[str]:
        return self.pandas_content
    
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
    
    def __getPandasRepresentationForColumn(self, column: ColumnValue):
        if column.codeName == '':
            assert isinstance(column, ColumnValue)
            return column.value
        else:
            return column.codeName
        
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
                assert preAggrExpr.value in node.child.getTableColumns()
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
        
        # KeyExpressions
        assert len(node.keyExpressions) > 0
        assert len(node.postAggregateOperations) > 0, "We can't have a Group By without aggregations"
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
                previousColumnName = list(node.child.getTableColumns() - set(processedKeys))[0]
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
        
    def visitPandasJoinNode(self, node):
        self.nodesCounter[PandasJoinNode] += 1
        nodeNumber = self.nodesCounter[PandasJoinNode]
        createdDataFrameName = f"df_join_{nodeNumber}"
        
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 2
    
        if not all(isinstance(x, EqualsOperator) for x in node.joinCondition):
            # This is a non-equi join situation
            assert node.joinType == "inner"
            node.joinType = "non-equi"
            
        leftKeys = [self.__getPandasRepresentationForColumn(x.left) for x in node.joinCondition]
        rightKeys = [self.__getPandasRepresentationForColumn(x.right) for x in node.joinCondition]
        joinMethod = None
        
        # TODO: Fix this upstream, as a Hyper Tree transformation
        # Sometimes, the keys (direct from the Hyper plan)
        # Aren't actually the correct way round
        if node.checkLeftRightKeysValid(leftKeys, rightKeys) == False:
            # Swap, then assert if true
            oldLeft = leftKeys
            leftKeys = rightKeys
            rightKeys = oldLeft
            assert node.checkLeftRightKeysValid(leftKeys, rightKeys) 
        
        match node.joinMethod:
            case "hash":
                # If you set 'merge(Sort=False)', then it's a Hash Join
                joinMethod = False
            case _:
                raise Exception(f"Unknown Join Method provided: {node.joinMethod}")
        
        assert joinMethod != None
        
        # And set the table columns
        match node.joinType:
            case "inner":
                self.writeContent(
                    f"{createdDataFrameName} = {childTableList[0]}.merge({childTableList[1]}, left_on={leftKeys}, right_on={rightKeys}, how='{'inner'}', sort={joinMethod})"
                )
                node.columns = node.left.getTableColumns().union(node.right.getTableColumns())
            case "rightsemijoin":
                if len(leftKeys) == 1 and len(rightKeys) == 1:
                    self.writeContent(
                        f"{createdDataFrameName} = {childTableList[1]}[{childTableList[1]}['{rightKeys[0]}'].isin({childTableList[0]}['{leftKeys[0]}'])]"
                    )
                else:
                    self.writeContent(
                        f"{createdDataFrameName} = {childTableList[1]}[{childTableList[1]}[{rightKeys}].isin({childTableList[0]}.set_index([{leftKeys}]).index)]"
                    )
                node.columns = node.right.getTableColumns()
            case "leftsemijoin":
                if len(leftKeys) == 1 and len(rightKeys) == 1:
                    self.writeContent(
                        # df_join_1 = df_scan_1[df_scan_1['o_orderkey'].isin(df_scan_2["l_orderkey"])]
                        f"{createdDataFrameName} = {childTableList[0]}[{childTableList[0]}['{leftKeys[0]}'].isin({childTableList[1]}['{rightKeys[0]}'])]"
                    )
                else:
                    self.writeContent(
                        # df_join_1 = df_scan_1[df_scan_1[['o_orderkey']].isin(df_scan_2.set_index(["l_orderkey"]).index)]
                        f"{createdDataFrameName} = {childTableList[0]}[{childTableList[0]}[{leftKeys}].isin({childTableList[1]}.set_index([{rightKeys}]).index)]"
                    )
                node.columns = node.left.getTableColumns()
            case "leftantijoin" | "rightantijoin":
                self.writeContent(
                    f"{createdDataFrameName} = {childTableList[0]}.merge({childTableList[1]}, left_on={leftKeys}, right_on={rightKeys}, how='{'outer'}', sort={joinMethod}, indicator=True)"
                )
                
                # Set the joinKeep
                if node.joinType == "leftantijoin":
                    joinKeep = 'left_only'
                    node.columns = node.left.getTableColumns()
                elif node.joinType == "rightantijoin":
                    joinKeep = 'right_only'
                    node.columns = node.right.getTableColumns()
                else:
                    raise Exception(f"Unknown type of antijoin: {node.joinType}")
                
                self.writeContent(
                    f"{createdDataFrameName} = {createdDataFrameName}[{createdDataFrameName}._merge == '{joinKeep}'].drop('_merge', axis = 1)"
                )
            case "non-equi":
                assert len(node.joinCondition) == 1
                joinCondition = convert_expression_operator_to_pandas(node.joinCondition[0], createdDataFrameName)
                self.writeContent(
                    f"{createdDataFrameName} = {childTableList[0]}.merge({childTableList[1]}, how='{'cross'}', sort={joinMethod})"
                )
                self.writeContent(
                    f"{createdDataFrameName} = {createdDataFrameName}[{joinCondition}]"
                )
            case _:
                raise Exception(f"Unexpected Join Type supplied: {node.joinType}")
        
        # Look for duplicate columns
        columnOverlap = node.left.getTableColumns().intersection(node.right.getTableColumns())
        if columnOverlap:
            # There was column overlap between the tables
            # so '_x' and '_y' versions have been created.
            node.overlapColumns = columnOverlap
        
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
        node.addToTableColumns(node.getTableColumnsForDF())
        # Add to inuseColumns
        self.usedColumns.update(node.getTableColumnsForDF())
        
    def visitPandasAggrNode(self, node):        
        self.nodesCounter[PandasGroupNode] += 1
        nodeNumber = self.nodesCounter[PandasGroupNode]
        
        # Get child name
        childTableList = self.getChildTableNames(node)
        assert len(childTableList) == 1
        childTable = childTableList[0]
        
        # Do preAggregateExpressions first
        if node.preAggregateExpressions != []:
            # Create the new column(s) in the childTable
            for preAggrExpr in node.preAggregateExpressions:
                assert not isinstance(preAggrExpr, ColumnValue)
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
        
        self.writeContent(
            f"{createdDataFrameName} = {childTable}.rename(columns={currentNewNameDict})"
        )
            
        # Limit by node.outputNames
        # Limit by table columns
        self.writeContent(
            f"{createdDataFrameName} = {createdDataFrameName}[{node.outputNames}]"
        )
