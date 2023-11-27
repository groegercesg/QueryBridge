from collections import Counter, defaultdict

from universal_plan_nodes import *
from expression_operators import *

from sdqlpy_helpers import *
TAB = "    "

# Classes for the SDQLpy Tree
class SDQLpyBaseNode():
    def __init__(self):
        # The columns, as strs
        self.incomingDict = None
        self.outputDict = None
        self.nodeID = None
        self.sdqlrepr = None
        self.topNode = False
        self.filterContent = None
        self.cardinality = None
    
    def setCardinality(self, card):
        assert self.cardinality == None and isinstance(card, int)
        self.cardinality = card
    
    def addFilterContent(self, filterContent):
        assert self.filterContent == None
        self.filterContent = filterContent
        
    def addID(self, value):
        assert self.nodeID == None
        self.nodeID = value
    
    def getTableName(self, unparser, not_output=False):
        assert self.sdqlrepr != None
        if self.topNode == True and not_output == False:
            # Default SDQL name for top level output
            self.tableName =  "results"
        else:
            nodeNumber = unparser.nodesCounter[self.__class__.__name__]
            self.tableName = f"{self.sdqlrepr}_{str(nodeNumber)}"
            
        return self.tableName
    
    def refreshNode(self):
        pass
        
    def set_output_dict(self):
        raise Exception("We should never run this Base class version")

class LeafSDQLpyNode(SDQLpyBaseNode):
    def __init__(self):
        super().__init__()
        
class PipelineBreakerNode(SDQLpyBaseNode):
    def __init__(self):
        super().__init__()
        
class UnarySDQLpyNode(PipelineBreakerNode):
    def __init__(self):
        super().__init__()
        self.child = None
        self.incomingDict = None
        
    def addChild(self, child: SDQLpyBaseNode):
        assert self.child == None
        self.child = child
        assert self.incomingDict == None
        self.incomingDict = self.child.outputDict
        
    def getChildName(self, unparser):
        childTableList = unparser.getChildTableNames(self)
        assert len(childTableList) == 1
        return childTableList[0]
        
class BinarySDQLpyNode(PipelineBreakerNode):
    def __init__(self):
        super().__init__()
        self.left = None
        self.right = None
        self.incomingDicts = []
        
    def addLeft(self, left: SDQLpyBaseNode):
        assert self.left == None
        self.left = left
        self.incomingDicts.append(self.left.outputDict)
        
    def addRight(self, right: SDQLpyBaseNode):
        assert self.right == None
        self.right = right
        self.incomingDicts.append(self.right.outputDict)
        
    def getChildNames(self, unparser):
        childTableList = unparser.getChildTableNames(self)
        assert len(childTableList) == 2
        return childTableList
    
    def refreshNode(self):
        assert isinstance(self, SDQLpyJoinNode)
        self.create_output_dict()

# Classes for Nodes
class SDQLpyRecordNode(LeafSDQLpyNode):
    def __init__(self, tableName, tableColumns):
        super().__init__()
        self.tableName = tableName
        self.sdqlrepr = tableName
        # Filter for only essential columns
        self.tableColumns = [x for x in tableColumns if x.essential == True]
        self.incomingDict = SDQLpySRDict(
            self.tableColumns,
            list()
        )
        self.outputDict = self.outputDict = SDQLpySRDict(
            self.tableColumns,
            list() 
        )
        
    def set_output_dict(self):
        pass
        
    def getTableName(self, unparser):
        if self.filterContent != None:
            nodeNumber = unparser.nodesCounter[self.__class__.__name__]
            self.tableName = f"filter_{str(nodeNumber)}"
            return self.tableName
        else:
            return self.sdqlrepr

class SDQLpyJoinBuildNode(UnarySDQLpyNode):
    def __init__(self, tableKeys, additionalColumns):
        super().__init__()
        assert isinstance(tableKeys, list)
        self.tableKeys = tableKeys
        assert isinstance(additionalColumns, list)
        # Filter for additional columns not equal to the key column
        self.additionalColumns = additionalColumns
        self.sdqlrepr = "indexed"
        self.outputDict = None
        
    def set_output_dict(self):
        self.outputDict = SDQLpySRDict(
            self.tableKeys,
            self.child.outputDict.flatCols() 
        )

class SDQLpyFilterNode(UnarySDQLpyNode):
    def __init__(self):
        super().__init__()
        self.outputDict = None
        self.sdqlrepr = "filter"
        
    def set_output_dict(self):
        # A filter node should only have keys, no values
        assert self.child != None
        self.outputDict = SDQLpySRDict(
            self.child.outputDict.keys + self.child.outputDict.values,
            list()
        )

class SDQLpyAggrNode(UnarySDQLpyNode):
    def __init__(self, aggregateOperations):
        super().__init__()
        self.aggregateOperations = aggregateOperations
        self.sdqlrepr = "aggr"
        self.outputDict = None
        
    def set_output_dict(self):
        self.outputDict = SDQLpySRDict(
            list(),
            self.aggregateOperations
        )
        
class SDQLpyConcatNode(UnarySDQLpyNode):
    def __init__(self, outputColumns):
        super().__init__()
        self.sdqlrepr = "concat"
        self.outputColumns = outputColumns
        self.outputDict = None
        
    def set_output_dict(self):
        self.outputDict = SDQLpySRDict(
            list(self.child.outputDict.flatCols()),
            list()
        )
    
class SDQLpyGroupNode(UnarySDQLpyNode):
    def __init__(self, keyExpressions, aggregateOperations):
        super().__init__()
        self.keyExpressions = keyExpressions
        self.aggregateOperations = aggregateOperations
        self.outputDict = None
        self.sdqlrepr = "group"
        
    def set_output_dict(self):
        # Track and carry previous outputDict value
        if self.outputDict == None:
            previousDuplicateUser = False
        else:
            previousDuplicateUser = self.outputDict.duplicateUser
        
        self.outputDict = SDQLpySRDict(
            self.keyExpressions,
            self.aggregateOperations
        )
        self.outputDict.set_duplicateUser(previousDuplicateUser)
        
class SDQLpyJoinNode(BinarySDQLpyNode):
    KNOWN_JOIN_METHODS = set([
        'hash'
    ])
    KNOWN_JOIN_TYPES = set([
        'inner', 'rightsemijoin'
    ])
    
    def __init__(self, joinMethod, joinType, joinCondition):
        super().__init__()
        assert joinMethod in self.KNOWN_JOIN_METHODS, f"{joinMethod} is not in the known join methods"
        self.joinMethod = joinMethod
        assert joinType in self.KNOWN_JOIN_TYPES, f"{joinType} is not in the known join types"
        self.joinType = joinType
        joinCondition = self.__splitConditionsIntoList(joinCondition)
        assert isinstance(joinCondition, list)
        self.joinCondition = joinCondition
        self.outputDict = None
        self.sdqlrepr = "join"
        self.third_node = None
        self.is_update_sum = False
        
    def update_update_sum(self, newValue):
        assert isinstance(newValue, bool)
        self.is_update_sum = newValue
        
    def get_output_dict(self):
        self.set_output_dict()
        return self.outputDict
        
    def set_output_dict(self):
        assert (self.left != None) and (self.right != None)
        # Track and carry previous outputDict value
        if self.outputDict == None:
            previousDuplicateCounter = False
        else:
            previousDuplicateCounter = self.outputDict.duplicateCounter
        
        match self.joinType:
            case "inner":
                self.outputDict = SDQLpySRDict(
                    self.left.outputDict.keys + self.right.outputDict.keys +
                    self.left.outputDict.values + self.right.outputDict.values,
                    list()
                )
                self.outputDict.set_duplicateCounter(
                    previousDuplicateCounter
                )
            case "rightsemijoin":
                self.outputDict = SDQLpySRDict(
                    self.right.outputDict.keys +
                    self.right.outputDict.values,
                    list()
                )
                self.outputDict.set_duplicateCounter(
                    previousDuplicateCounter
                )
            case _:
                raise Exception(f"No columns variable set for joinType: {self.joinType}")
        
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
            
        # Fix postJoinFilters
        if len(self.postJoinFilters) == 0:
            # Skip if no 'postJoinFilters', set as None
            self.postJoinFilters = None
        elif len(self.postJoinFilters) == 1:
            self.postJoinFilters = self.postJoinFilters[0]
        else:
            raise Exception(f"Length of postJoinFilters wasn't 1, we should join it with and/or. Examine context to decide which")
        
        return realNewConditions
    
    def do_join_key_separation(self):
        self.leftKeys, self.rightKeys = [], []
        for x in self.joinCondition:
            if id(x.left) in [id(col) for col in self.left.outputDict.flatCols()]:
                self.leftKeys.append(x.left)
            elif id(x.left) in [id(col) for col in self.right.outputDict.flatCols()]:
                self.rightKeys.append(x.left)
            else:
                raise Exception(f"Couldn't find the x.left value in either of the left and right tables!")
            
            if id(x.right) in [id(col) for col in self.left.outputDict.flatCols()]:
                self.leftKeys.append(x.right)
            elif id(x.right) in [id(col) for col in self.right.outputDict.flatCols()]:
                self.rightKeys.append(x.right)
            else:
                raise Exception(f"Couldn't find the x.right value in either of the left and right tables!")
            
        assert len(self.leftKeys) == len(self.rightKeys) == len(self.joinCondition)

    def add_third_node(self, node):
        assert self.third_node == None
        self.third_node = node
        
    def make_leftTableRef(self, unparser, lambda_index):
        leftTable, rightTable = self.getChildNames(unparser)
        # {'{leftKey}': {lambda_index}[0].{rightKey}}
        lr_pairs = []
        for idx, l_key in enumerate(self.leftKeys):
            lr_pairs.append(
                f"'{l_key}': {lambda_index}[0].{self.rightKeys[idx]}"
            )
        innerRecord = f"{{{', '.join(lr_pairs)}}}"
        return f"{leftTable}[record({innerRecord})]"

    def swapLeftAndRight(self):
        # Swap left and right
        new_right = self.left
        new_left = self.right
        
        self.left = new_left
        self.right = new_right
        
        # Run do_join_key_separation again
        self.do_join_key_separation()
    
# Classes for SDQLpy Constructs
class SDQLpyNKeyJoin():
    def __init__(self, leftNode, leftKeys, rightKeys):
        self.leftNode = leftNode
        assert (len(leftKeys) > 1)
        self.leftKeys = leftKeys
        assert (len(rightKeys) > 1)
        self.rightKeys = rightKeys
        self.tableName = None
        
    def setTableName(self, value):
        assert self.tableName == None
        self.tableName = value

class SDQLpySRDict():
    def __init__(self, keys, values):
        assert isinstance(keys, list)
        self.keys = self.reduceDuplicates(keys)
        assert isinstance(values, list)
        self.values = self.reduceDuplicates(values)
        self.third_wrap_counter = 0
        self.unique = False
        self.duplicateCounter = False
        self.duplicateUser = False
        
    def reduceDuplicates(self, items):
        # Remove duplicates in incoming list of items: either keys or values
        itemsById = defaultdict(list)
        itemIds = list()
        for item in items:
            itemIds.append(str(id(item)))
            itemsById[str(id(item))].append(item)
        
        # Get unique id order: 
        itemIdOrder = list(dict.fromkeys(itemIds))
        
        # Insert into newItems based of order of items
        newItems = []
        for itemId in itemIdOrder:
            # Only use first item
            newItems.append(itemsById.get(str(itemId))[0])
        return newItems
        
    def set_duplicateCounter(self, value):
        self.duplicateCounter = value
        
    def set_duplicateUser(self, value):
        self.duplicateUser = value
        
    def flatCols(self):
        return list(self.keys + self.values)
    
    def flatKeys(self):
        return list(self.keys)
    
    def flatVals(self):
        return list(self.values)
        
    def setUnique(self, value):
        self.unique = value
        
    def wrapColumns(self, col, third_node, third_cols, target_key):
        left_col, right_col, child_col = None, None, None
        if isinstance(col, BinaryExpressionOperator):
            left_col = self.wrapColumns(col.left, third_node, third_cols, target_key)
            right_col = self.wrapColumns(col.right, third_node, third_cols, target_key)
        elif isinstance(col, UnaryExpressionOperator):
            child_col = self.wrapColumns(col.child, third_node, third_cols, target_key)
        else:
            # A value node
            assert isinstance(col, LeafNode)
            pass
        
        # Assign previous changes
        if (left_col != None) and (right_col != None):
            col.left = left_col
            col.right = right_col
        elif (child_col != None):
            col.child = child_col
        else:
            # A leaf node
            pass
        
        match col:
            case ColumnValue():
                if col.codeName in third_cols != None:
                    new_col = SDQLpyThirdNodeWrapper(col, third_node, target_key)
                    self.third_wrap_counter += 1
                    return new_col
            
        return col
        
    def checkForThirdNodeColumns(self, third_node, target_keys):
        self.third_wrap_counter = 0
        # Checks the keys and columns
        # To see if theyre a part of the third node
        # If they are, it wraps them up in a third node wrapper
        # Increment the counter, so we know how many we've found
        
        assert len(target_keys) == 1
        target_key = target_keys[0]
        
        third_cols_str = [x.codeName for x in third_node.outputColumns]
        
        for idx, key in enumerate(self.keys):
            self.keys[idx] = self.wrapColumns(
                key, third_node, third_cols_str, target_key
            )
        for idx, col in enumerate(self.columns):
            self.values[idx] = self.wrapColumns(
                col, third_node, third_cols_str, target_key
            )
        
        return self.third_wrap_counter
        
        
    def generateSDQLpyTwoLambda(self, unparser, l_lambda_idx, r_lambda_idx_key, r_lambda_idx_val, l_node, r_node):
        l_columns = l_node.outputDict.flatCols()
        r_keys = r_node.outputDict.flatKeys()
        r_values = r_node.outputDict.flatVals()
        # Reduce keys and values
        self.keys = self.reduceDuplicates(self.keys)
        self.values = self.reduceDuplicates(self.values)
        
        # Assign sourceNode to the Column Values
        for key in self.keys:
            setSourceNodeColumnValues(key, l_lambda_idx, l_columns,
                                      r_lambda_idx_key, r_keys, r_lambda_idx_val, r_values)
        for col in self.values:
            setSourceNodeColumnValues(col, l_lambda_idx, l_columns,
                                      r_lambda_idx_key, r_keys, r_lambda_idx_val, r_values)
        
        output_content = self.generateSDQLpyContent(unparser)
        
        for key in self.keys:
            resetColumnValues(key)
        for col in self.values:
            resetColumnValues(col)
            
        return output_content
    
    def generateSDQLpyOneLambda(self, unparser, lambda_idx_key, lambda_idx_val, node):
        keys = node.incomingDict.flatKeys()
        values = node.incomingDict.flatVals()
        # Reduce keys and values
        self.keys = self.reduceDuplicates(self.keys)
        self.values = self.reduceDuplicates(self.values)
        # Assign sourceNode to the Column Values
        for key in self.keys:
            setSourceNodeColumnValues(key, lambda_idx_key, keys, lambda_idx_val, values)
        for col in self.values:
            setSourceNodeColumnValues(col, lambda_idx_key, keys, lambda_idx_val, values)
        
        # Update the self.values, if needed
        if self.duplicateUser:
            for idx, val in enumerate(self.values):
                newValue = MulOperator()
                newValue.codeName = val.codeName
                newValue.addLeft(val)
                rightValue = SDQLpyLambdaReference(lambda_idx_val)
                newValue.addRight(rightValue)
                self.values[idx] = newValue
        
        output_content = self.generateSDQLpyContent(unparser)
        
        for key in self.keys:
            resetColumnValues(key)
        for col in self.values:
            resetColumnValues(col)
            
        return output_content
    
    def counterAllValuesOne(self, counter):
        return all([x == 1 for x in counter.values()])
        
    def generateSDQLpyContent(self, unparser):
        output_content = []
        
        if self.keys == []:
            # If there are no keys, this should be an aggr output
            assert len(self.values) == 1
            output_content.append(
                f"{TAB}{unparser._UnparseSDQLpyTree__convert_expression_operator_to_sdqlpy(self.values[0])}"
            )
            return output_content
        
        output_content.append(
            f"{{"
        )
        
        keyCounter = defaultdict(int)
        writtenKeys = dict()
        
        # Process: Keys
        keyContent = []
        for key in self.keys:
            expr = unparser._UnparseSDQLpyTree__convert_expression_operator_to_sdqlpy(key)

            assert key.codeName != ''
            # Check if can write out key
            if key.codeName in writtenKeys:
                assert writtenKeys[key.codeName] == expr
            else:
                writtenKeys[key.codeName] = expr
                keyCounter[key.codeName] += 1
                keyContent.append(
                    f'"{key.codeName}": {expr}'
                )
        keyFormatted = f"record({{{', '.join(keyContent)}}})"
        if self.unique == True:
            keyFormatted = f"unique({keyFormatted})"
        output_content.append(
            f"{TAB}{keyFormatted}:"
        )
        
        assert self.counterAllValuesOne(keyCounter)
        
        # Process: Values
        if self.values == []:
            if self.duplicateCounter == True:
                # Insert a 1.0, so it can count up the duplicates
                output_content.append(
                    f"{TAB}{1.0}"
                )
            else:
                # If no columns, then write True
                output_content.append(
                    f"{TAB}{True}"
                )
        else:
            valueCounter = defaultdict(int)
            writtenValues = dict()
            colContent = []
            for val in self.values:
                if isinstance(val, (ColumnValue, CountAllOperator, SDQLpyThirdNodeWrapper,
                                    SumAggrOperator, MulOperator, ConstantValue)):
                    expr = unparser._UnparseSDQLpyTree__convert_expression_operator_to_sdqlpy(val)
                else:
                    expr = unparser._UnparseSDQLpyTree__convert_expression_operator_to_sdqlpy(val.child)

                assert val.codeName != ''
                # Check if can write out key
                if val.codeName in writtenValues:
                    assert writtenValues[val.codeName] == expr
                else:
                    writtenValues[val.codeName] = expr
                    valueCounter[val.codeName] += 1
                    colContent.append(
                        f'"{val.codeName}": {expr}'
                    )
            columnFormatted = f"record({{{', '.join(colContent)}}})"
            output_content.append(
                f"{TAB}{columnFormatted}"
            )
            
            assert self.counterAllValuesOne(Counter(valueCounter))
        
        output_content.append(
            f"}}"
        )
        
        return output_content
