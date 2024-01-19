from collections import Counter, defaultdict
import difflib

from uplan_nodes import *
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
        self.replacementDict = {}
        
        self.primaryKey = None
        self.foreignKeys = set()
        
        self.completedTables = set()
        self.foldedInto = False
    
    def updateReplacementDict(self, update_dictionary: dict):
        self.replacementDict.update(update_dictionary)
        
    def setReplacementDict(self, replacementDict):
        assert self.replacementDict == {}
        self.replacementDict = replacementDict
        
    def rd_outputDict(self, no_sumaggr_warn):
        if hasattr(self, "outputDict") and self.outputDict != None:
            for valuesLocation in [self.outputDict.keys, self.outputDict.values]:
                for idx, val in enumerate(valuesLocation):
                    if str(id(val)) in self.replacementDict:
                        # If it's in the replacementDict
                        # Then use the previously created value
                        valuesLocation[idx] = self.replacementDict[str(id(val))]      
                    elif isinstance(val, (SumAggrOperator)):
                        if no_sumaggr_warn:
                            pass
                        else:
                            print(str(id(val)))
                            raise Exception("Discovered a SumAggr, but we don't have any known replacement for this!")
                    # elif isinstance(val, (MaxAggrOperator, AvgAggrOperator, MinAggrOperator)):
                    #     raise Exception("Max/Min/Avg operator detected, we don't have support for these")

    def replaceInExpression(self, expression, replacements, no_sumaggr_warn):
        # Post Order traversal: Visit Children
        leftNode, rightNode, childNode = None, None, None
        if isinstance(expression, BinaryExpressionOperator):
            leftNode = self.replaceInExpression(expression.left, replacements, no_sumaggr_warn)
            rightNode = self.replaceInExpression(expression.right, replacements, no_sumaggr_warn)
        elif isinstance(expression, UnaryExpressionOperator):
            childNode = self.replaceInExpression(expression.child, replacements, no_sumaggr_warn)
        else:
            # A leaf node
            pass
        
        # Assign previous changes
        if (leftNode != None) and (rightNode != None):
            expression.left = leftNode
            expression.right = rightNode
        elif (childNode != None):
            expression.child = childNode
        else:
            # A leaf node
            pass
        
        if str(id(expression)) in replacements:
            expression = replacements[str(id(expression))]
        elif isinstance(expression, (SumAggrOperator)):
            if no_sumaggr_warn:
                pass
            else:
                raise Exception("Discovered a SumAggr, but we don't have any known replacement for this!")
        # elif isinstance(expression, (MaxAggrOperator, AvgAggrOperator, MinAggrOperator)):
        #     raise Exception("Max/Min/Avg operator detected, we don't have support for these")
            
        return expression

    def rd_filterContent(self, no_sumaggr_warn):
        if self.filterContent != None:
            newFilterContent = self.replaceInExpression(self.filterContent, self.replacementDict, no_sumaggr_warn)
            self.filterContent = newFilterContent
    
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
            self.tableName = "results"
        else:
            nodeNumber = unparser.nodesCounter[self.__class__.__name__]
            self.tableName = f"{self.sdqlrepr}_{str(nodeNumber)}"
            
        return self.tableName
    
    def refreshNode(self):
        pass
        
    def set_output_dict(self, no_sumaggr_warn=False):
        raise Exception("We should never run this Base class version")

class LeafSDQLpyNode(SDQLpyBaseNode):
    def __init__(self):
        super().__init__()
        
    def rd_tableColumns(self, no_sumaggr_warn):
        if hasattr(self, "tableColumns"):
            for idx, val in enumerate(self.tableColumns):
                if str(id(val)) in self.replacementDict:
                    self.tableColumns[idx] = self.replacementDict[str(id(val))]
                elif isinstance(val, (SumAggrOperator)):
                    if no_sumaggr_warn:
                        pass
                    else:
                        raise Exception("we shouldn't find a SumAggrOperation in tableColumns that we don't already know about")
                # elif isinstance(val, (MaxAggrOperator, AvgAggrOperator, MinAggrOperator)):
                #     raise Exception("Max/Min/Avg operator detected, we don't have support for these")

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
    
    def rd_aggregateOperations(self, no_sumaggr_warn):
        if hasattr(self, "aggregateOperations"):
            for idx, val in enumerate(self.aggregateOperations):
                self.aggregateOperations[idx] = self.replaceInExpression(val, self.replacementDict, no_sumaggr_warn)
                    
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
        
    def rd_joinCondition(self, no_sumaggr_warn):
        if hasattr(self, "joinCondition"):
            for idx, val in enumerate(self.joinCondition):
                if hasattr(self.joinCondition[idx], "left"):
                    self.joinCondition[idx].left = self.replaceInExpression(val.left, self.replacementDict, no_sumaggr_warn)
                if hasattr(self.joinCondition[idx], "right"):
                    self.joinCondition[idx].right = self.replaceInExpression(val.right, self.replacementDict, no_sumaggr_warn)
                
    def rd_leftRightKeys(self, no_sumaggr_warn):
        if hasattr(self, "leftKeys"):
            for idx, val in enumerate(self.leftKeys):
                self.leftKeys[idx] = self.replaceInExpression(val, self.replacementDict, no_sumaggr_warn)
        
        if hasattr(self, "rightKeys"):
            for idx, val in enumerate(self.rightKeys):
                self.rightKeys[idx] = self.replaceInExpression(val, self.replacementDict, no_sumaggr_warn)

# Classes for Nodes
class SDQLpyRecordNode(LeafSDQLpyNode):
    def __init__(self, tableName, tableColumns):
        super().__init__()
        self.tableName = tableName
        self.sdqlrepr = tableName
        # Filter for only essential columns
        self.oldTableColumns = tableColumns
        self.tableColumns = tableColumns
        # self.tableColumns = [x for x in self.oldTableColumns if x.essential == True]
        self.createInputOutputDicts()
        
    def filterTableColumns(self):
        # Filter tableColumns
        self.oldTableColumns = self.tableColumns
        self.tableColumns = []
        primaryKeyIDs = [id(x) for x in self.primaryKey]
        for x in self.oldTableColumns:
            if (x.essential == True) or (id(x) in primaryKeyIDs):
                self.tableColumns.append(x)
        self.createInputOutputDicts()
        
    def createInputOutputDicts(self):
        self.incomingDict = SDQLpySRDict(
            self.tableColumns,
            list()
        )
        self.outputDict = self.outputDict = SDQLpySRDict(
            self.tableColumns,
            list() 
        )
        
    def set_output_dict(self, no_sumaggr_warn=False):
        self.rd_outputDict(no_sumaggr_warn)
        self.rd_filterContent(no_sumaggr_warn)
        self.rd_tableColumns(no_sumaggr_warn)
        pass
        
    def getTableName(self, unparser):
        if self.filterContent != None:
            nodeNumber = unparser.nodesCounter[self.__class__.__name__]
            self.tableName = f"scan_{str(nodeNumber)}"
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
        
    def set_output_dict(self, no_sumaggr_warn=False):
        self.outputDict = SDQLpySRDict(
            self.tableKeys,
            self.child.outputDict.flatCols() 
        )
        
        self.rd_outputDict(no_sumaggr_warn)
        self.rd_filterContent(no_sumaggr_warn)

class SDQLpyPromoteToFloatNode(UnarySDQLpyNode):
    def __init__(self):
        super().__init__()
        self.outputDict = None
        self.sdqlrepr = "promote"
        self.singlePromote = False
        
    def set_output_dict(self, no_sumaggr_warn=False):
        assert self.child != None
        self.outputDict = SDQLpySRDict(
            self.child.outputDict.keys,
            self.child.outputDict.values
        )
        
        self.rd_outputDict(no_sumaggr_warn)
        self.rd_filterContent(no_sumaggr_warn)

class SDQLpyFilterNode(UnarySDQLpyNode):
    def __init__(self):
        super().__init__()
        self.outputDict = None
        self.sdqlrepr = "filter"
        
    def set_output_dict(self, no_sumaggr_warn=False):
        # A filter node should only have keys, no values
        assert self.child != None
        self.outputDict = SDQLpySRDict(
            self.child.outputDict.keys + self.child.outputDict.values,
            list()
        )
        
        self.rd_outputDict(no_sumaggr_warn)
        self.rd_filterContent(no_sumaggr_warn)

class SDQLpyAggrNode(UnarySDQLpyNode):
    def __init__(self, aggregateOperations):
        super().__init__()
        self.aggregateOperations = aggregateOperations
        self.sdqlrepr = "aggr"
        self.outputDict = None
        self.repeated_aggr = False
        
    def set_output_dict(self, no_sumaggr_warn=False):
        self.rd_aggregateOperations(no_sumaggr_warn)
        self.outputDict = SDQLpySRDict(
            list(),
            self.aggregateOperations
        )
        
        self.rd_outputDict(no_sumaggr_warn)
        self.rd_filterContent(no_sumaggr_warn)
        
    def set_repeated_aggr(self):
        self.repeated_aggr = True
        
class SDQLpyRetrieveNode(LeafSDQLpyNode):
    def __init__(self, tableColumns, targetID):
        super().__init__()
        self.tableColumns = tableColumns
        self.targetID = targetID
        self.outputDict = None
        self.sdqlrepr = "retrieve"
        
    def set_output_dict(self, no_sumaggr_warn=False):
        self.rd_filterContent(no_sumaggr_warn)
        self.rd_tableColumns(no_sumaggr_warn)
        self.outputDict = SDQLpySRDict(
            list(self.tableColumns),
            list()
        )
        
        self.rd_outputDict(no_sumaggr_warn)
        
class SDQLpyConcatNode(UnarySDQLpyNode):
    def __init__(self):
        super().__init__()
        self.sdqlrepr = "concat"
        self.outputDict = None
        
        self.output_dict_value_dict_size = False
        self.promote_to_float = False
        
    def set_output_dict(self, no_sumaggr_warn=False):
        self.outputDict = SDQLpySRDict(
            list(self.child.outputDict.flatCols()),
            list()
        )
        
        self.rd_outputDict(no_sumaggr_warn)
        self.rd_filterContent(no_sumaggr_warn)
    
class SDQLpyGroupNode(UnarySDQLpyNode):
    def __init__(self, keyExpressions, aggregateOperations):
        super().__init__()
        self.keyExpressions = keyExpressions
        self.aggregateOperations = aggregateOperations
        self.outputDict = None
        self.sdqlrepr = "group"
        
        self.output_dict_value_sr_dict = False
        
    def set_output_dict(self, no_sumaggr_warn=False):
        self.rd_aggregateOperations(no_sumaggr_warn)
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
        
        self.rd_outputDict(no_sumaggr_warn)
        self.rd_filterContent(no_sumaggr_warn)
        
class SDQLpyJoinNode(BinarySDQLpyNode):
    KNOWN_JOIN_METHODS = set([
        'hash', 'merge', 'bnl'
    ])
    KNOWN_JOIN_TYPES = set([
        'inner', 'rightsemijoin', 'leftsemijoin', 'rightantijoin', 'leftantijoin', 'outer'
    ])
    
    def __init__(self, joinMethod, joinType, incomingJoinCondition, incomingLeftKeys, incomingRightKeys):
        super().__init__()
        if joinMethod == None:
            joinMethod = "merge"
        assert joinMethod in self.KNOWN_JOIN_METHODS, f"{joinMethod} is not in the known join methods"
        self.joinMethod = joinMethod
        assert joinType in self.KNOWN_JOIN_TYPES, f"{joinType} is not in the known join types"
        self.joinType = joinType
        parsedJoinCondition = [incomingJoinCondition]
        assert isinstance(parsedJoinCondition, list) and len(parsedJoinCondition) > 0
        self.joinCondition = parsedJoinCondition
        self.outputDict = None
        self.sdqlrepr = "join"
        self.third_node = None
        self.is_update_sum = False
        
        self.equatingConditions = []
        self.comparingTree = None
        
        self.leftKeys = incomingLeftKeys
        self.rightKeys = incomingRightKeys
        
        self.output_dict_value_sr_dict = False
        
    def update_update_sum(self, newValue):
        assert isinstance(newValue, bool)
        self.is_update_sum = newValue
        
    def set_output_dict(self, no_sumaggr_warn=False):
        assert (self.left != None) and (self.right != None)
        # Track and carry previous outputDict value
        if self.outputDict == None:
            previousDuplicateCounter = False
        else:
            previousDuplicateCounter = self.outputDict.duplicateCounter
        
        match self.joinType:
            case "inner" | "outer":
                self.outputDict = SDQLpySRDict(
                    self.left.outputDict.keys + self.right.outputDict.keys +
                    self.left.outputDict.values + self.right.outputDict.values,
                    list()
                )
                self.outputDict.set_duplicateCounter(
                    previousDuplicateCounter
                )
            case "leftsemijoin":
                self.outputDict = SDQLpySRDict(
                    self.left.outputDict.keys +
                    self.left.outputDict.values,
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
            case "rightantijoin":
                self.outputDict = SDQLpySRDict(
                    self.right.outputDict.keys +
                    self.right.outputDict.values,
                    list()
                )
                self.outputDict.set_duplicateCounter(
                    previousDuplicateCounter
                )
            case "leftantijoin":
                self.outputDict = SDQLpySRDict(
                    self.left.outputDict.keys +
                    self.left.outputDict.values,
                    list()
                )
                self.outputDict.set_duplicateCounter(
                    previousDuplicateCounter
                )
            case _:
                raise Exception(f"No columns variable set for joinType: {self.joinType}")

        self.rd_outputDict(no_sumaggr_warn)
        self.rd_filterContent(no_sumaggr_warn)
        self.rd_joinCondition(no_sumaggr_warn)
        self.rd_leftRightKeys(no_sumaggr_warn)
        
        # Check for topNode and filter based on IDs
        if self.topNode == True:
            assert len(self.topNodeIds) > 0
            removeKeys = []
            removeValues = []
            for idx, key in enumerate(self.outputDict.keys):
                if id(key) not in self.topNodeIds:
                    removeKeys.append(idx)
            for idx, val in enumerate(self.outputDict.values):
                if id(val) not in self.topNodeIds:
                    removeValues.append(idx)
            
            removeKeys.sort(reverse=True)
            removeValues.sort(reverse=True)
            
            for key_pos in removeKeys:
                self.outputDict.keys.pop(key_pos)
            for val_pos in removeValues:
                self.outputDict.values.pop(val_pos)
                
            # Order it as well
            ordering = {k:v for v,k in enumerate(self.topNodeIds)}
            self.outputDict.keys.sort(key = lambda x : ordering.get(id(x), -1))
            self.outputDict.values.sort(key = lambda x : ordering.get(id(x), -1))

    def add_third_node(self, node):
        assert self.third_node == None
        self.third_node = node
        
    def set_sources_for_comparing_condition(self, left_source, right_key, right_value):
        
        # And those that should be used as part of the joinComparator
        # Set these this for ColumnValues or ones that are created
        
        def traverse_tree(comparing):
            if comparing.created == True:
                pass
            elif isinstance(comparing, BinaryExpressionOperator):
                traverse_tree(comparing.left)
                traverse_tree(comparing.right)
            elif isinstance(comparing, UnaryExpressionOperator):
                traverse_tree(comparing.child)
            else:
                pass
            
            if comparing.created == True or isinstance(comparing, (ColumnValue, IntervalNotionOperator, LikeOperator, LookupOperator)):
            
                # Set the sourceNode
                leftColumns = [str(col.codeName) for col in self.left.outputDict.flatCols()]
                rightKeys = [str(col.codeName) for col in self.right.outputDict.flatKeys()]
                rightValues = [str(col.codeName) for col in self.right.outputDict.flatVals()]
            
                if comparing.created == True or isinstance(comparing, ColumnValue):
                    
                    if comparing.codeName in leftColumns:
                        comparing.sourceNode = left_source
                    elif comparing.codeName in rightKeys:
                        comparing.sourceNode = right_key
                    elif comparing.codeName in rightValues:
                        comparing.sourceNode = right_value
                    else:
                        raise Exception(f"The comparing ({comparing.sourceNode}) was not found in either Left or Right.")
                elif isinstance(comparing, (IntervalNotionOperator, LikeOperator)):
                    if comparing.value.codeName in leftColumns:
                        comparing.value.sourceNode = left_source
                    elif comparing.value.codeName in rightKeys:
                        comparing.value.sourceNode = right_key
                    elif comparing.value.codeName in rightValues:
                        comparing.value.sourceNode = right_value
                    else:
                        raise Exception(f"The comparing ({comparing.sourceNode}) was not found in either Left or Right.")
                elif isinstance(comparing, LookupOperator):
                    for val in comparing.values:
                        if val.codeName in leftColumns:
                            val.sourceNode = left_source
                        elif val.codeName in rightKeys:
                            val.sourceNode = right_key
                        elif val.codeName in rightValues:
                            val.sourceNode = right_value
                        else:
                            raise Exception(f"The comparing ({comparing.sourceNode}) was not found in either Left or Right.") 
                else:
                    raise Exception("Unknown error")
            
        traverse_tree(self.comparingTree)
        
    def decompose_join_condition(self):
        # We have the joinConditions all bundled up
        # We need to split these into, conditions for the leftTableRef
        
        # The left table ref ones should be equating columns (== or !=)
        # The comparator ones should be anything else
        def expr_to_string(columns: list) -> list:
            returningList = []
            for col in columns:
                returningList.append(
                    col.codeName
                )
            return returningList
        
        def extract_column_equating(condition):
            current_equating = []
            
            if isinstance(condition, (AndOperator, OrOperator)):
                left_equating, left_condition = extract_column_equating(condition.left)
                right_equating, right_condition = extract_column_equating(condition.right)
                
                current_equating.extend(left_equating)
                current_equating.extend(right_equating)
                
                condition.left = left_condition
                condition.right = right_condition
            else:
                # Should be a workable value
                if isinstance(condition, (EqualsOperator, NotEqualsOperator)) and (
                    isinstance(condition.left, ColumnValue) and isinstance(condition.right, ColumnValue)
                ):
                    current_equating.append(condition)
                    condition = None
                    
            return current_equating, condition
        
        def rebalance_and_or_tree(condition):
            if isinstance(condition, BinaryExpressionOperator):
                left_condition = rebalance_and_or_tree(condition.left)
                right_condition = rebalance_and_or_tree(condition.right)
            
                condition.left = left_condition
                condition.right = right_condition
            else:
                pass
            
            if isinstance(condition, (AndOperator, OrOperator)):
                if (condition.left == None) and (condition.right == None):
                    return None
                elif condition.left == None:
                    condition = condition.right
                elif condition.right == None:
                    condition = condition.left
                else:
                    # No reworking needed
                    pass
                
            return condition
        
        assert len(self.joinCondition) == 1
        equating, stripped_join_condition = extract_column_equating(self.joinCondition[0])
                
        # Audit before assigning
        if equating != []:
            equating_types = Counter(equating)
            assert len(equating_types) == 1 and (equating_types[EqualsOperator()] > 0 or equating_types[NotEqualsOperator()] > 0)
        
        leftColumns = expr_to_string(self.left.outputDict.flatCols())
        rightColumns = expr_to_string(self.right.outputDict.flatCols())
        
        leftIDs = [id(x) for x in self.left.outputDict.flatCols()]
        rightIDs = [id(x) for x in self.right.outputDict.flatCols()]
        
        # Rotate equating if needed
        for x in equating:
            if id(x.left) in leftIDs and id(x.right) in rightIDs:
                # No rotate needed, all good
                pass
            elif id(x.left) in rightIDs and id(x.right) in leftIDs:
                oldLeft = x.left
                oldRight = x.right
                x.left = oldRight
                x.right = oldLeft
            else:
                if x.left.codeName in leftColumns and x.right.codeName in rightColumns:
                    # No rotate needed, all good
                    pass
                elif x.left.codeName in rightColumns and x.right.codeName in leftColumns:
                    oldLeft = x.left
                    oldRight = x.right
                    x.left = oldRight
                    x.right = oldLeft
                else:
                    raise Exception(f"Unknown format for x")
        
        # Rebalance stripped_join_condition    
        stripped_join_condition = rebalance_and_or_tree(stripped_join_condition)
         
        assert self.equatingConditions == [] and self.comparingTree == None
        self.equatingConditions = equating
        self.comparingTree = stripped_join_condition
        
    def make_leftTableRef(self, unparser, lambda_index):
        # Now we have decomposed the joinCondition into equating columns and comparing Conditions
        # For the leftTableRef, we use equating
        
        leftTable, rightTable = self.getChildNames(unparser)
        # {'{leftKey}': {lambda_index}[0].{rightKey}}
        lr_pairs = []
        # Iterate through equatingConditions
        for cond in self.equatingConditions:
            leftName = None
            rightName = None
            if cond.left.created == False:
                leftName = cond.left.value
            else:
                leftName = cond.left.codeName
            
            if cond.right.created == False:
                rightName = cond.right.value
            else:
                rightName = cond.right.codeName
            
            lr_pairs.append(
                f"'{leftName}': {lambda_index}[0].{rightName}"
            )
        innerRecord = f"{{{', '.join(lr_pairs)}}}"
        return f"{leftTable}[record({innerRecord})]"

    def swapLeftAndRight(self):
        # Swap left and right
        new_right = self.left
        new_left = self.right
        
        # Swap the keys as well
        oldLeftKeys = self.leftKeys
        oldRightKeys = self.rightKeys
        
        self.left = new_left
        self.right = new_right
        
        self.leftKeys = oldRightKeys
        self.rightKeys = oldLeftKeys
        
        # Swap equatingConditions
        for x in self.equatingConditions:
            oldLeft = x.left
            oldRight = x.right
            x.left = oldRight
            x.right = oldLeft
    
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
        
        self.value_sr_dict = False
        
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
    
    def generateSDQLpyFourLambda(self, unparser, l_lambda_idx_key, l_lambda_idx_val, r_lambda_idx_key, r_lambda_idx_val, node):
        l_node, r_node = node.left, node.right
        assert node.joinMethod == "bnl"
        
        l_keys_IDs = [id(x) for x in l_node.outputDict.flatKeys()]
        l_values_IDs = [id(x) for x in l_node.outputDict.flatVals()]
        r_keys_IDs = [id(x) for x in r_node.outputDict.flatKeys()]
        r_values_IDs = [id(x) for x in r_node.outputDict.flatVals()]
        
        # Reduce keys and values
        self.keys = self.reduceDuplicates(self.keys)
        self.values = self.reduceDuplicates(self.values)
        
        # Assign sourceNode to the Column Values
        for key in self.keys:
            setSourceNodeByIDs(key, l_lambda_idx_key, l_keys_IDs,
                               l_lambda_idx_val, l_values_IDs, 
                               r_lambda_idx_key, r_keys_IDs,
                               r_lambda_idx_val, r_values_IDs)
        for col in self.values:
            setSourceNodeByIDs(col, l_lambda_idx_key, l_keys_IDs,
                               l_lambda_idx_val, l_values_IDs, 
                               r_lambda_idx_key, r_keys_IDs,
                               r_lambda_idx_val, r_values_IDs)
        
        # Generate content
        output_content = self.generateSDQLpyContent(unparser)
        
        # Reset SourceNodes
        for key in self.keys:
            resetColumnValues(key)
        for col in self.values:
            resetColumnValues(col)
        
        return output_content
    
    def generateSDQLpyTwoLambda(self, unparser, l_lambda_idx, r_lambda_idx_key, r_lambda_idx_val, node):
        l_node, r_node = node.left, node.right
        
        # Create l/r_columns based on join type
        if "left" in node.joinType:
            l_columns = l_node.outputDict.flatCols()
            r_keys = []
            r_values = []
        elif "right" in node.joinType:
            l_columns = []
            r_keys = r_node.outputDict.flatKeys()
            r_values = r_node.outputDict.flatVals()
        else:
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
        if len(self.keys) == 0 and len(self.values) > 1:
            # If many values, check we're in an Aggr
            assert isinstance(node, SDQLpyAggrNode)
            #self.keys.append(self.values.pop(0))
        
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
            if len(self.values) == 1:
                output_content.append(
                    f"{TAB}{unparser._UnparseSDQLpyTree__convert_expression_operator_to_sdqlpy(self.values[0])}"
                )
                return output_content
            else:
                # We need to build a record just for these values
                valueCounter = defaultdict(int)
                writtenValues = dict()
                
                colContent = []
                for val in self.values:
                    if isinstance(val, (ColumnValue, CountAllOperator, SDQLpyThirdNodeWrapper,
                                        SumAggrOperator, MulOperator, ConstantValue, CaseOperator, DivOperator)):
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
                output_content = f"{TAB}record({{{', '.join(colContent)}}})"
                
                assert self.counterAllValuesOne(Counter(valueCounter))
                
                return [output_content]
        
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
        elif len(self.values) == 1 and self.value_sr_dict == True:
            # Make the value section an SR Dict
            val_codeName = self.values[0].child.codeName
            val_sourceNode = self.values[0].child.sourceNode
            
            output_content.append(f"{TAB}sr_dict({{{val_sourceNode}.{val_codeName}: True}})")
        else:
            assert self.value_sr_dict == False
            valueCounter = defaultdict(int)
            writtenValues = dict()
            colContent = []
            for val in self.values:
                if isinstance(val, (ColumnValue, CountAllOperator, SDQLpyThirdNodeWrapper,
                                    SumAggrOperator, MulOperator, ConstantValue, CaseOperator, DivOperator,
                                    SubOperator, AvgAggrOperator, MaxAggrOperator, MinAggrOperator)):
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
    
    def set_created(self, unparser):
        # Do codeName updates
        for expr in unparser.codeNameUpdates:
            expr.value = expr.codeName
        unparser.codeNameUpdates = []
        
        # Set all vals and keys to be created
        for key in self.keys:
            key.setCreated()
        for val in self.values:
            val.setCreated()
            
        # Check that all keys and values have a sourceNode of None
        for key in self.keys:
            if hasattr(key, "sourceNode"):
                assert key.sourceNode == None
        for val in self.values:
            if hasattr(val, "sourceNode"):
                assert val.sourceNode == None
        