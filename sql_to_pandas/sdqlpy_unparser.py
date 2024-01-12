from collections import defaultdict

from expression_operators import *

from sdqlpy_classes import *
from sdqlpy_helpers import *

TAB = "    "

# Unparser
class UnparseSDQLpyTree():
    def __init__(self, sdqlpy_tree: SDQLpyBaseNode) -> None:
        self.sdqlpy_content = []
        self.sdqlpy_temp_content = []
        self.nodesCounter = defaultdict(int)
        self.sdqlpy_tree = sdqlpy_tree
        
        self.relations = set()
        self.parserCreatedColumns = set()
        self.nodeDict = {}
        self.gatherNodeDict(self.sdqlpy_tree)
        
        self.codeNameUpdates = []
        
        # Variable dict
        self.variableDict = {}
        # Set top node of the sdqlpy_tree to True
        sdqlpy_tree.topNode = True
        self.doing_repeated_aggr = False
        
    def getChildTableNames(self, current: SDQLpyBaseNode) -> list[str]:
        childTables = []
        if isinstance(current, BinarySDQLpyNode):
            childTables.append(current.left.tableName)
            childTables.append(current.right.tableName)
        elif isinstance(current, UnarySDQLpyNode):
            childTables.append(current.child.tableName)
        else:
            # A leaf node
            pass
        
        return childTables
    
    def getOutputColumns(self) -> list:
        using_node = None
        if isinstance(self.sdqlpy_tree, SDQLpyConcatNode):
            using_node = self.sdqlpy_tree.child
        else:
            using_node = self.sdqlpy_tree
        
        assert using_node != None
        query_output_columns = []
        for expr in using_node.outputDict.flatCols():
            query_output_columns.append(
                expr.codeName
            )
            
        return query_output_columns
    
    def writeContent(self, content: str) -> None:
        self.sdqlpy_content.append(content)
    
    def writeTempContent(self, content: str) -> None:
        self.sdqlpy_temp_content.append(content)
    
    def commitTempContent(self) -> None:
        assert (self.sdqlpy_temp_content != [])
        for row in self.sdqlpy_temp_content:
            self.writeContent(row)
        self.sdqlpy_temp_content = []
        
    def getSDQLpyContent(self) -> list[str]:
        # At the start of this method, we should have captured no content
        assert self.sdqlpy_content == []
        # Then we walk the tree to gather it
        self.__walk_tree(self.sdqlpy_tree)
        
        # We need to return, with also the variableDict content
        variableDictContent = []
        for var_name, var_string in self.variableDict.items():
            variableDictContent.append(
                # Enclose the RHS in quotation marks
                f"{var_name} = '{var_string}'"
            )
        
        if variableDictContent != []:
            # Add an empty string at the end, to create a newline
            variableDictContent.append("")
        
        return variableDictContent + self.sdqlpy_content
    
    def gatherNodeDict(self, current_node):
        if isinstance(current_node, BinarySDQLpyNode):
            self.gatherNodeDict(current_node.left)
            self.gatherNodeDict(current_node.right)
        elif isinstance(current_node, UnarySDQLpyNode):
            self.gatherNodeDict(current_node.child)
        else:
            # A leaf node
            pass
        
        if current_node.nodeID != None:
            assert current_node.nodeID not in self.nodeDict
            self.nodeDict[current_node.nodeID] = current_node
    
    def __walk_tree(self, current_node):
        # Walk to children Children
        if isinstance(current_node, BinarySDQLpyNode):
            self.__walk_tree(current_node.left)
            self.__walk_tree(current_node.right)
        elif isinstance(current_node, UnarySDQLpyNode):
            self.__walk_tree(current_node.child)
        else:
            # A leaf node
            assert isinstance(current_node, LeafSDQLpyNode)
            pass
        
        # Visit the current_node and add it to self.pandas_content
        targetVisitorMethod = f"visit_{current_node.__class__.__name__}"
        # Refresh current_node's outputDict, before running
        # assert hasattr(current_node, "set_output_dict")
        # current_node.set_output_dict()
        if hasattr(self, targetVisitorMethod):
            # Count number of nodes
            self.nodesCounter[current_node.__class__.__name__] += 1
            getattr(self, targetVisitorMethod)(current_node)
        else:
            raise Exception(f"No visit method found for class name: {current_node.__class__.__name__}, was expected to find a: '{targetVisitorMethod}' method.")
        
    def visit_SDQLpyRetrieveNode(self, node):
        assert node.retrieveTargetID in self.nodeDict
        retrievedNode = self.nodeDict[node.retrieveTargetID]
        
        nodeTableColumnsCounter = Counter([type(x) for x in node.outputDict.flatCols()])
        retrievedNodeColumnsCounter = Counter([type(x) for x in retrievedNode.outputDict.flatCols()])
        
        assert len(node.outputDict.flatCols()) == len(retrievedNode.outputDict.flatCols())
        assert nodeTableColumnsCounter == retrievedNodeColumnsCounter
        assert len(set(nodeTableColumnsCounter.values())) <= 1, "All should have the same value"
        assert all(1 == x for x in nodeTableColumnsCounter.values()), "All should be 1"
        
        node.getTableName(self)
        node.tableName = retrievedNode.tableName
        
    def visit_SDQLpyFilterNode(self, node):
        # Get child name
        childTable = node.getChildName(self)
        
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        self.writeContent(
            f"{createdDictName} = {childTable}.sum(\n"
            f"{TAB}lambda {lambda_index} : "
        )
        
        for output_line in node.outputDict.generateSDQLpyOneLambda(
                self, f"{lambda_index}[0]", f"{lambda_index}[1]", node
            ):
                self.writeContent(
                    f"{TAB}{TAB}{output_line}"
                )
        
        assert node.filterContent != None, "A SDQLpyFilterNode should have some filterContent"
        
        # If there's a filter, then carry it out
        filterContent = self.convert_expr_to_sdqlpy(
            node.filterContent,
            f"{lambda_index}[0]",
            node.incomingDict.flatKeys(),
            f"{lambda_index}[1]",
            node.incomingDict.flatVals()
        )
        
        node.outputDict.set_created(self)
        
        self.writeContent(
            f"{TAB}if\n"
            f"{TAB}{TAB}{filterContent}\n"
            f"{TAB}else\n"
            f"{TAB}{TAB}None\n"
            f")"
        )
    
    def visit_SDQLpyRecordNode(self, node):
        self.relations.add(node.tableName)
        createdDictName = node.getTableName(self)
        
        if node.filterContent != None:
            originalTableName = node.sdqlrepr
            lambda_index = "p"
            
            self.writeContent(
                f"{createdDictName} = {originalTableName}.sum(\n"
                f"{TAB}lambda {lambda_index} : "
            )
            
            for output_line in node.outputDict.generateSDQLpyOneLambda(
                self, f"{lambda_index}[0]", f"{lambda_index}[1]", node
            ):
                self.writeContent(
                    f"{TAB}{TAB}{output_line}"
                )
            
            # If there's a filter, then carry it out
            filterContent = self.convert_expr_to_sdqlpy(
                node.filterContent,
                f"{lambda_index}[0]",
                node.incomingDict.flatKeys(),
                f"{lambda_index}[1]",
                node.incomingDict.flatVals()
            )
            
            node.outputDict.set_created(self)
            
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}None\n"
                f")"
            )
        
    def visit_SDQLpyConcatNode(self, node):
        # Get child name
        childTable = node.getChildName(self)
        
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        if node.output_dict_value_dict_size == True:
            # Use dictsize on the output_value
            self.writeContent(
                f"{createdDictName} = {childTable}.sum(lambda {lambda_index} : {{unique({lambda_index}[0].concat("
            )
            assert isinstance(node.outputDict.keys[-1], CountDistinctAggrOperator)
            val_codeName = node.outputDict.keys[-1].codeName
            self.writeContent(
                f"{TAB}record(\n"
                f"{TAB}{TAB}{{\n"
                f'{TAB}{TAB}{TAB}"{val_codeName}": dictSize({lambda_index}[1])\n'
                f"{TAB}{TAB}}}\n"
                f"{TAB})"
            )
            self.writeContent(
                ")): True})"
            )
        else:        
            # Do the summation at the end
            self.writeContent(
                f"{createdDictName} = {childTable}.sum(lambda {lambda_index} : {{unique({lambda_index}[0].concat({lambda_index}[1])): True}})"
            )
        
    def visit_SDQLpyJoinBuildNode(self, node):
        # Get child name
        childTable = node.getChildName(self)
        
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        self.writeContent(
            f"{createdDictName} = {childTable}.sum(\n"
            f"{TAB}lambda {lambda_index}:"
        )
        
        assert len(node.outputDict.keys) > 0
        
        for output_line in node.outputDict.generateSDQLpyOneLambda(
            self, f"{lambda_index}[0]", f"{lambda_index}[1]", node
        ):
            self.writeContent(
                f"{TAB}{TAB}{output_line}"
            )
            
        if node.filterContent != None:
            filterContent = self.convert_expr_to_sdqlpy(
                node.filterContent,
                f"{lambda_index}[0]",
                node.incomingDict.flatKeys(),
                f"{lambda_index}[1]",
                node.incomingDict.flatVals()
            )
            
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}None"
            )
        
        node.outputDict.set_created(self)
        
        self.writeContent(
            f")"
        )
        
    def visit_SDQLpyJoinNode(self, node):
        # If it has a third_node, we should run that
        if node.third_node != None:
            self.__walk_tree(node.third_node)
        
        # Get child name
        leftTable, rightTable = node.getChildNames(self)
        
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        # TODO: We only support an inner hash join at the moment
        assert node.joinType in node.KNOWN_JOIN_TYPES
        # assert node.joinMethod == "hash"
        
        # Check its a Valid setup
        if ((isinstance(node.left, (SDQLpyJoinBuildNode, SDQLpyAggrNode))) or (isinstance(node.left, (SDQLpyFilterNode, SDQLpyJoinNode)) and node.left.foldedInto == True)) and (isinstance(node.right, (SDQLpyRecordNode, SDQLpyJoinNode, SDQLpyFilterNode, SDQLpyConcatNode))):
            pass
        else:
            raise Exception("Invalid/Unsupported Left and Right Layout")
        
        self.writeTempContent(
            f"{createdDictName} = {rightTable}.sum(\n"
            f"{TAB}lambda {lambda_index} : "
        )
        
        # Carry over the value sr_dict
        node.outputDict.value_sr_dict = node.output_dict_value_sr_dict
        
        leftTableRef = node.make_leftTableRef(self, lambda_index)
        
        # Write the output Record
        for output_line in node.outputDict.generateSDQLpyTwoLambda(
            self, leftTableRef, f"{lambda_index}[0]", f"{lambda_index}[1]",
            node
        ):
            self.writeTempContent(
                f"{TAB}{TAB}{output_line}"
            )
        
        joinComparator = "!="
        if "anti" in node.joinType:
            joinComparator = "=="
        
        # Add the other joinComparison
        if node.equatingConditions != [] and node.comparingTree == None:
            self.writeTempContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{leftTableRef} {joinComparator} None\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}None"
            )
        elif node.equatingConditions == [] and node.comparingTree != None:
            # A Non-equi join
            # Assign sources for the comparing condition
            node.set_sources_for_comparing_condition(leftTableRef, f"{lambda_index}[0]", f"{lambda_index}[1]")
            
            if isinstance(node.left, SDQLpyAggrNode) and node.left.repeated_aggr == True:
                # Change the codeName when the sourceNode is
                self.traverse_to_change_codeName_when_sourceNode(node.comparingTree, leftTable, leftTableRef)
                # Convert the equation
                nonEquiJoinCondition = self.__convert_expression_operator_to_sdqlpy(node.comparingTree)
            else:
                nonEquiJoinCondition = self.__convert_expression_operator_to_sdqlpy(node.comparingTree)
            
            # reset sources, so as to not cause issues later down the line
            resetColumnValues(node.comparingTree)
            
            self.writeTempContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{nonEquiJoinCondition}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}None"
            )
            
        elif node.equatingConditions != [] and node.comparingTree != None:
            # Assign sources for the comparing condition
            node.set_sources_for_comparing_condition(leftTableRef, f"{lambda_index}[0]", f"{lambda_index}[1]")
            otherJoinComparison = self.__convert_expression_operator_to_sdqlpy(node.comparingTree)
            # reset sources, so as to not cause issues later down the line
            resetColumnValues(node.comparingTree)
            
            self.writeTempContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{leftTableRef} {joinComparator} None and ({otherJoinComparison})\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}None"
            )
        else:
            assert node.equatingConditions == [] and node.comparingTree == None
            raise Exception(f"Illogical format of join")
            
        node.outputDict.set_created(self)
        
        # Filter Content
        assert node.filterContent == None
        assert hasattr(node, "postJoinFilters") == False
            
        self.writeTempContent(
            f")"
        )
        # Commit Temp Content, now that we're finished
        self.commitTempContent()
        
    def visit_SDQLpyGroupNode(self, node):
        # Get child name
        childTable = node.getChildName(self)
        
        initialDictName = node.getTableName(self, not_output = True)
        lambda_index = "p"
        
        self.writeContent(
            f"{initialDictName} = {childTable}.sum(lambda {lambda_index} :"
        )
        
        # Carry over the value sr_dict
        node.outputDict.value_sr_dict = node.output_dict_value_sr_dict
        
        # Output the RecordOutput
        for output_line in node.outputDict.generateSDQLpyOneLambda(
            self, f"{lambda_index}[0]", f"{lambda_index}[1]", node
        ):
            self.writeContent(
                f"{TAB}{output_line}"
            )
        
        # Write filterContent, if we have it
        if node.filterContent != None:
            filterContent = self.convert_expr_to_sdqlpy(
                node.filterContent,
                f"{lambda_index}[0]",
                node.incomingDict.flatKeys(),
                f"{lambda_index}[1]",
                node.incomingDict.flatVals()
            )
            
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}None"
            )
        
        node.outputDict.set_created(self)
        
        self.writeContent(
            f")"
        )
            
    def visit_SDQLpyAggrNode(self, node):        
        # Get child name
        childTable = node.getChildName(self)
        
        createdDictName = node.getTableName(self)
        lambda_index = "p"
        
        if node.repeated_aggr == True:
            # Do repeated aggr mode
            # This is essentially just the equation
            
            # Check that the current node and child are the right size
            assert len(node.outputDict.flatCols()) == 1
            assert len(node.outputDict.flatKeys()) == 0
            
            # Convert the codeName for all columnValues or created to the childTable
            aggr_equation = None
            if len(node.child.outputDict.flatCols()) == 1:
                # If there's only one child column, then change the codeName
                equation_to_output = node.outputDict.values[0]
                self.traverse_to_change_codeName(equation_to_output, childTable)
                # Convert the equation
                self.doing_repeated_aggr = True
                aggr_equation = self.__convert_expression_operator_to_sdqlpy(equation_to_output)
                self.doing_repeated_aggr = False
            else:
                # Otherwise, more than one child column, we need to:
                #   - Set the sourceNode as childTable
                equation_to_output = node.outputDict.values[0]
                self.traverse_to_set_sourceNode(equation_to_output, childTable)
                aggr_equation = self.__convert_expression_operator_to_sdqlpy(equation_to_output)
                # Reset sourceNode
                resetColumnValues(equation_to_output)
            
            assert aggr_equation != None
            self.writeContent(
                f"{createdDictName} = {aggr_equation}"
            )
            
        else:
            self.writeContent(
                f"{createdDictName} = {childTable}.sum(\n"
                f"{TAB}lambda {lambda_index} :"
            )
            
            # Output the RecordOutput
            for output_line in node.outputDict.generateSDQLpyOneLambda(
                self, f"{lambda_index}[0]", f"{lambda_index}[1]", node
            ):
                self.writeContent(
                    f"{TAB}{output_line}"
                )
        
        if node.filterContent != None:
            assert node.repeated_aggr == False
            filterContent = self.convert_expr_to_sdqlpy(
                node.filterContent,
                f"{lambda_index}[0]",
                node.incomingDict.flatKeys(),
                f"{lambda_index}[1]",
                node.incomingDict.flatVals()
            )
            
            self.writeContent(
                f"{TAB}if\n"
                f"{TAB}{TAB}{filterContent}\n"
                f"{TAB}else\n"
                f"{TAB}{TAB}0.0"
            )
        else:
            # Write a 'if True' filter to trick the compiler
            if (len(node.outputDict.flatVals()) >= 2) and (len(node.outputDict.flatKeys()) == 0):
                self.writeContent(
                    f"{TAB}if\n"
                    f"{TAB}{TAB}True\n"
                    f"{TAB}else\n"
                    f"{TAB}{TAB}None"
                )
            
        node.outputDict.set_created(self)
        
        if node.repeated_aggr != True:
            self.writeContent(
                f")"
            )
    
    # =========================
    # Unparser helper functions
    
    def traverse_to_change_codeName_when_sourceNode(self, value, newCodeName, checkingSourceNode):
        if isinstance(value, BinaryExpressionOperator):
            self.traverse_to_change_codeName_when_sourceNode(value.left, newCodeName, checkingSourceNode)
            self.traverse_to_change_codeName_when_sourceNode(value.right, newCodeName, checkingSourceNode)
        elif isinstance(value, UnaryExpressionOperator):
            self.traverse_to_change_codeName_when_sourceNode(value.child, newCodeName, checkingSourceNode)
        else:
            pass
            
        if hasattr(value, "sourceNode") and value.sourceNode == checkingSourceNode:
            value.codeName = newCodeName
            value.no_source = True
            
    def traverse_to_set_sourceNode(self, value, newSourceNode):
        if value.created == True:
            pass
        elif isinstance(value, BinaryExpressionOperator):
            self.traverse_to_set_sourceNode(value.left, newSourceNode)
            self.traverse_to_set_sourceNode(value.right, newSourceNode)
        elif isinstance(value, UnaryExpressionOperator):
            self.traverse_to_set_sourceNode(value.child, newSourceNode)
        else:
            pass
            
        if value.created == True or isinstance(value, ColumnValue):
            value.sourceNode = newSourceNode
    
    def traverse_to_change_codeName(self, value, newCodeName):
        if value.created == True:
            pass
        elif isinstance(value, BinaryExpressionOperator):
            self.traverse_to_change_codeName(value.left, newCodeName)
            self.traverse_to_change_codeName(value.right, newCodeName)
        elif isinstance(value, UnaryExpressionOperator):
            self.traverse_to_change_codeName(value.child, newCodeName)
        else:
            pass
            
        if value.created == True or isinstance(value, ColumnValue):
            value.codeName = newCodeName
    
    def convert_nkeyjoin_to_sdqlpy(self, value, l_lambda_idx, l_node_columns, r_lambda_idx = None, r_node_columns = None):
        assert isinstance(value, SDQLpyNKeyJoin)
        self.visit_SDQLpyNKeyJoin(value)
        
        for r_key in value.rightKeys:
            setSourceNodeColumnValues(r_key, l_lambda_idx, l_node_columns, r_lambda_idx, r_node_columns)
        expr_content = self.__convert_expression_operator_to_sdqlpy(value)
        for r_key in value.rightKeys:
            resetColumnValues(r_key)
        
        return expr_content
    
    def convert_expr_to_sdqlpy(self, value, l_lambda_idx, l_node_columns, r_lambda_idx = None, r_node_columns = None):
        setSourceNodeColumnValues(value, l_lambda_idx, l_node_columns, r_lambda_idx, r_node_columns)
        expr_content = self.__convert_expression_operator_to_sdqlpy(value)
        resetColumnValues(value)
        
        return expr_content
    
    def __convert_expression_operator_to_sdqlpy(self, expr_tree: ExpressionBaseNode) -> str:
        # Visit Children
        if expr_tree.created == True and isinstance(expr_tree, MulOperator):
            pass
        elif isinstance(expr_tree, BinaryExpressionOperator):
            leftNode = self.__convert_expression_operator_to_sdqlpy(expr_tree.left)
            rightNode = self.__convert_expression_operator_to_sdqlpy(expr_tree.right)
        elif isinstance(expr_tree, UnaryExpressionOperator):
            childNode = self.__convert_expression_operator_to_sdqlpy(expr_tree.child)
        else:
            # A value node
            assert isinstance(expr_tree, (LeafNode, SDQLpyNKeyJoin))
            pass
        
        expression_output = None
        match expr_tree:
            case ColumnValue():
                if self.doing_repeated_aggr == True or expr_tree.no_source == True:
                    expression_output = f"{expr_tree.codeName}"
                else:
                    assert expr_tree.sourceNode != None
                    expression_output = f"{expr_tree.sourceNode}.{expr_tree.value}"
                    # Update the value after creation
                    # Add to codeNameUpdates
                    self.codeNameUpdates.append(
                        (expr_tree)
                    )
            case ConstantValue():
                expression_output = self.__handle_ConstantValue(expr_tree)
            case LessThanOperator():
                expression_output = f"({leftNode} < {rightNode})"
            case LessThanEqOperator():
                expression_output = f"({leftNode} <= {rightNode})"
            case GreaterThanOperator():
                expression_output = f"({leftNode} > {rightNode})"
            case GreaterThanEqOperator():
                expression_output = f"({leftNode} >= {rightNode})"
            case IntervalNotionOperator():
                expression_output = self.__handle_IntervalNotion(expr_tree)
            case AndOperator():
                expression_output = f"({leftNode} and {rightNode})"
            case OrOperator():
                expression_output = f"({leftNode} or {rightNode})"
            case DivOperator():
                expression_output = f"{leftNode} / {rightNode}"
            case MulOperator():
                if expr_tree.created == False:
                    expression_output = f"{leftNode} * {rightNode}"
                elif expr_tree.no_source == True:
                    expression_output = f"{expr_tree.codeName}"
                else:
                    expression_output = f"{expr_tree.sourceNode}.{expr_tree.codeName}"
            case SubOperator():
                expression_output = f"({leftNode} - {rightNode})"
            case AddOperator():
                expression_output = f"({leftNode} + {rightNode})"
            case CountAllOperator():
                expression_output = "1.0"
            case EqualsOperator():
                expression_output = self.__handle_EqualsOperator(expr_tree, leftNode, rightNode)
            case NotEqualsOperator():
                expression_output = f"({leftNode} != {rightNode})"
            case SDQLpyThirdNodeWrapper():
                expression_output = self.__handle_SDQLpyThirdNodeWrapper(expr_tree)
            case InSetOperator():
                expression_output = self.__handle_InSetOperator(expr_tree)
            case SDQLpyNKeyJoin():
                expression_output = self.__handle_SDQLpyNKeyJoin(expr_tree)
            case SDQLpyLambdaReference():
                expression_output = f"{expr_tree.value}"
            case NotOperator():
                expression_output = f"({childNode} == False)"
            case CountDistinctAggrOperator():
                expression_output = f"dictSize({childNode})"
            case LikeOperator():
                expression_output = self.__handle_LikeOperator(expr_tree)
            case ExtractYearOperator():
                expression_output = self.__handle_ExtractYearOperator(expr_tree)
            case SDQLpyFirstIndex():
                expression_output = self.__handle_SDQLpyFirstIndex(expr_tree, leftNode, rightNode)
            case SDQLpyStartsWith():
                expression_output = self.__handle_SDQLpyStartsWith(expr_tree, leftNode, rightNode)
            case SDQLpyEndsWith():
                expression_output = self.__handle_SDQLpyEndsWith(expr_tree, leftNode, rightNode)
            case CaseOperator():
                expression_output = self.__handle_CaseOperator(expr_tree)
            case _: 
                raise Exception(f"Unrecognised expression operator: {type(expr_tree)}")

        return expression_output
    
    def __handle_EqualsOperator(self, expr: EqualsOperator, leftValue: str, rightValue: str) -> str:
        if expr.left.type == "Float" and expr.right.type == "Float":
            raise Exception("Float to float comparison")
        elif expr.left.type == "Float" and expr.right.type != "Float":
            raise Exception("Left float comparison")
        elif expr.left.type != "Float" and expr.right.type == "Float":
            # Handle the right being a float
            # Rewrite it into the "close" statement
            closeThreshold = ConstantValue(0.01, "Float")
            zeroValue = ConstantValue(0.0, "Float")
            
            # Left Portion
            leftMinus = SubOperator()
            leftMinus.addLeft(expr.left)
            leftMinus.addRight(expr.right)
            leftCompare = LessThanOperator()
            leftCompare.addLeft(leftMinus)
            leftCompare.addRight(closeThreshold)
            leftZero = GreaterThanEqOperator()
            leftZero.addLeft(leftMinus)
            leftZero.addRight(zeroValue)
            
            leftAnd = AndOperator()
            leftAnd.addLeft(leftCompare)
            leftAnd.addRight(leftZero)
            
            # Right Portion
            rightMinus = SubOperator()
            rightMinus.addLeft(expr.right)
            rightMinus.addRight(expr.left)
            rightCompare = LessThanOperator()
            rightCompare.addLeft(rightMinus)
            rightCompare.addRight(closeThreshold)
            rightZero = GreaterThanEqOperator()
            rightZero.addLeft(rightMinus)
            rightZero.addRight(zeroValue)
            
            rightAnd = AndOperator()
            rightAnd.addLeft(rightCompare)
            rightAnd.addRight(rightZero)
            
            # Overall portion
            overallClose = OrOperator()
            overallClose.addLeft(leftAnd)
            overallClose.addRight(rightAnd)
            expression_output = self.__convert_expression_operator_to_sdqlpy(overallClose)
        else:
            expression_output = f"({leftValue} == {rightValue})"
        return expression_output
    
    def __handle_CaseOperator(self, expr: CaseOperator) -> str:
        assert len(expr.caseInstances) == 1
        outputValue = self.__convert_expression_operator_to_sdqlpy(expr.caseInstances[0].outputValue)
        caseValue = self.__convert_expression_operator_to_sdqlpy(expr.caseInstances[0].case)
        elseValue = self.__convert_expression_operator_to_sdqlpy(expr.elseExpr)
        # [caseInstances[0].outputValue] if [caseInstances[0].case] else [elseExpr] 
        return f"{outputValue} if {caseValue} else {elseValue}"
    
    def __handle_SDQLpyFirstIndex(self, expr: SDQLpyFirstIndex, leftValue: str, rightValue: str) -> str:
        return f"firstIndex({leftValue}, {rightValue})"
    
    def __handle_SDQLpyStartsWith(self, expr: SDQLpyStartsWith, leftValue: str, rightValue: str) -> str:
        return f"startsWith({leftValue}, {rightValue})"
    
    def __handle_SDQLpyEndsWith(self, expr: SDQLpyEndsWith, leftValue: str, rightValue: str) -> str:
        return f"endsWith({leftValue}, {rightValue})"
    
    def __handle_ExtractYearOperator(self, expr: ExtractYearOperator):
        childValue = self.__convert_expression_operator_to_sdqlpy(expr.child)
        return f"extractYear({childValue})"
    
    def __handle_LikeOperator(self, expr: LikeOperator):
        # Process the comparator
        if expr.comparator.value.count("%") == 1:
            if expr.comparator.value[-1] == "%":
                # startsWith(p[0].p_type, medpol) == False
                expr.comparator.value = expr.comparator.value.replace("%", "")
                
                startsWith = SDQLpyStartsWith()
                startsWith.addLeft(expr.value)
                startsWith.addRight(expr.comparator)
                return self.__convert_expression_operator_to_sdqlpy(startsWith)
            elif expr.comparator.value[0] == "%":
                # endsWith(p[0].p_type, brass) == False
                expr.comparator.value = expr.comparator.value.replace("%", "")
                
                startsWith = SDQLpyEndsWith()
                startsWith.addLeft(expr.value)
                startsWith.addRight(expr.comparator)
                return self.__convert_expression_operator_to_sdqlpy(startsWith)
            
            else:
                raise Exception("Unexpected format of Universal LikeOperator expression")
        elif expr.comparator.value.count("%") == 2:
            assert isinstance(expr.comparator, ConstantValue)
            assert isinstance(expr.value, ColumnValue)
            
            expr.comparator.value = expr.comparator.value.replace("%", "")
            
            # 2 percentages means "in"
            comparator_value = self.__convert_expression_operator_to_sdqlpy(expr.comparator)
            column_value = self.__convert_expression_operator_to_sdqlpy(expr.value)
            return f"{comparator_value} in {column_value}"
        elif expr.comparator.value.count("%") > 2:
            assert expr.comparator.value[0] == "%" and expr.comparator.value[-1] == "%"
            # use FirstIndex
            # Split comparator into many values
            many_comparators = list(filter(None, expr.comparator.value.split("%")))
            
            comparator_values = []
            for comparator_string in many_comparators:
                comparator = ConstantValue(comparator_string, expr.comparator.type)
                comparator_values.append(comparator)
            
            # Check the First Index of Each aren't -1
            comparators_checks = []
            for comparator in comparator_values:
                # Create comparator Value
                firstIndex = SDQLpyFirstIndex()
                firstIndex.left = expr.value
                firstIndex.right = comparator
                notEqual = NotEqualsOperator()
                notEqual.left = firstIndex
                minusOne = ConstantValue(-1, "Integer")
                minusOne.setForceInteger(True)
                notEqual.right = minusOne
                comparators_checks.append(notEqual)
                
            # Reverse comparator_values
            comparator_values.reverse()
            # Check for each comparator, the firstIndex is > firstIndex of the Next + (len(next) - 1)
            for i in range(0, len(comparator_values) - 1):
                leftPosition = SDQLpyFirstIndex()
                leftPosition.left = expr.value
                leftPosition.right = comparator_values[i]
                rightPostion = SDQLpyFirstIndex()
                rightPostion.left = expr.value
                rightPostion.right = comparator_values[i + 1]
                rightLocation = AddOperator()
                rightLocation.left = rightPostion
                rightSize = ConstantValue(len(comparator_values[i + 1].value) - 1, "Integer")
                rightSize.setForceInteger(True)
                rightLocation.right = rightSize
                positionCompare = GreaterThanOperator()
                positionCompare.left = leftPosition
                positionCompare.right = rightLocation
                comparators_checks.append(positionCompare)
            
            # Join with ANDs
            and_join = join_statements_with_operator(comparators_checks, "AndOperator")
            
            return self.__convert_expression_operator_to_sdqlpy(and_join)
        else:
            # Unknown number of percentages
            # Should use "startsWith"/"endsWith" macros from SDQLpy
            raise Exception(f"Unknown format of comparator: {expr.comparator.value}")
    
    def __handle_ConstantValue(self, expr: ConstantValue):
        if expr.type == "String":
            # Save value in variableDict
            variableString = str(expr.value)
            newVariable = variableString.replace(" ", "_").replace("#", "").replace("-", "").lower()
            # Check it's not all integers
            assert not newVariable.isdigit()
            # Fix start with integer
            while newVariable[0].isdigit():
                oldStart = newVariable[0]
                newVariable = newVariable[1:] + oldStart
            if newVariable in self.variableDict:
                if self.variableDict.get(newVariable) == variableString:
                    # The same string is happening twice, just use the existing created reference
                    return f"{newVariable}"
                else:
                    # We need to complicate our variable naming futher
                    raise Exception("Variable name corresponds to different strings")
            else:
                self.variableDict[newVariable] = variableString
                return f"{newVariable}"
        elif expr.type == "Float":
            return expr.value
        elif expr.type == "Datetime":
            year = str(expr.value.year).zfill(4)
            month = str(expr.value.month).zfill(2)
            day = str(expr.value.day).zfill(2)
            return f"{year}{month}{day}"
        elif expr.type == "Integer":
            if expr.forceInteger == True:
                return f'{expr.value}'
            else:
                return f'{expr.value}.0'
        elif expr.type == "Bool":
            return expr.value
        else:
            raise Exception(f"Unknown Constant Value Type: {expr.type}")
            
    def __handle_IntervalNotion(self, expr: IntervalNotionOperator):
        match expr.mode:
            case "[]":
                leftExpr = GreaterThanEqOperator()
                rightExpr = LessThanEqOperator()
            case "()":
                leftExpr = GreaterThanOperator()
                rightExpr = LessThanOperator()
            case "[)":
                leftExpr = GreaterThanEqOperator()
                rightExpr = LessThanOperator()
            case "(]":
                leftExpr = GreaterThanOperator()
                rightExpr = LessThanEqOperator()
            case _:
                raise Exception(f"Unknown Internal Notion operator: {expr.mode}")
        
        leftExpr.left = expr.value
        leftExpr.right = expr.left
                
        rightExpr.left = expr.value
        rightExpr.right = expr.right
        
        convertedExpression = AndOperator()
        convertedExpression.left = leftExpr
        convertedExpression.right = rightExpr
        
        sdqlpyExpression = self.__convert_expression_operator_to_sdqlpy(convertedExpression)
        return sdqlpyExpression
        
    def __handle_SDQLpyThirdNodeWrapper(self, expr_tree):
        outputString = f"{expr_tree.third_node.tableName}[{expr_tree.sourceNode}.{expr_tree.target_key.codeName}].{expr_tree.col.codeName}"
        return outputString
        
    def __handle_InSetOperator(self, expr: InSetOperator):
        # rewrite as child == set[0] or child == set[1]
        equating = []
        for set_opt in expr.set:
            eq_op = EqualsOperator()
            eq_op.addLeft(expr.child)
            eq_op.addRight(set_opt)
            equating.append(
                eq_op
            )
        
        or_tree = join_statements_with_operator(equating, "OrOperator")
        
        return f"({self.__convert_expression_operator_to_sdqlpy(or_tree)})"

    def __handle_SDQLpyNKeyJoin(self, expr_tree):
        keyContent = []
        for r_key in expr_tree.rightKeys:
            expr = self._UnparseSDQLpyTree__convert_expression_operator_to_sdqlpy(r_key)
            keyContent.append(
                f'"{r_key.codeName}": {expr}'
            )
        rightKeyFormatted = f"{', '.join(keyContent)}"
        outputString = f"{expr_tree.tableName}[record({{{rightKeyFormatted}}})] != None"
        return outputString
