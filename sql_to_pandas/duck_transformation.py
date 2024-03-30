from duckdb_nodes import *
from uplan_nodes import *

def duck_to_uplan(duck_tree):
    
    def remove_delim_scan(op_node):
        leftNode = None
        rightNode = None
        childNode = None
        if isinstance(op_node, BinaryBaseNode):
            leftNode = remove_delim_scan(op_node.left)
            rightNode = remove_delim_scan(op_node.right)
        elif isinstance(op_node, UnaryBaseNode):
            childNode = remove_delim_scan(op_node.child)
        else:
            # A leaf node
            pass
        
        if isinstance(op_node, DDelimScan):
            return True
        
        if leftNode == True:
            return rightNode
        elif rightNode == True:
            raise Exception()
        elif childNode == True:
            raise Exception()
        else:
            # Attach together
            if isinstance(op_node, BinaryBaseNode):
                op_node.left = leftNode
                op_node.right = rightNode
            elif isinstance(op_node, UnaryBaseNode):
                op_node.child = childNode
            
            return op_node
    
    def find_delim_scan(op_node):
        leftNode = False
        rightNode = False
        childNode = False
        if isinstance(op_node, BinaryBaseNode):
            leftNode = find_delim_scan(op_node.left)
            rightNode = find_delim_scan(op_node.right)
        elif isinstance(op_node, UnaryBaseNode):
            childNode = find_delim_scan(op_node.child)
        else:
            # A leaf node
            pass
        
        if isinstance(op_node, DDelimScan):
            return True
        elif isinstance(leftNode, ExpressionBaseNode):
            return leftNode
        elif isinstance(rightNode, ExpressionBaseNode):
            return rightNode
        elif isinstance(childNode, ExpressionBaseNode):
            return childNode
        
        
        if (leftNode == False) and (rightNode == False):
            return False
        elif (leftNode != False) or (rightNode != False):
            # Do interesting things
            return op_node.joinCondition
        elif (childNode == False):
            return False
        else:
            # Time to do something useful
            return None
    
    def visit_duck_nodes(op_node: DuckNode):
        # Visit Children
        
        if isinstance(op_node, TernaryDuckNode):
            leftNode = visit_duck_nodes(op_node.left)
            middleNode = visit_duck_nodes(op_node.middle)
            rightNode = visit_duck_nodes(op_node.right)
        elif isinstance(op_node, BinaryDuckNode):
            leftNode = visit_duck_nodes(op_node.left)
            rightNode = visit_duck_nodes(op_node.right)
        elif isinstance(op_node, UnaryDuckNode):
            childNode = visit_duck_nodes(op_node.child)
        else:
            # A leaf node
            pass
        
        # Create a 'new_op_node' from an existing 'op_node'
        match op_node:
            case DSeqScan():
                new_op_node = ScanNode(op_node.tableName, op_node.tableColumns, [], [])
                
                if op_node.condition != None:
                    filter_node = FilterNode(op_node.condition)
                    filter_node.addChild(new_op_node)
                    new_op_node = filter_node
            case DSimpleAggregate():
                child_ops = [x.child for x in op_node.aggregateOperations]
                new_op_node = GroupNode([], child_ops, op_node.aggregateOperations)
            case DChunkScan():
                new_op_node = op_node
            case DHashJoinNode():
                if isinstance(op_node.left, DChunkScan) or isinstance(op_node.right, DChunkScan):
                    new_op_node = FilterNode(op_node.joinCondition)
                    if isinstance(op_node.left, DChunkScan):
                        childNode = rightNode
                    elif isinstance(op_node.right, DChunkScan):
                        childNode = leftNode
                elif op_node.joinType == "MARK":
                    if (isinstance(op_node.joinCondition, EqualsOperator) and
                        isinstance(op_node.joinCondition.right, NotOperator) and 
                        isinstance(op_node.joinCondition.right.child, DSubqueryOp)):
                        new_cond = EqualsOperator()
                        new_cond.addLeft(op_node.leftKeys[0])
                        new_cond.addRight(op_node.rightKeys[0])
                        new_op_node = JoinNode("hash", "rightantijoin", new_cond, op_node.leftKeys, op_node.rightKeys)
                elif op_node.joinType == "INNER":
                    new_op_node = JoinNode("hash", "inner", op_node.joinCondition, op_node.leftKeys, op_node.rightKeys)
                elif op_node.joinType == "SEMI":
                    new_op_node = JoinNode("hash", "leftsemijoin", op_node.joinCondition, op_node.leftKeys, op_node.rightKeys)
                else:
                    raise Exception(f"Unrecognised Join Type: {op_node.joinType}")
            case DFilter():
                new_op_node = FilterNode(op_node.condition)
            case DHashGroupBy():
                child_ops = [x.child for x in op_node.aggregateOperations]
                new_op_node = GroupNode(op_node.keys, child_ops, op_node.aggregateOperations)
            case DDelimScan():
                new_op_node = op_node
            case DHashGroupByLeaf():
                new_op_node = op_node
            case DDelimJoin():
                # Left, Right
                leftCond = find_delim_scan(leftNode)
                middleCond = find_delim_scan(middleNode)
                if leftCond != False:
                    leftNode = remove_delim_scan(leftNode)
                elif middleCond != False:
                    middleNode = remove_delim_scan(middleNode)
                    middleNode = middleNode.child
                else:
                    raise Exception()
                
                new_join_type = None
                if op_node.joinType == "SEMI":
                    new_join_type = "leftsemijoin"
                elif op_node.joinType == "ANTI":
                    new_join_type = "leftantijoin"
                else:
                    raise Exception()
                
                lefts = [middleCond.left.left, middleCond.right.left]
                rights = [middleCond.left.right, middleCond.right.right]
                new_op_node = JoinNode("hash", new_join_type, middleCond, lefts, rights)
                
                # Overwrite leftNode and rightNode
                if isinstance(leftNode, DHashGroupByLeaf):
                    leftNode = middleNode
                    rightNode = rightNode
                elif isinstance(middleNode, DHashGroupByLeaf):
                    leftNode = leftNode
                    rightNode = rightNode
                elif isinstance(rightNode, DHashGroupByLeaf):
                    leftNode = leftNode
                    rightNode = middleNode
                else:
                    raise Exception("Couldn't find a 'DHashGroupByLeaf' node")
            case _: 
                raise Exception(f"Unexpected op_node, it was of class: {op_node.__class__}")

        # Overwrite the existing OpNode
        lowest_node_pointer = new_op_node
        # Find the lowest node
        searching = True
        while searching == True:
            match lowest_node_pointer:
                case UnaryBaseNode():
                    if lowest_node_pointer.child != None:
                        lowest_node_pointer = lowest_node_pointer.child
                    else:
                        searching = False
                case _:
                    searching = False
        
        # Add in the children
        match lowest_node_pointer:
            case UnaryBaseNode():
                lowest_node_pointer.addChild(childNode)
            case BinaryBaseNode():
                lowest_node_pointer.addLeft(leftNode)
                lowest_node_pointer.addRight(rightNode)
            case _:
                # ScanNode
                assert isinstance(lowest_node_pointer, (ScanNode, DChunkScan, DDelimScan, DHashGroupByLeaf))
                
        # Add hyperID to new_op_node
    #        assert hasattr(op_node, "hyperID")
        # Set as "-1", for now
        new_op_node.addID(op_node.nodeID)
        # Add cardinality to new_op_node
        assert hasattr(op_node, "cardinality")
        new_op_node.setCardinality(op_node.cardinality)
        
        return new_op_node

    return visit_duck_nodes(duck_tree)