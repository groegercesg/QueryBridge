from duckdb_nodes import *
from uplan_nodes import *

def duck_to_uplan(duck_tree):
    def visit_duck_nodes(op_node: DuckNode):
        # Visit Children
        if isinstance(op_node, BinaryDuckNode) and op_node.isJoinNode == True:
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
                assert isinstance(lowest_node_pointer, ScanNode)
                
        # Add hyperID to new_op_node
    #        assert hasattr(op_node, "hyperID")
        # Set as "-1", for now
        new_op_node.addID(op_node.nodeID)
        # Add cardinality to new_op_node
        assert hasattr(op_node, "cardinality")
        new_op_node.setCardinality(op_node.cardinality)
        
        return new_op_node

    return visit_duck_nodes(duck_tree)