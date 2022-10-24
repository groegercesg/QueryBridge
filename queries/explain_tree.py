def solve_nested_loop_node(tree):
    # Preorder traversal
    
    if tree.node_type == "Nested Loop":
        # Add in merge_cond attribute
        # Check the plans of Node, for an index scan, add on that only!
        merging_condition = None
        for individual_plan in tree.plans:
            if individual_plan.node_type == "Index Scan":
                if merging_condition != None:
                    # We have already found the merging condition
                    raise Exception
                else:
                    # We should flip this around at this point
                    # So originally index_cond will be:
                    # (a = b)
                    # And we should change it to:
                    # (b = a)
                    index = str(individual_plan.index_cond)
                    replace_brackets = False
                    if index[0] == "(" and index[-1] == ")":
                        # Strip brackets
                        index = index[1:-1]
                        replace_brackets = True
                        
                    index_split = str(index).split(" = ")
                    if len(index_split) != 2:
                        raise ValueError("There should be two sides to an index condition")
                    else:
                        new_index = str(index_split[1]) + " = " + str(index_split[0])
                        if replace_brackets:
                            new_index = "(" + new_index + ")"
                    
                    merging_condition = new_index
                    break
    
        if merging_condition != None:
            tree.add_merge_cond(merging_condition)
        else:
            raise Exception("Didn't find a merging condition to add to our Nested Loop node.")
    
    if tree.plans != None:
        for individual_plan in tree.plans:
            solve_nested_loop_node(individual_plan)
            
def solve_hash_node(tree):
    # Preorder Traversal
    # We have a tree with the potential for "Hash" nodes in it
    # We want to knock them out, specifically:
        # We are at the current node, we check the plans
        # If one of the plans is == "Hash"
            # We get the Hash's plans and set that in the place of "Hash"
            # This is okay to do as "Hash" only ever has one child
            # And "Hash" will never be the last node
            
    # Check current node
    if tree.plans != None:
        for i in range(len(tree.plans)):
            current_node = tree.plans[i]
            if current_node.node_type == "Hash":
                # We have found a Hash node
                # Get the Hash's children (child)
                if len(current_node.plans) == 1:
                    hashes_children = current_node.plans[0]
                else:
                    raise ValueError("We have assumed a Hash node only has one child, not the case in reality!")
                # Set current_node to be the children
                tree.plans[i] = hashes_children
    
    # Run this function on below nodes
    if tree.plans != None:
        for individual_plan in tree.plans:
            solve_hash_node(individual_plan)
    