from plan_to_explain_tree import * 

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
    
    
def make_tree(json, tree):
    # First node check
    if tree == None:
        node = json["Plan"]
    else:
        node = json

    node_class = None    
    node_type = node["Node Type"]
    if node_type.lower() == "limit":
        node_class = limit_node(node_type, node["Parallel Aware"], node["Async Capable"], node["Output"])
    elif node_type.lower() == "aggregate":
        if "Group Key" in node:
            node_class = group_aggregate_node("Group " + node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Strategy"], node["Partial Mode"], node["Parent Relationship"], node["Group Key"])
        else:
            node_class = aggregate_node(node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Strategy"], node["Partial Mode"], node["Parent Relationship"])
    elif node_type.lower() == "gather":    
        node_class = gather_node(node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Workers Planned"], node["Single Copy"], node["Parent Relationship"])
    elif node_type.lower() == "seq scan":
        if "Filter" in node:
            node_class = seq_scan_node(node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Relation Name"], node["Schema"], node["Alias"], node["Parent Relationship"], node["Filter"])
        else:
            # From the planner, some Seq Scan nodes just return the data, with no filtering
            node_class = seq_scan_node(node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Relation Name"], node["Schema"], node["Alias"], node["Parent Relationship"], None)
    elif node_type.lower() == "sort":
        node_class = sort_node(node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Sort Key"], node["Parent Relationship"])
    elif node_type.lower() == "nested loop":
        node_class = nested_loop_node(node_type, node['Parallel Aware'], node['Async Capable'], node['Output'], node['Inner Unique'], node['Join Type'], node['Parent Relationship']) 
    elif node_type.lower() == "hash join":
        node_class = hash_join_node(node_type, node['Parallel Aware'], node['Async Capable'], node['Output'], node['Inner Unique'], node['Join Type'], node['Hash Cond'], node['Parent Relationship'])
    elif node_type.lower() == "hash":
        node_class = hash_node(node_type, node['Parallel Aware'], node['Async Capable'], node['Output'], node['Parent Relationship'])
    elif node_type.lower() == "index scan":
        node_class = index_scan_node(node_type, node['Parallel Aware'], node['Async Capable'], node['Scan Direction'], node['Index Name'], node['Relation Name'], node['Schema'], node['Alias'], node['Index Cond'], node['Filter'], node['Output'], node['Parent Relationship'])
    else:
        raise Exception("Node Type", node_type, "is not recognised, many Node Types have not been implemented.")
        
    # Check if this node has a child
    if "Plans" in node:
        node_class_plans = []
        for individual_plan in node['Plans']:
            node_class_plans.append(make_tree(individual_plan, ""))
        node_class.set_plans(node_class_plans)
    
    return node_class