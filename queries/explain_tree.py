from plan_to_explain_tree import * 
import regex
import re

def solve_nested_loop_node(tree):
    # Preorder traversal
    
    if tree.node_type == "Nested Loop":
        # Add in merge_cond attribute
        # Check the plans of Node, for an index scan, add on that only!
        merging_condition = None
        for individual_plan in tree.plans:
            if individual_plan.node_type == "Index Scan":
                # Perform work for the merging condition
                if merging_condition != None:
                    # We have already found the merging condition
                    raise Exception("We have found two merging conditions")
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
                
                # Perform work for the filter condition
                # We are going to investigate individual_plan.filter
                # We want to use a regular expression to split up a string but respect nested brackets
                if hasattr(individual_plan, "filter"):
                    keep_filter, up_filter = process_matches(str(individual_plan.filter)[1:-1], individual_plan.relation_name)
                    
                    # Handle keep_filter ending in "AND" or "OR"
                    if keep_filter[-1] == "AND" or keep_filter[-1] == "OR":
                        # Pop last element out
                        keep_filter.pop()
                    
                    # Set individual_node filters back to keep_filter
                    individual_plan.filter = "(" + " ".join(keep_filter) + ")"
                    
                    # Set tree filters
                    if up_filter != []:
                        tree.add_filter(" ".join(up_filter))
                        
        if merging_condition != None:
            tree.add_merge_cond(merging_condition)
        else:
            raise Exception("Didn't find a merging condition to add to our Nested Loop node.")
    
    if tree.plans != None:
        for individual_plan in tree.plans:
            solve_nested_loop_node(individual_plan)

def process_matches(string, relation):
    matches = return_matches(string)
    
    keep_filter = []
    up_filter = []
    
    for current_match in matches:
        up_match = False
        regex = r"[^\s]+\."
        matches = re.finditer(regex, current_match, re.MULTILINE)
        out_matches = []
        for matchNum, in_match in enumerate(matches, start=1):
            match = str(str(str(in_match.group()).replace("(", "")).replace(")", "")).replace(".", "")
            out_matches.append(match)
        
        # Check that all elements in out_matches are relation
        for element in out_matches:
            if element != relation:
                up_match = True
                break
            
        # Do upmatch
        if up_match:
            up_filter.append(current_match)
        else:
            keep_filter.append(current_match)
            
    return keep_filter, up_filter

def return_matches(string):
    return [match.group() for match in regex.finditer(r"(?:(\((?>[^()]+|(?1))*\))|\S)+", string)]
 
def solve_prune_node(prune_type, tree):
    # Modified to Support Any node we input
    
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
            if current_node.node_type == prune_type:
                # We have found a Prune node
                # Get the Prune's children (child)
                if len(current_node.plans) == 1:
                    prune_children = current_node.plans[0]
                else:
                    raise ValueError("We have assumed a " + prune_type + " node only has one child, not the case in reality!")
                # Set current_node to be the children
                tree.plans[i] = prune_children
    
    # Run this function on below nodes
    if tree.plans != None:
        for individual_plan in tree.plans:
            solve_prune_node(prune_type, individual_plan)               
            
alias_locations = {
    "Index Scan": ["output", "index_cond"],
    "Seq Scan": ["output"],
    "Group Aggregate": ["group_key", "output", "filter"],
    "Hash Join": ["hash_cond", "output"],
    "Sort": ["sort_key", "output"],
    "Merge Join": ["output", "merge_cond"],
    "Nested Loop": ["merge_cond", "output"],
    "Incremental Sort": ["output", "presorted_key", "sort_key"],
    "Limit": ["output"]
}
            
def solve_aliases(tree, replaces = None):
    # Maybe instead of a tree traversal we could just
    # Iterate post-order through the tree collecting a dict of replaces
    # Then Iterate again to replace the dodgy items
    
    # Root creates dictionary
    # If replaces = None, create it
    if replaces == None:
        replaces = {}
    
    # We want to use a post-order traversal
    # First we traverse the left subtree, then the right subtree and finally the root node.
    if tree.plans != None:
        for individual_plan in tree.plans:
            solve_aliases(individual_plan, replaces)
         
    # Act on current node  
    
    # Add a new replaces if exists
    if hasattr(tree, "alias") and hasattr(tree, "relation_name"):
        if tree.alias != tree.relation_name:
            # Add to dict, if doesn't exist already
            if replaces.get(tree.alias, None) == None:
                # Add to dict
                replaces[tree.alias] = tree.relation_name
            else:
                # This is already in the dict, we don't need to add it again
                pass
            
    # Do alterations to all the alteration places, based on the content in replaces
    for key in replaces.keys():
        locs = alias_locations[tree.node_type]
        for attribute in locs:
            if hasattr(tree, attribute):
                # Iterate through output
                modified_attribute = getattr(tree, attribute)
                # Modified attribute may not always be a list
                if isinstance(modified_attribute, list):
                    for i in range(len(modified_attribute)):
                        modified_attribute[i] = modified_attribute[i].replace(key, replaces[key])
                elif isinstance(modified_attribute, str):
                    modified_attribute = modified_attribute.replace(key, replaces[key])
                else:
                    raise ValueError("Attribute is not a type we expected")
                # Set it back
                setattr(tree, attribute, modified_attribute)
            else:
                # The class doesn't have this attribute, ignore it
                pass 
    
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
            # Switch if no Parent Relationship
            if "Parent Relationship" in node:
                node_class = group_aggregate_node("Group " + node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Strategy"], node["Partial Mode"], node["Parent Relationship"], node["Group Key"])
            else:
                node_class = group_aggregate_node("Group " + node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Strategy"], node["Partial Mode"], None, node["Group Key"])
            
            # Handle adding Filter
            if "Filter" in node:
                node_class.add_filter(node["Filter"])
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
        node_class = index_scan_node(node_type, node['Parallel Aware'], node['Async Capable'], node['Scan Direction'], node['Index Name'], node['Relation Name'], node['Schema'], node['Alias'], node['Output'], node['Parent Relationship']) 
        if "Index Cond" in node:
            node_class.add_index_cond(node['Index Cond'])
        if "Filter" in node:
            node_class.add_filter(node['Filter'])
    elif node_type.lower() == "incremental sort":
        # node_type, parallel_aware, async_capable, output, parent_relationship, sort_key, presorted_key
        node_class = incremental_sort_node(node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Parent Relationship"], node["Sort Key"], node["Presorted Key"])
    elif node_type.lower() == "merge join":
        node_class = merge_join_node(node_type, node['Parallel Aware'], node['Async Capable'], node['Output'], node['Inner Unique'], node['Join Type'], node['Merge Cond'], node['Parent Relationship'])
    elif node_type.lower() == "materialize":
        node_class = materialize_node(node_type, node['Parallel Aware'], node['Async Capable'], node['Output'], node['Parent Relationship'])
    else:
        raise Exception("Node Type", node_type, "is not recognised, many Node Types have not been implemented.")
        
    # Check if this node has a child
    if "Plans" in node:
        node_class_plans = []
        for individual_plan in node['Plans']:
            node_class_plans.append(make_tree(individual_plan, ""))
        node_class.set_plans(node_class_plans)
    
    return node_class