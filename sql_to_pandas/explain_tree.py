from plan_to_explain_tree import * 
import regex
import re
from queue import Queue

agg_funcs = {"sum", "avg", "count", "max", "min", "distinct", "mean", "count_star"}

def get_class_id(node):
    return str(id(node))

def flip_cond_around(in_cond):
    # We should flip this around at this point
    # So originally index_cond will be:
    # (a = b)
    # And we should change it to:
    # (b = a)
    
    # Sometimes we have multiple conditions, that each of them we need to flip
    if " AND " in in_cond:
        # Strip brackets
        if in_cond[0] == "(" and in_cond[-1] == ")":
            # Strip brackets
            in_cond = in_cond[1:-1]
        
        split_cond = in_cond.split(" AND ")
        for i in range(len(split_cond)):
            split_cond[i] = split_cond[i].strip()
    else:
        # Use the same variable for processing, just set to a single element
        split_cond = [in_cond]        
    
    # Iterate through split_cond
    for i in range(len(split_cond)):
        index = str(split_cond[i])
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
                
        # Set it out
        split_cond[i] = new_index
        
    if len(split_cond) == 1:
        # Only one value
        return str(split_cond[0].strip())
    else:
        # Join back together
        return str("(" + str(" AND ".join(split_cond)).strip() + ")")

def process_into_relations_list(output):
    relations = set()
    
    for i in range(len(output)):
        relations.add(str(output[i].split(".")[0]).strip())
        
    return relations

def determine_flip(tree):
    # Process flip into left_relation and right_relation
    index = str(tree.filter)
    if index[0] == "(" and index[-1] == ")":
        # Strip brackets
        index = index[1:-1]
    index_split = str(index).split(" = ")
    for i in range(len(index_split)):
        index_split[i] = str(index_split[i].split(".")[0]).strip()
    if len(index_split) != 2:
        raise ValueError("Unexpected result from processing")
    left_relation = index_split[0]
    right_relation = index_split[1]
    
    if len(tree.plans) != 2:
        raise ValueError("Tree has unexpected number of nodes below it")
    
    # Process nodes below into left_node_relations and right
    left_node_relations = process_into_relations_list(tree.plans[0].output)
    right_node_relations = process_into_relations_list(tree.plans[1].output)
    
    # If left_relation not in left set and in right
    if (left_relation not in left_node_relations) and (left_relation in right_node_relations) and (right_relation not in right_node_relations) and (right_relation in left_node_relations):
        # Flip!
        return True
    elif (left_relation in left_node_relations) and (left_relation not in right_node_relations) and (right_relation in right_node_relations) and (right_relation not in left_node_relations):
        # Don't flip
        return False
    else:
        raise ValueError("Unable to process and analyse the relations correctly")
    

def solve_nested_loop_node(tree):
    # Preorder traversal
    
    if tree.node_type == "Nested Loop":
        # Add in merge_cond attribute
        # Check the plans of Node, for an index scan, add on that only!
        merging_condition = None
        
        # If the tree node has an attribute of filter.
        # We don't need to carry out the searching below procedure
        # And can instead use functions to swap around the filter and add as a merging cond
        if hasattr(tree, "filter"):
            # Sometimes we need to flip the condition around, and sometimes we don't
            if determine_flip(tree) == True:
                # We should flip
                tree.filter = flip_cond_around(tree.filter)
            
            # We set the filter, swap around if we need to, but generally leave it in situ
            
        for individual_plan in tree.plans:
            if individual_plan.node_type == "Index Scan":
                # Perform work for the merging condition
                if merging_condition != None:
                    # We have already found the merging condition
                    
                    # We need to decide which based on which condition has more in common with the
                    # present relations
                    
                    # Gather relations used in tree.plans:
                    present_relations = []
                    for i in range(len(tree.plans)):
                        present_relations.append(tree.plans[i].relation_name)
                    
                    # Print out both merge_conditions
                    merge_conditions = []
                    for i in range(len(tree.plans)):
                        index_cond = tree.plans[i].index_cond
                        merge_conditions.append(index_cond)
                    
                    # Count up which condition has the most relations, we should use this one and leave the other to
                    # be evaluated 
                    counted_conds = []
                    for i in range(len(merge_conditions)):
                        index_cond = merge_conditions[i]
                        
                        if index_cond[0] == "(" and index_cond[-1] == ")":
                            # Strip brackets
                            index_cond = index_cond[1:-1]
                        
                        split_cnd = index_cond.split(" = ")
                        for j in range(len(split_cnd)):
                            split_cnd[j] = split_cnd[j].split(".")[0]
                            
                        cnd_count = 0
                        for j in split_cnd:
                            if j in present_relations:
                                cnd_count += 1
                        
                        counted_conds.append(cnd_count)
                    
                    # Find maximum value index, that index in merge conditions, is the one we should
                    # propagate upwards
                    decided_merge_cond = merge_conditions[counted_conds.index(max(counted_conds))]
                    merging_condition = flip_cond_around(decided_merge_cond)
                    
                    #raise Exception("We have found two merging conditions")
                    continue
                else:
                    if hasattr(individual_plan, "index_cond"):
                        merging_condition = flip_cond_around(individual_plan.index_cond)
                
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
            # iterate across individual plans, setting the merge_condition
            # that we actually use to none and leaving the others
            original_cond = flip_cond_around(merging_condition)
            for individual_plan in tree.plans:
                if hasattr(individual_plan, "index_cond"):
                    if individual_plan.index_cond == original_cond:
                        # Set to none
                        individual_plan.index_cond = None
        else:
            # We haven't got a merging condition
            # We set the filter, to be this, and wipe the filter
            if hasattr(tree, "filter"):
                tree.add_merge_cond(tree.filter)
                # Set filter to none
                tree.filter = None
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
 
def solve_null_output(tree):
    # Preorder Traversal
 
    # Check current node
    if tree.plans != None:
        for i in range(len(tree.plans)):
            child_node = tree.plans[i]
            # Iterate through output
            removes = []
            
            for i in range(len(child_node.output)):
                if "NULL:" in child_node.output[i]:
                    removes.append(i)
            
            removes.reverse()
            
            child_output_list = list(child_node.output)
            
            # Delete tags that have NULL in them
            for num in removes:
                del child_output_list[num]
            
            child_node.output = child_output_list
    
    # Run this function on below nodes
    if tree.plans != None:
        for individual_plan in tree.plans:
            solve_null_output(individual_plan)   
        
def solve_groupby_fusion(tree):
    # Check current node
    # Only run checking if we're a group aggregate node
    if tree.node_type == "Group Aggregate":
        if tree.plans != None:
            for i in range(len(tree.plans)):
                current_node = tree.plans[i]
                if current_node.node_type == "Sort":
                    # We have found a Sort node
                    # Get the Prune's children (child)
                    if len(current_node.plans) == 1:
                        prune_children = current_node.plans[0]
                    else:
                        raise ValueError("We have assumed a sort node only has one child, not the case in reality!")
                    
                    # Set the parent of prune_children to be tree.plans[i]
                    prune_children.parent = tree
                    
                    # Set current_node to be the children
                    tree.plans[i] = prune_children
    
    # Run this function on below nodes
    if tree.plans != None:
        for individual_plan in tree.plans:
            solve_groupby_fusion(individual_plan)  

def solve_merge_join_fusion(tree):
    # Check current node
    # Only run checking if we're a group aggregate node
    if tree.node_type == "Merge Join" and tree.join_type.lower() != "semi":
        if tree.plans != None:
            for i in range(len(tree.plans)):
                current_node = tree.plans[i]
                if current_node.node_type == "Sort":
                    # We have found a Sort node
                    # Get the Prune's children (child)
                    if len(current_node.plans) == 1:
                        prune_children = current_node.plans[0]
                    else:
                        raise ValueError("We have assumed a sort node only has one child, not the case in reality!")
                    
                    # Set the parent of prune_children to be tree.plans[i]
                    prune_children.parent = tree
                    
                    # Set current_node to be the children
                    tree.plans[i] = prune_children
    
    # Run this function on below nodes
    if tree.plans != None:
        for individual_plan in tree.plans:
            solve_merge_join_fusion(individual_plan)  
 
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
                
                # Set the parent of prune_children to be tree.plans[i]
                prune_children.parent = tree.plans[i]
                
                # Set current_node to be the children
                tree.plans[i] = prune_children
    
    # Run this function on below nodes
    if tree.plans != None:
        for individual_plan in tree.plans:
            solve_prune_node(prune_type, individual_plan)               
            
alias_locations = {
    "Index Only Scan": ["output", "filter"],
    "Index Scan": ["output", "index_cond", "filter"],
    "Seq Scan": ["output", "filters"],
    "Group Aggregate": ["group_key", "output", "filter"],
    "Hash Join": ["hash_cond", "output", "filter"],
    "Sort": ["sort_key", "output"],
    "Merge Join": ["output", "merge_cond", "filter"],
    "Nested Loop": ["merge_cond", "output", "filter"],
    "Incremental Sort": ["output", "presorted_key", "sort_key"],
    "Limit": ["output"],
    "Aggregate": ["group_key", "output"],
    "Subquery Scan": ["output"]
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
            

def solve_initplan_nodes(tree):
    capturedTrees = []
    dropClasses = set()
    
    treeQueue = Queue()
    treeQueue.put(tree)
    while (not treeQueue.empty()):
        treeNode = treeQueue.get()
        if treeNode == None:
            continue
        else:
            # Act on the current node
            
            # Iterate on lower nodes
            if treeNode.plans != None:
                for nodeBelow in treeNode.plans:
                    
                    # If we have a parent
                    if hasattr(nodeBelow, "parent_relationship"):
                        # Check if is equal to InitPlan
                        if nodeBelow.parent_relationship == "InitPlan":
                            # We have an InitPlan
                            # Extract the InitPlan set value
                            if hasattr(nodeBelow, "subplan_name"):
                                set_value = str(str(nodeBelow.subplan_name).split("returns ")[1].split(")")[0]).strip()
                                # Replace this to dollar, the standard form of set values
                                set_value = str(set_value).replace("$", "dollar_")
                            else:
                                raise ValueError("We have an InitPlan but with no information about what it's set as!")
                            
                            # Add this to the captured Trees List
                            capturedTrees.append((nodeBelow, set_value))
                            
                            # Add the id of nodeBelow to dropClasses
                            dropClasses.add(get_class_id(nodeBelow))
                            
                            # Run next iteration
                            continue
                        elif nodeBelow.parent_relationship == "SubPlan":
                            # TODO: Do continue for now, we haven't decided how to use this functionality
                            
                            correlated_subquery_return, gatheredAliases = determine_if_correlated_subquery(nodeBelow)
                            if correlated_subquery_return == True:
                                print("We have a correlated Subquery")
                                # We should edit the tree, so that we unnest the correlated subquery.
                                
                                # Class for the correlated key
                                class subquery_helper():
                                    def __init__(self, in_aliases):
                                        self.keys = None
                                        self.aliases = in_aliases
                                    
                                    def add_keys(self, in_keys):
                                        self.keys = in_keys
                                        
                                    def add_aliases(self, new_aliases):
                                        self.aliases = self.aliases | new_aliases
                                        
                                helper = subquery_helper(gatheredAliases)
                                
                                solve_correlated_subquery(tree, helper, None)
                                print("We successfully unnest it!")

                            else:
                                print("We don't have a correlated Subquery")
                            
                            continue
                            # We have a SubPlan
                            # Extract the name
                            if hasattr(nodeBelow, "subplan_name"):
                                name = nodeBelow.subplan_name
                            else:
                                raise ValueError("We have a SubPlan but with no name")
                            
                            # Add this to the captured Trees List
                            capturedTrees.append((nodeBelow, name))
                            
                            # Add the id of nodeBelow to dropClasses
                            # dropClasses.add(get_class_id(nodeBelow))
                            
                            # Run next iteration
                            continue
                            
                        
                    # Add them to the queue
                    treeQueue.put(nodeBelow)
                    
    # Remove classes by their id
    delete_tree_branches_by_id(tree, dropClasses)
    
    # Add the main tree, now potentially altered, to the end of captured trees
    capturedTrees.append(tree)
    
    return capturedTrees

def solve_correlated_subquery(tree, subquery_helper, subplan_zone):
    """Solve the correlated subquery

    Args:
        tree (object): the entire explain tree for an input SQL query
    """
    
    # Traverse the tree recursively, in a pre_order traversal
    
    # Detect if we have entered a subplan
    if hasattr(tree, "parent_relationship"):
        if tree.parent_relationship == "SubPlan":
            subplan_zone = True
            
    # Go left, right
    # Run this function on below nodes
    if tree.plans != None:
        for individual_plan in tree.plans:
            solve_correlated_subquery(individual_plan, subquery_helper, subplan_zone)
    
    # Act on the children of the current node
    if tree.plans != None:
        new_plans = []
        for current_child in tree.plans:            
            child_subplan = False
            # Detect if we a child subplan
            if hasattr(current_child, "parent_relationship"):
                if current_child.parent_relationship == "SubPlan":
                    child_subplan = True
            
            # Task 1: Convert node where actual correlation happens
            # Iterate through search_locations of the current node
            # Only run if we're in a subplan
            if (child_subplan == True) or (subplan_zone == True):
                for search_location in alias_locations[current_child.node_type]:
                    if hasattr(current_child, search_location):
                        # If the set from relation_finder is not a subset of gathered_aliases
                        # Then we have a the node where the actual correlation happens
                        
                        # As we have a relation that is been detected as part of a node,
                        # But is not one that we have gathered already, not an alias we've brought in already
                        detected_relations = relation_finder(getattr(current_child, search_location))
                        if not detected_relations.issubset(subquery_helper.aliases):
                            # Node of actual correlation
                            
                            # Convert node where the actual correlation occurs into a hash join between the
                            # existing and new node
                            
                            # Alias difference
                            alias_difference = detected_relations - subquery_helper.aliases
                            
                            # Edit in place
                            current_child, output_correlated_key = actual_correlation_to_join_node(current_child, search_location, alias_difference)
                            
                            # Add the overlapping aliases to gatheredAliases
                            subquery_helper.add_aliases(alias_difference)
                            
                            if subquery_helper.keys != None:
                                raise ValueError("We have a correlated_key that we are carrying around and have just detected a new one")
                            else:
                                subquery_helper.add_keys(output_correlated_key)
                                
                            # Task 4: For the nodes between the actual correlation and to where the subplan is stated, we need
                            # to add the key into their output
                            t4_tree = current_child
                            while not hasattr(t4_tree, "subplan_name"):
                                for c_key in subquery_helper.keys:
                                    if c_key not in t4_tree.output:
                                        t4_tree.output.append(c_key)
                                
                                t4_tree = t4_tree.parent
                            
            # Task 2: Convert the node where the subplan is stated
            # Into a group aggregate
            # Only run if we're in a subplan
            if (child_subplan == True) or (subplan_zone == True):
                # If it has the attribute of "subplan_name"
                if hasattr(current_child, "subplan_name"):
                    # Then we have the actual node where the subplan is stated
                    
                    # Edit in place
                    current_child = actual_subplan_to_group_aggr_node(current_child, subquery_helper.keys)
                    
            # Task 3: Convert the existing Join (the one that combines in the subplan)
            # Into two joins
            
            # Check each of the locations
            for search_location in alias_locations[current_child.node_type]:
                if hasattr(current_child, search_location):
                    if "SubPlan" in getattr(current_child, search_location):
                        # The current child has a subplan in one of it's filters, we need to edit this
                        # using subplan join into many joins
                        
                        # Edit in place
                        supported_joins = ["Hash Join", "Merge Join"]
                        supported_scans = ["Seq Scan", "Index Scan"]
                        if getattr(current_child, "node_type") in supported_joins:
                            current_child = subplan_join_into_many_joins(current_child, subquery_helper.keys)
                        elif getattr(current_child, "node_type") in supported_scans:
                            current_child = subplan_scan_into_scan_and_join(current_child, subquery_helper.keys)
                        else:
                            # TODO: Add support Index Scan type of Task 3
                            raise Exception(" We have to do Task 3 on a node that isn't supported. Node Type: " + str(getattr(current_child, "node_type")))
            
            # Task 4: For the nodes between the actual correlation and where the subplan started, we need to add the key into their output
            # Do this in the Task 1 section
            # Done above
            
            # Store the current children
            new_plans.append(current_child)
            
        # Set the plans back into the tree
        tree.plans = new_plans
        
def subplan_scan_into_scan_and_join(node, correlated_key):
    """_summary_

    Args:
        node (object): child where the subplan scan happens
        correlated_key (string): key that the subplan needs to merge on

    Returns:
        object: new remade node
    """
    
    # Check number of children, we think it should have 1
    if len(node.plans) != 1:
        raise Exception("Not enough children, we think the node should have 1.")
    # Check the node has a filter, otherwise what related to the subplan
    if not hasattr(node, "filter"):
        raise Exception("Node doesn't have ability to filter, node: " + str(node.node_type))
    
    # New Node Layout:
    #        (top_join)
    #        /       \
    # (lower_scan)   [SubPlan]
    
    # Split the node's filter into before and after filter
    # Anything with a subplan should go in after
    before_filter = []
    after_filter = []
    
    if hasattr(node, "filter"):
        if not "SubPlan" in node.filter:
            raise Exception("No mention of SubPlan in our filter attribute")
        else:
            # Subplan in filter!
            # Separate Subplan out
            split_filter_cond = filter(None, re.split(" AND | OR ", node.filter))
            for individual_filter_cond in split_filter_cond:
                if "SubPlan" in individual_filter_cond:
                    after_filter.append(individual_filter_cond)
                else:
                    before_filter.append(individual_filter_cond)
                    
    # Fix before/after filter condition        
    if len(before_filter) == 1:
        before_filter = before_filter[0]
    elif before_filter == []:
        pass
    else:
        raise ValueError("Unexpected size of before_filter")
    
    if len(after_filter) == 1:
        after_filter = after_filter[0]
    elif after_filter == []:
        raise ValueError("We need to have an after_filter, it can't just be None")
    else:
        raise ValueError("Unexpected size of after_filter")
    
    
    # Determine after_filter replacement
    # Replace Subplan name in either cond or filter
    # Set SubPlan child
    if not hasattr(node.plans[0], "subplan_name"):
        raise Exception("Is node.plans[0] even a subplan?")
    
    subplan_child = node.plans[0]
    if len(subplan_child.output) != (1 + len(correlated_key)):
        raise Exception("Unexpected number of outputs from the subplan")
    replace_key = list(subplan_child.output)
    for c_key in correlated_key:
        replace_key.remove(c_key)   
    if len(replace_key) == 1:
        replace_key = replace_key[0]
    else:
        raise Exception("Unexpected amount of replace_keys")
    subplan_name = str(subplan_child.subplan_name)
    
    # Replace in filter
    if subplan_name in after_filter:
        after_filter = after_filter.replace(subplan_name, replace_key)
    else:
        raise Exception("Subplan Name is not in the condition or the filter")
    
    # Construct the lower_scan
    lower_scan_output = list(node.output)
    for c_key in correlated_key:
        if c_key not in lower_scan_output:
            lower_scan_output.append(c_key)
    
    lower_scan = seq_scan_node("Seq Scan", lower_scan_output, node.relation_name, node.alias)
    
    # Set lower_scan filter
    if before_filter != []:
        lower_scan.add_filters(before_filter)
    
    # Construct the top_join
    top_join_output = list(node.output)
    top_join_inner_unique = False
    top_join_join_type = "Inner"
    
    # Make the merge condition
    top_join_merge_cond = []
    for c_key in correlated_key:
        top_join_merge_cond.append("(" + c_key + " = " + c_key + ")")
    top_join_merge_cond = "(" + " AND ".join(top_join_merge_cond) + ")"
    
    top_join = hash_join_node("Hash Join", top_join_output, top_join_inner_unique, top_join_join_type, top_join_merge_cond)

    # Set top_join after_filter
    top_join.add_filter(after_filter)

    # Set top_join plans
    # Set lower_scan and Subplan parent
    top_join_children = [lower_scan, subplan_child]
    # Set parent
    for child in top_join_children:
        child.set_parent(top_join)
    top_join.set_plans(top_join_children)
    
    # Set top_join parent
    top_join.set_parent(node.parent)

    return top_join
    
def subplan_join_into_many_joins(node, correlated_key):
    """
    Turn the existing hash join / merge join, that combines the subplan into the main tree, into two.
    The first merge is the existing one, with the subplan references removed and the second one joins
        this and the subplan to continue normal tree shape

    Args:
        node (object): the merge to be turned into two
        correlated_key (string): key that the subplan needs to merge on
    """
    
    # Check number of children, we think it should have 3
    if len(node.plans) != 3:
        raise Exception("Not enough children, we think the node should have 3.")
    
    # Join Layout:
    #    (2)
    #   /  \
    # (1) [SubPlan]
    
    # Join parameters
    join_1_filter = []
    join_2_filter = []
    
    join_1_cond = []
    join_2_cond = []
    
    # Goal here is to extract the subplan element
    
    # Process first the filter
    if hasattr(node, "filter"):
        if not "SubPlan" in node.filter:
            # Not Subplan in filter, add to Join 1
            join_1_filter.append(node.filter)
        else:
            # Subplan in filter!
            # Separate Subplan out
            # filter(None, re.split("[, \-!?:]+", "Hey, you - what are you doing here!?"))
            split_filter_cond = filter(None, re.split(" AND | OR ", node.filter))
            for individual_filter_cond in split_filter_cond:
                if "SubPlan" in individual_filter_cond:
                    join_2_filter.append(individual_filter_cond)
                else:
                    join_1_filter.append(individual_filter_cond)
                    
    # Fix join_1_filter condition        
    if len(join_1_filter) == 1:
        join_1_filter = join_1_filter[0]
    elif join_1_filter == []:
        pass
    else:
        raise ValueError("Unexpected size of join_1_filter")
    
    if len(join_2_filter) == 1:
        join_2_filter = join_2_filter[0]
    elif join_2_filter == []:
        pass
    else:
        raise ValueError("Unexpected size of join_2_filter")
    
    # Process the condition next
    
    # First set cond_name
    cond_name = None
    if node.node_type == "Merge Join":
        cond_name = "merge_cond"
    elif node.node_type == "Hash Join":
        cond_name = "hash_cond"
    else:
        raise Exception("Unexpected type of joining node")
    
    # Set connecting here
    connecting = None
    
    if hasattr(node, cond_name):
        node_condition = getattr(node, cond_name)
        if not "SubPlan" in node_condition:
            # No Subplan in cond, so add to join 1
            join_1_cond.append(node_condition)
        else:
            # We have a SubPlan in this cond
            # Separate Subplan out
            
            # Determine what's the connecting bloke
            skip = False
            if " AND " in node_condition:
                connecting = " AND "
            elif " OR " in node_condition:
                connecting = " OR "
            else:
                # We have no connectors, just add to join_2
                join_2_cond.append(node_condition)
                skip = True
            
            if skip == False:
                # strip outer brackets if exist
                if (node_condition[0] == "(") and (node_condition[-1] == ")"):
                    node_condition = node_condition[1:-1]
                
                # filter(None, re.split("[, \-!?:]+", "Hey, you - what are you doing here!?"))
                split_join_cond = filter(None, re.split(connecting, node_condition))
                for individual_join_cond in split_join_cond:
                    if "SubPlan" in individual_join_cond:
                        
                        # We want the SubPlan key on the righthand side of the equals
                        # Let's determine if it is, and it it's not, we split
                        
                        # Remove brackets first
                        remove_brackets = False
                        bracket_process = individual_join_cond
                        if (bracket_process[0] == "(") and (bracket_process[-1] == ")"):
                            bracket_process = bracket_process[1:-1]
                            remove_brackets = True
                            
                        split_bp = []
                        split_cond = None
                        if " = " in bracket_process:
                            split_cond = " = "
                            split_bp = bracket_process.split(split_cond)
                        else:
                            raise Exception("Unexpected format of join condition, doesn't have an equals in it")
                        
                        if len(split_bp) != 2:
                            raise Exception("Unexpected size of split of the join condition")
                        
                        # Determine which side it's in
                        # Right side, append and move on
                        # Left side, rearrangement needed
                        
                        if "SubPlan" in split_bp[1]:
                            join_2_cond.append(individual_join_cond)
                        else:
                            # Must be in left side, we need to rearrange
                            if remove_brackets == True:
                                new_join_cond = "(" + split_bp[1] + split_cond + split_bp[0] + ")"
                            else:
                                new_join_cond = split_bp[1] + split_cond + split_bp[0]
                                
                            # Add to array
                            join_2_cond.append(new_join_cond)
                    else:
                        join_1_cond.append(individual_join_cond)
                        
                        
    # Fix join_1/2_cond
    if len(join_1_cond) == 1:
        join_1_cond = join_1_cond[0]
    elif join_1_cond == []:
        pass
    else:
        if connecting == None:
            raise ValueError("Unexpected size of join_1_cond")
        else:
            # Join back together with connecting to space it
            join_1_cond = connecting.join(join_1_cond)
    
    if len(join_2_cond) == 1:
        join_2_cond = join_2_cond[0]
    elif join_2_cond == []:
        pass
    else:
        if connecting == None:
            raise ValueError("Unexpected size of join_2_cond")
        else:
            # Join back together with connecting to space it
            join_2_cond = connecting.join(join_2_cond)
    
    
    # Determine which child is the subplan
    subplan_child = None
    non_subplan_children = []
    for node_child in node.plans:
        if hasattr(node_child, "subplan_name"):
            subplan_child = node_child
        else:
            non_subplan_children.append(node_child)
            
    # Construct JOIN 1, the lower level join
    join_1_output = list(node.output)
    for c_key in correlated_key:
        if c_key not in join_1_output:
            join_1_output.append(c_key)
    
    # Get all the relations out of join_2_filter, check they are in join_1_output,
    # If they are not then add then in
    # Do this for join_2_filter and join_2_cond
    searching_arrays = [join_2_filter, join_2_cond]
    for search_target in searching_arrays:
        if search_target != []:
            special_temp = search_target
            if (special_temp[0] == "(") and (special_temp[-1] == ")"):
                special_temp = special_temp[1:-1]
            j2_relations = [match.group() for match in regex.finditer(r"(?:(\((?>[^()]+|(?1))*\))|\S)+", special_temp)]
            # Filter out operators and SubPlan
            clean_j2 = []
            for item in j2_relations:
                if any(c.isalpha() for c in item):
                    if "SubPlan" not in item:
                        clean_j2.append(item)
            # Add to join_1_output if not already in there
            for j2_item in clean_j2:
                if j2_item not in join_1_output:
                    join_1_output.append(j2_item) 
    
    
    # Make both be hash joins
    join_1 = hash_join_node("Hash Join", join_1_output, node.inner_unique, node.join_type, join_1_cond)
    # Set filter
    if join_1_filter != []:
        join_1.add_filter(join_1_filter)
    # Set children
    join_1.set_plans(non_subplan_children)
    
    # Construct JOIN 1, the top level join    
    # Finalise join_2_cond
    # Make correlated_cond
    correlated_cond = []
    for c_key in correlated_key:
        correlated_cond.append("(" + c_key + " = " + c_key + ")")
    correlated_cond = " AND ".join(correlated_cond)
    
    if join_2_cond == []:
        join_2_cond = "(" + correlated_cond + ")"
    else:
        join_2_cond = "(" + join_2_cond + " AND " + correlated_cond + ")"
        
    # Replace Subplan name in either cond or filter
    if len(subplan_child.output) != (1 + len(correlated_key)):
        raise Exception("Unexpected number of outputs from the subplan")
    replace_key = list(subplan_child.output)
    for c_key in correlated_key:
        replace_key.remove(c_key)   
    if len(replace_key) == 1:
        replace_key = replace_key[0]
    else:
        raise Exception("Unexpected amount of replace_keys")
    subplan_name = str(subplan_child.subplan_name)
    
    # Try in cond
    if subplan_name in join_2_cond:
        join_2_cond = join_2_cond.replace(subplan_name, replace_key)
    elif join_2_filter != []:
        if subplan_name in join_2_filter:
            join_2_filter = join_2_filter.replace(subplan_name, replace_key)
    else:
        raise Exception("Subplan Name is not in the condition or the filter")
    
    join_2_output = list(node.output)
    join_2 = hash_join_node("Hash Join", join_2_output, node.inner_unique, node.join_type, join_2_cond)
    # Set filter
    if join_2_filter != []:
        join_2.add_filter(join_2_filter)
    # Set children
    join_2.set_plans([join_1, subplan_child])
    # Set parent
    join_2.set_parent(node.parent)
    
    # Set Join 1 parent
    join_1.set_parent(join_2)
    
    # Fix parents, for children of the joins
    for non_subplan_child in non_subplan_children:
        non_subplan_child.set_parent(join_1)
    subplan_child.set_parent(join_2)
    
    return join_2

def actual_subplan_to_group_aggr_node(node, correlated_key):
    """
    Convert the node where the subplan is stated from (hopefully) an aggregate into a group aggregate
    """    
    
    # Hope this node is an aggregate
    if node.node_type != "Aggregate":
        raise Exception("Node is not an Aggregate, help!")
    
    # Add correlated_key to the node output
    gp_aggr_output = list(node.output)
    for c_key in correlated_key:
        gp_aggr_output.append(c_key)
    
    # Add correlated_key to the node as the group key   
    # Construct new_node
    new_node = group_aggregate_node("Group Aggregate", gp_aggr_output, correlated_key)
    
    # Handle adding Filter
    if hasattr(node, "filter"):
        new_node.add_filter(node.filter)
    # Add SubPlan
    if hasattr(node, "subplan_name"):
        new_node.add_subplan(node.subplan_name)
    
    # Add in parent
    new_node.set_parent(node.parent)
    
    # Set below
    new_node.set_plans(node.plans)
    
    # Fix the children of this node's parent
    if new_node.plans != None:
        for current_child_child in new_node.plans:
            current_child_child.parent = new_node

    return new_node


def actual_correlation_to_join_node(node, correlation_location, correlated_relation):
    """
    Convert node where the actual correlation occurs into a hash join between the
    existing and new node

    Args:
        node (object): node of the correlation, we convert this into a join
    """
    
    # Check for children
    if node.plans != None:
        raise Exception("Correlated node has children, we haven't written to deal with this.")
    
    # Reset correlated_relation
    if isinstance(correlated_relation, set):
        correlated_relation = list(correlated_relation)[0]
    
    # Separate the condition from correlation_location into before and after
    before_condition = []
    after_condition = []
    # Detect AND in the correlation location
    connector = None
    if (" AND " in getattr(node, correlation_location)):
        connector = " AND "
    elif (" OR " in getattr(node, correlation_location)):
        connector = " OR "
        
    if (connector != None):
        if (connector in getattr(node, correlation_location)):
            # filter(None, re.split("[, \-!?:]+", "Hey, you - what are you doing here!?"))
            # Strip outer brackets
            outer_brackets = False
            local_corr = getattr(node, correlation_location)
            if (local_corr[0] == "(") and (local_corr[-1] == ")"):
                local_corr = local_corr[1:-1]
                outer_brackets = True
            
            split_condition = filter(None, re.split(connector, local_corr))
            for cond in split_condition:
                if correlated_relation in cond:
                    after_condition.append(cond)
                else:
                    before_condition.append(cond)
            
            # Fix before condition
            if len(before_condition) == 1:
                before_condition = before_condition[0]
            else:
                before_condition = connector.join(before_condition)
                if (outer_brackets == True) and (before_condition != ""):
                    before_condition = "(" + before_condition + ")"
        else:
            after_condition = [getattr(node, correlation_location)]
    else:
        after_condition = [getattr(node, correlation_location)]
    
    # Get the correlated key from the after condition
    left_node_condition = []
    correlated_keys = []
    for af_cond in after_condition:
        if " = " in af_cond:
            local_after_cond = af_cond
            # String brackets
            if (local_after_cond[0] == "(") and (local_after_cond[-1] == ")"):
                local_after_cond = local_after_cond[1:-1]
            
            for part in local_after_cond.split(" = "):
                if str(correlated_relation+".") in part:
                    # TODO: Should we just be overwriting the correlated_key like this, what if we have two?!
                    correlated_keys.append(str(part).strip())
                else:    
                    # Set the left_node condition
                    left_node_condition.append(str(part).strip())
        else:    
            raise Exception("Unrecognised formation for the after_condition")
    
    # Fix after condition        
    if len(after_condition) == 1:
        after_condition = after_condition[0]
    else:
        after_condition = connector.join(after_condition)
        # Add back in if we have removed them
        if (outer_brackets == True) and (after_condition != ""):
            after_condition = "(" + after_condition + ")"
    
    # Add the correlated key to the join output
    join_output = list(node.output)
    for c_key in correlated_keys:
        join_output.append(c_key)
    
    # Create new nodes
    new_node = hash_join_node("Hash Join", join_output, False, "Inner", after_condition)
    
    # First create the left node
    # Create the left node output
    left_node_output = list(node.output)
    # Check if the left_node condition is in the output, add it if not
    for ln_cond in left_node_condition:
        if ln_cond not in left_node_output:
            left_node_output.append(ln_cond)
    left_node = seq_scan_node("Seq Scan", left_node_output, node.relation_name, node.alias)
    
    if (before_condition != "") and (before_condition != []):
        left_node.add_filters(before_condition)
        if hasattr(node, "filter"):
            raise Exception("We have some filters to carry forward and have also added something already to filters, we are unsure how to combine these!")
    elif hasattr(node, "filter"):
        # Add node filters if it has filters
        left_node.add_filters(node.filter)
    # Add left node parent
    left_node.set_parent(new_node)
        
    # Create right node
    right_node = seq_scan_node("Seq Scan", correlated_keys, correlated_relation, correlated_relation)
    # Add left node parent
    right_node.set_parent(new_node)
    
    new_node.set_plans([left_node, right_node])
    # Set new_node parent
    new_node.set_parent(node.parent)
    
    return new_node, correlated_keys


def determine_if_correlated_subquery(tree):
    """Determine if the explain plan features a correlated subquery or not

    Args:
        tree (object): explain tree for input SQL query, from the SubPlan declared node
    
    Returns:
        returnValue (boolean): true or false for whether or not we have a correlated subquery
    """
    returnValue = False
    
    # Iterate down the tree in a queue once to gather aliases
    gatheredAliases = set()
    treeQueue = Queue()
    treeQueue.put(tree)
    while (not treeQueue.empty()):
        treeNode = treeQueue.get()
        if treeNode == None:
            continue
        else:
            # Act on the current node
            
            # We want to look at the nodes with aliases and relations
            if hasattr(treeNode, "alias"):
                gatheredAliases.add(treeNode.alias)
                
            # Iterate on lower nodes
            if treeNode.plans != None:
                for nodeBelow in treeNode.plans:
                    # Add them to the queue
                    treeQueue.put(nodeBelow)
    
    # Iterate down the tree in a queue
    treeQueue = Queue()
    treeQueue.put(tree)
    while (not treeQueue.empty()):
        treeNode = treeQueue.get()
        if treeNode == None:
            continue
        else:
            # Act on the current node
            
            # Iterate through it's search locations
            for search_location in alias_locations[treeNode.node_type]:
                if hasattr(treeNode, search_location):
                    # If the set from relation_finder is not a subset of gathered_aliases
                    # Then we have a correlated subquery
                    
                    # As we have a relation that is been detected as part of a node,
                    # But is not one that we have gathered already, not an alias we've brought in already 
                    if relation_finder(getattr(treeNode, search_location)).issubset(gatheredAliases):
                        pass
                    else:
                        # Correlated Subquery
                        return True, gatheredAliases
            
            # Iterate on lower nodes
            if treeNode.plans != None:
                for nodeBelow in treeNode.plans:
                    # Add them to the queue
                    treeQueue.put(nodeBelow)
    
    return returnValue, gatheredAliases
    
def relation_finder(in_list):
    relations_set = set()
    if isinstance(in_list, str):
        in_list = [in_list]
    
    for process_string in in_list:
        regex = r"[A-Za-z0-9\_]+(?=\.)"
        matches = re.finditer(regex, process_string, re.MULTILINE)

        for matchNum, match in enumerate(matches, start=1):
            pending_match = str(match.group())
            if pending_match.isdigit():
                # Don't add a "relation" that is actually just digits, this wouldn't be valid
                pass
            else:
                relations_set.add(pending_match)
    return relations_set

def delete_tree_branches_by_id(tree, ids):
    # Preorder Traversal
            
    # Check current node
    if tree.plans != None:
        to_remove = []
        for i in range(len(tree.plans)):
            below_current_node = tree.plans[i]
            if get_class_id(below_current_node) in ids:
                # Remove it
                to_remove.append(i)
        
        # Do in reverse to not cause logic errors
        for i in sorted(to_remove, reverse=True):
            del tree.plans[i]
    
    # Run this function on below nodes
    if tree.plans != None:
        for individual_plan in tree.plans:
            delete_tree_branches_by_id(individual_plan, ids)     

def solve_view_set_values(tree_list):
    # Gather all the set_values and their original names
    # In a list called replaces, with 1st element original and 2nd the new name
    replaces = []
    
    for i in range(len(tree_list)):
        if isinstance(tree_list[i], tuple):
            # We have a replace
            set_value = tree_list[i][1]
            # Edit set_value
            new_set_value = set_value.lower()
            if "dollar" in set_value:
                new_set_value = new_set_value.replace("dollar_", "$")
                
                replaces.append((new_set_value, set_value))
            elif "subplan " in set_value.lower():
                new_set_value = new_set_value.replace("subplan ", "subplan_")
                replaces.append((set_value, new_set_value))
    
    # Recursively move through the tree, changing if we see the presence of replace[i][0] to replace[i][1]
    for i in range(len(tree_list)):
        if isinstance(tree_list[i], tuple):
            # Don't run for a list that is a subplan
            pass
        else:
            recurse_and_replace(tree_list[i], replaces)
    
def recurse_and_replace(tree, replaces):
    # Recursively move through the tree, changing if we see the presence of replace[i][0] to replace[i][1]
    
    # We want to use a post-order traversal
    # First we traverse the left subtree, then the right subtree and finally the root node.
    if tree.plans != None:
        for individual_plan in tree.plans:
            recurse_and_replace(individual_plan, replaces)
    
    # Do alterations to all the alteration places, based on the content in replaces
    for original, new in replaces:
        locs = alias_locations[tree.node_type]
        for attribute in locs:
            if hasattr(tree, attribute):
                # Iterate through output
                modified_attribute = getattr(tree, attribute)
                # Modified attribute may not always be a list
                if isinstance(modified_attribute, list):
                    for i in range(len(modified_attribute)):
                        modified_attribute[i] = modified_attribute[i].replace(original, new)
                elif isinstance(modified_attribute, str):
                    modified_attribute = modified_attribute.replace(original, new)
                elif modified_attribute is None:
                    continue
                else:
                    raise ValueError("Attribute is not a type we expected")
                # Set it back
                setattr(tree, attribute, modified_attribute)
            else:
                # The class doesn't have this attribute, ignore it
                pass 
    
def make_tree_from_pg(json, tree, parent=None):
    # First node check
    if tree == None:
        node = json["Plan"]
    else:
        node = json

    node_class = None    
    node_type = node["Node Type"]
    if node_type.lower() == "limit":
        node_class = limit_node(node_type, node["Output"])
    elif node_type.lower() == "aggregate":
        if "Group Key" in node:
            node_class = group_aggregate_node("Group " + node_type, node["Output"], node["Group Key"])

            # Handle adding Filter
            if "Filter" in node:
                node_class.add_filter(node["Filter"])
        else:
            node_class = aggregate_node(node_type, node["Output"])
        
        # Add SubPlan
        if "Subplan Name" in node:
            node_class.add_subplan(node["Subplan Name"])
    elif node_type.lower() == "gather":    
        node_class = gather_node(node_type, node["Output"], node["Workers Planned"], node["Single Copy"])
    elif node_type.lower() == "seq scan":
        node_class = seq_scan_node(node_type, node["Output"], node["Relation Name"], node["Alias"])
        
        # Check if a filter exists and add it
        if "Filter" in node:
            node_class.add_filters(node["Filter"])
        
        # Check if a parent relationship exists and add it
        if "Parent Relationship" in node:
            # Add Subplan
            if node["Parent Relationship"] == "SubPlan":
                node_class.add_subplan_name(node["Subplan Name"])
    elif node_type.lower() == "sort":
        node_class = sort_node(node_type, node["Output"], node["Sort Key"])
        
    elif node_type.lower() == "subquery scan":
        node_class = subquery_scan_node(node_type, node["Output"], node["Alias"])
    elif node_type.lower() == "nested loop":
        node_class = nested_loop_node(node_type, node['Output'], node['Inner Unique'], node['Join Type']) 
        
        if "Join Filter" in node:
            node_class.add_filter(node["Join Filter"])
    elif node_type.lower() == "hash join":
        node_class = hash_join_node(node_type, node['Output'], node['Inner Unique'], node['Join Type'], node['Hash Cond'])
        if "Filter" in node:
            node_class.add_filter(node['Filter'])
        elif "Join Filter" in node:
            node_class.add_filter(node['Join Filter'])
    elif node_type.lower() == "hash":
        node_class = hash_node(node_type, node['Output'])
    elif node_type.lower() == "index scan":
        node_class = index_scan_node(node_type, node['Scan Direction'], node['Index Name'], node['Relation Name'], node['Alias'], node['Output']) 
        
        if "Index Cond" in node:
            node_class.add_index_cond(node['Index Cond'])
        if "Filter" in node:
            node_class.add_filter(node['Filter'])
    elif node_type.lower() == "incremental sort":
        # node_type, parallel_aware, async_capable, output, sort_key, presorted_key
        node_class = incremental_sort_node(node_type, node["Output"], node["Sort Key"], node["Presorted Key"])
    elif node_type.lower() == "merge join":
        node_class = merge_join_node(node_type, node['Output'], node['Inner Unique'], node['Join Type'], node['Merge Cond'])
        
        if "Join Filter" in node:
            node_class.add_filter(node['Join Filter'])
    elif node_type.lower() == "materialize":
        node_class = materialize_node(node_type, node['Output'])
    elif node_type.lower() == "index only scan":
        node_class = index_only_scan_node(node_type, node["Scan Direction"], node["Index Name"], node["Relation Name"], node["Alias"], node["Output"])
        if "Filter" in node:
            node_class.add_filter(node['Filter'])
    elif node_type.lower() == "bitmap index scan":
        node_class = bitmap_index_scan_node(node_type, node["Index Name"])
        if "Index Cond" in node:
            node_class.add_index_cond(node['Index Cond'])
    elif node_type.lower() == "bitmap heap scan":
        node_class = bitmap_heap_scan_node(node_type, node["Output"], node["Relation Name"], node["Alias"], node["Recheck Cond"])
    elif node_type.lower() == "unique":
        node_class = unique_node(node_type, node["Output"])
    elif node_type.lower() == "group":
        node_class = group_node(node_type, node["Output"], node["Group Key"])
        if "Filter" in node:
                node_class.add_filter(node['Filter'])
    
    else:
        raise Exception("Node Type", node_type, "is not recognised, many Node Types have not been implemented.")
    
    # Do Parent Relationship
    if "Parent Relationship" in node:
        node_class.set_parent_relationship(node["Parent Relationship"])
    
    # Check if this node has a parent
    if parent != None:
        # We have a parent
        node_class.set_parent(parent)
        
    # Check if this node has a child
    if "Plans" in node:
        node_class_plans = []
        for individual_plan in node['Plans']:
            node_class_plans.append(make_tree_from_pg(individual_plan, "", node_class))
        node_class.set_plans(node_class_plans)
    
    return node_class

def create_in_capture(sql):
    # We want to scan the SQL for IN statements, and extract them
    # Then put them in the form of
    # [attribute, [values..]]
    in_capture = []
    
    regex = r"\w+\s(IN|in)\s\([^\)]+"
    matches = re.finditer(regex, sql, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
            
        gp_match = match.group()
        if " in " in gp_match:
            attribute = str(gp_match.split(" in ")[0]).strip()
            vals = str(str(str(gp_match.split(" in ")[1]).strip())[1:]).split(",")
        elif " IN " in gp_match:
            attribute = str(gp_match.split(" IN ")[0]).strip()
            vals = str(str(str(gp_match.split(" IN ")[1]).strip())[1:]).split(",")    
        else:
            raise Exception("In structure not recognised")
        
        # Remove ' in vals
        for i in range(len(vals)):
            vals[i] = str(vals[i]).strip()
            if (vals[i][0] == "'") and (vals[i][-1] == "'"):
                vals[i] = vals[i][1:-1]
        
        new_attr = attribute not in [i[0] for i in in_capture]
        new_vals = False
        for i in range(len(in_capture)):
            if attribute == in_capture[i][0]:
                if vals != in_capture[i][1]:
                    new_vals = True
                    break 
        if (new_attr == True) or (new_vals == True):
            in_capture.append((attribute, vals)) 
    
    return in_capture

def make_tree_from_duck(json, tree, sql):
    # Make the: IN Capture
    in_capture = create_in_capture(sql)
            
    # Make the Class tree
    explain_tree = make_class_tree_from_duck(json, tree, in_capture) 
    
    # Iterate through the class tree, fix column references
    duck_fix_class_tree(explain_tree) 
    
    # Carry out remove laters
    duck_fix_remove_laters(explain_tree)
    
    return explain_tree

def duck_fix_remove_laters(tree):
    # Run this function on below nodes
    if tree.plans != None:
        for individual_plan in tree.plans:
            duck_fix_remove_laters(individual_plan)   
            
    # Preorder Traversal
    
    if tree.plans != None:
        for i in range(len(tree.plans)):
            current_node = tree.plans[i]
            if hasattr(current_node, "remove_later"):
                if current_node.remove_later == True:
                    # We have found a node with remove_later attr
                    # Get the Prune's children (child)
                    if len(current_node.plans) == 1:
                        prune_children = current_node.plans[0]
                    else:
                        raise ValueError("We have assumed a sort node only has one child, not the case in reality!")
                    
                    # Set the parent of prune_children to be tree.plans[i]
                    prune_children.parent = tree
                    
                    # Set current_node to be the children
                    tree.plans[i] = prune_children
    

def duck_fix_class_tree(tree):
    # Change to a postorder traversal
    # Check if this node has a child
    if hasattr(tree, "plans"):
        if tree.plans != None:
            for individual_plan in tree.plans:
                duck_fix_class_tree(individual_plan)
    
    # Column reference regex
    col_ref = r"\#[0-9]*(?!.*('|\ ))"
    
    # Process current
    # Do we have lower references, that we need to fix
    need_col_fix = False
    if tree.node_type in alias_locations:
        for loc in alias_locations[tree.node_type]:
            if hasattr(tree, loc):
                if isinstance(getattr(tree, loc), list):
                    for individual_attr in getattr(tree, loc):
                        reg = re.search(col_ref, individual_attr)
                        if reg != None:
                            need_col_fix = True
                            break
                else:
                    reg = re.search(col_ref, getattr(tree, loc))
                    if reg != None:
                        need_col_fix = True
                        break
            
    if need_col_fix == True:
        # Do column fix, first determine child of current node
        
        # Iterate through current node
        if tree.node_type in alias_locations:
            for loc in alias_locations[tree.node_type]:
                if hasattr(tree, loc):
                    if isinstance(getattr(tree, loc), list):
                        for i in range(len(getattr(tree, loc))):
                            reg = re.search(col_ref, getattr(tree, loc)[i])
                            if reg != None:
                                col_replace = reg.group(0)
                                col_index = int(col_replace.replace("#", ""))
                                
                                # Set child
                                local_child = None
                                if len(tree.plans) == 1:
                                    local_child = tree.plans[0]
                                else: 
                                    local_child = determine_local_child(tree, loc, col_replace)
                                
                                if local_child.output != []:
                                    getattr(tree, loc)[i] = str(str(getattr(tree, loc)[i]).replace(col_replace, local_child.output[col_index])).strip()
                    else:
                        reg = re.search(col_ref, getattr(tree, loc))
                        if reg != None:
                            col_replace = reg.group(0)
                            col_index = int(col_replace.replace("#", ""))
                            
                            # Set child
                            local_child = None
                            if len(tree.plans) == 1:
                                local_child = tree.plans[0]
                            else: 
                                local_child = determine_local_child(tree, loc, col_replace)
                            
                            if local_child.output != []:
                                setattr(tree, loc, str(str(getattr(tree, loc)).replace(col_replace, local_child.output[col_index])).strip())
                

def determine_local_child(tree, location, target):
    if len(tree.plans) != 2:
        raise Exception("Tree doesn't have two children like we expect, it has: " + str(len(tree.plans)))
    
    # Return value
    child = None
    
    related_attr = getattr(tree, location)
    if " = " in related_attr:
        if target in related_attr.split(" = ")[0]:
            # LHS
            # While not "remove_later"
            child = tree.plans[0]
            while hasattr(child, "remove_later") and (getattr(child, "remove_later") == True):
                if len(child.plans) != 1:
                    raise Exception("Child has too many children")
                child = child.plans[0]
            return 
        elif target in related_attr.split(" = ")[1]:
            # RHS
            child = tree.plans[1]
            while hasattr(child, "remove_later") and (getattr(child, "remove_later") == True):
                if len(child.plans) != 1:
                    raise Exception("Child has too many children")
                child = child.plans[0]
        else:
            raise Exception("Unexpected equal options for tree with two children")
    else:
        raise Exception("Unexpected options for tree with two children")
    
    return child

def process_extra_info(extra_info, in_capture):
    # Before running, construct in_plan_expected from in_capture
    # Rewrite in_capture to in_plan_expected
    in_plan_expected = []
    in_plan_desired = []
    for attr, vals in in_capture:
        local_vals = vals.copy()
        desired_str = str(attr) + " = ANY ([" + str(local_vals)[1:-1] + "])"
        in_plan_desired.append(desired_str)
        
        for i in range(len(local_vals)):
            local_vals[i] = str(attr) + " = '" + str(local_vals[i]) + "'"
            
        in_plan_expected.append(" OR ".join(local_vals))
    
    # Construct altered extra_info
    new_extra_info = list(filter(None, extra_info.split("\n")))
    # Replace out ".000"
    for i in range(len(new_extra_info)):
        new_extra_info[i] = new_extra_info[i].replace(".000", "")
        new_extra_info[i] = new_extra_info[i].replace("True AND ", "")
        new_extra_info[i] = new_extra_info[i].replace("count_star()", "count(*)")
        new_extra_info[i] = str(new_extra_info[i]).strip()
        if re.search("\w=\w", new_extra_info[i]) != None:
            new_extra_info[i] = new_extra_info[i].replace("=", " = ")
            
        # Search for in_plan_expected
        for j in range(len(in_plan_expected)):
            if in_plan_expected[j] in new_extra_info[i]:
                new_extra_info[i] = str(new_extra_info[i]).replace(in_plan_expected[j], in_plan_desired[j])
    
    return new_extra_info
                
def make_class_tree_from_duck(json, tree, in_capture, parent=None):
    # First node check
    if tree == None:
        # Assume only 1 child for top node
        node = json["children"][0]
    else:
        node = json
        
    node_class = None    
    node_type = node["name"]
    
    node["extra_info"] = process_extra_info(node["extra_info"], in_capture)
    
    top_n_special = False
    if node_type.lower() == "top_n":
        # We start with a limit node
        node_class = limit_node("Limit", [])
        # Below this we have a sort_node
        start_info = 0
    
        for idx, elem in enumerate(node["extra_info"]):
            if elem == '[INFOSEPARATOR]':
                start_info = idx
                break
        
        sort_keys = list(node["extra_info"][start_info+1:])
        # TODO: Hardcoded replace, in future, if theres a "."
        # Audit the LHS for relations that we are tracking up,
        # If it's not in there, then replace it
        for i in range(len(sort_keys)):
            if "c_orders" in sort_keys[i]:
                sort_keys[i] = sort_keys[i].replace("c_orders", "orders")
            elif "profit" in sort_keys[i]:
                sort_keys[i] = sort_keys[i].replace("profit", "nation")
            
        sort = sort_node("Sort", [], sort_keys)
        
        # Set plans
        node_class.set_plans([sort])
        # And set special
        top_n_special = True
    elif node_type.lower() == "limit":
        node_class = limit_node("Limit", node["extra_info"])
    elif node_type.lower() == "simple_aggregate":
        node_class = aggregate_node("Aggregate", node["extra_info"])
    elif node_type.lower() == "projection":
        # Make a projection node an aggregate node
        node_class = aggregate_node("Aggregate", node["extra_info"])
        node_class.add_remove_later(True)
    elif node_type.lower() == "hash_join":
        node_class = process_hash_join(node)
    elif node_type.lower() == "hash_group_by":
        node_class = process_hash_group_by(node)
    elif node_type.lower() == "filter":
        # If it's a filter node, check if we have a seq_scan below
        if len(node["children"]) != 1:
            raise Exception("Too many children, the filter node has: " + str(len(node["children"]) + " children."))
        
        child_name = str(node["children"][0]["name"])
        if child_name.lower() == "seq_scan":
            # child_copy
            child_copy = node["children"][0].copy()
            child_copy["extra_info"] = process_extra_info(child_copy["extra_info"], in_capture)
            # Get the class
            node_class = process_seq_scan(child_copy, node["extra_info"])
            
            # Change the json
            node = node["children"][0]
        elif child_name.lower() == "hash_join":
            # child_copy
            child_copy = node["children"][0].copy()
            child_copy["extra_info"] = process_extra_info(child_copy["extra_info"], in_capture)
            # Get the class
            node_class = process_hash_join(child_copy, node["extra_info"])
            
            # Change the json
            node = node["children"][0]
        elif child_name.lower() == "hash_group_by":
            # child_copy
            child_copy = node["children"][0].copy()
            child_copy["extra_info"] = process_extra_info(child_copy["extra_info"], in_capture)
            # Get the class
            node_class = process_hash_group_by(child_copy, node["extra_info"])
            
            # Change the json
            node = node["children"][0]    
        
        else:
            raise Exception("Child is not a seq_scan or hash_join, it's a: " + str(node["children"][0]["name"]))
        
    elif node_type.lower() == "seq_scan":
        node_class = process_seq_scan(node)
    else:
        raise Exception("Node Type", node_type, "is not recognised, many Node Types have not been implemented.")
    
    # Check if this node has a parent
    if parent != None:
        # We have a parent
        node_class.set_parent(parent)
        
    # Check if this node has a child
    if "children" in node:
        if node["children"] != []:
            node_class_plans = []
            for individual_plan in node['children']:
                node_class_plans.append(make_class_tree_from_duck(individual_plan, "", in_capture, node_class))
            
            if top_n_special == True:
                node_class.plans[0].set_plans(node_class_plans)
            elif top_n_special == False:
                node_class.set_plans(node_class_plans)
            else:
                raise Exception("Unknown top_n value")
    
    return node_class

def process_hash_group_by(json, external_filters=None):
    # Iterate through extra_info
    new_output = list(json["extra_info"])
    
    removes = []
    group_keys = []
    for i in range(len(new_output)):
        if not any([True for agg in agg_funcs if agg+"(" in new_output[i]]):
            group_keys.append(new_output[i])
        
    # Group aggregate filters should have no child aggregate
    child_relation = None
    
    node_class = group_aggregate_node("Group Aggregate", new_output, group_keys)
    
    if external_filters != None:
        node_class.add_filter(process_external_filters(external_filters, child_relation))
    
    return node_class

def determine_child_relation(item):
    # TODO: Hardcode for TPC-H
    child_relation = None
    
    if item[0] == "l":
        child_relation = "lineitem"
    elif item[0] == "o":
        child_relation = "orders"
    elif item[0] == "r":
        child_relation = "region"
    elif item[0] == "n":
        child_relation = "nation"
    elif item[0] == "ps":
        child_relation = "partsupp"
    elif item[0] == "p":
        child_relation = "part"
    elif item[0] == "c":
        child_relation = "customer"
    elif item[0] == "s":
        child_relation = "supplier"
    else:
        raise Exception("Unrecognised item")
    
    return child_relation

def process_hash_join(json, external_filters=None):
    
    hash_output = []
    # TODO: Hardcoded, but we should remove
    inner_unique = False
    join_type = json["extra_info"][0]
    condition = " AND ".join(json["extra_info"][1:])
    node_class = hash_join_node("Hash Join", hash_output, inner_unique, join_type, condition)
    
    child_relation = determine_child_relation(condition[0])

    if external_filters != None:
        node_class.add_filter(process_external_filters(external_filters, child_relation))
    
    return node_class

def process_external_filters(external_filters, child_relation):
    
    # Split on AND/OR
    for j in range(len(external_filters)):
        line_split = list(filter(None, re.split('(AND)|(OR)', external_filters[j])))
            
        for i in range(len(line_split)):
            if (line_split[i][0] == "(") and (line_split[i][-1] == ")"):
                line_split[i] = line_split[i][1:-1]
            
            if (line_split[i] != "AND") and (line_split[i] != "OR"):
                line_split[i] = str(line_split[i]).strip()
                if child_relation != None:
                    are_there_aggs = [agg+"(" for agg in agg_funcs if agg+"(" in line_split[i]]
                    if any(are_there_aggs):
                        # Put relation after agg_func
                        insert_length = sum(len(s) for s in are_there_aggs)
                        line_split[i] = line_split[i][:insert_length] + child_relation + "." + line_split[i][insert_length:]
                    else:
                        # Stick relation at end
                        line_split[i] = child_relation + "." + line_split[i]
                        
                    # Contains
                    if "contains" in line_split[i]:
                        # (part.p_name)::text ~~ '%green%'
                        items = str(str(line_split[i]).replace("contains(", ""))[:-1].split(", ")
                        line_split[i] = str(items[0]) + " ~~ '%" + str(items[1]) + "%'" 
        
        external_filters[j] = " ".join(line_split)
    external_filters = " AND ".join(external_filters)
    
    return external_filters

def process_seq_scan(json, external_filters=None):
    # Set the relation and alias
    if json["extra_info"][0] != "[INFOSEPARATOR]":
        relation_name = json["extra_info"][0]
        alias_name = json["extra_info"][0]
    
    # Set the Output
    output=None
    start_info = 0
    
    for idx, elem in enumerate(json["extra_info"]):
        if elem == '[INFOSEPARATOR]':
            start_info = idx
            break
    
    end_info = None
    # Check if there is a second info_separator, belies the prescent of filters
    for idx, elem in enumerate(json["extra_info"][start_info+1:]):
        if elem == '[INFOSEPARATOR]':
            end_info = idx + start_info + 1
            break
        
    filters = None
    if end_info == None:
        # No end, just use the rest
        output = json["extra_info"][start_info+1:]
    else:
        output = json["extra_info"][start_info+1:end_info]
        
        if json["extra_info"][end_info+1][:9] == "Filters: ":
            json["extra_info"][end_info+1] = json["extra_info"][end_info+1][9:]
            pre_filters = json["extra_info"][end_info+1:]
            
            # Split on AND/OR
            for j in range(len(pre_filters)):
                if (" AND " in pre_filters[j]) and (" OR " not in pre_filters[j]):
                    line_split = re.split('(AND)', pre_filters[j])
                elif (" AND " not in pre_filters[j]) and (" OR " in pre_filters[j]):
                    line_split = re.split('(OR)', pre_filters[j])
                else:
                    line_split = [pre_filters[j]]
                    
                for i in range(len(line_split)):
                    if (line_split[i] != "AND") and (line_split[i] != "OR"):
                        line_split[i] = str(line_split[i]).strip()
                        line_split[i] = relation_name + "." + line_split[i]
                
                pre_filters[j] = " ".join(line_split)
                
            filter_pre_string = " AND ".join(pre_filters)
            filters = str(filter_pre_string).strip()
        else:
            raise Exception("Trying to format a filter string, formatting: " + str(filter_pre_string))
    
    node_class = seq_scan_node("Seq Scan", output, relation_name, alias_name)
    
    # Set the filters
    if (filters != None) and (external_filters != None):
        node_class.add_filters(filters + " AND " + process_external_filters(external_filters, relation_name))
    elif filters != None:
        # Check if a filter exists and add it
        node_class.add_filters(filters)
    elif external_filters != None:
        # Check if a external_filters exists and add it
        node_class.add_filters(process_external_filters(external_filters, relation_name))
        
    return node_class