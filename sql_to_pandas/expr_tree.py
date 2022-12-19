import re
import copy
from queue import Queue
from visualising_tree import plot_exp_tree


def get_class_id(node):
    return str(id(node))

class Expression_Tree_Node:
    def __init__(self, val):
        self.val = val
        self.left = None
        self.right = None
    
    def isLeaf(self):
        return self.left == None and self.right == None
        
class Expression_Solver:
    def __init__(self, s, visualise, previous_dataframe, current_dataframe, before_counter=None, codeComp=None):
        # Count number of columns used, for group_aggregate
        self.columns_count = 0
        self.prev_df = previous_dataframe
        self.this_df = current_dataframe
        # Set the beforecounter
        if before_counter == None:
            self.before_counter = 0
        else:
            self.before_counter = before_counter
        self.expression_tree = self.expTree(s)
        if visualise != False:
            plot_exp_tree(self.expression_tree, visualise)
        if codeComp != None:
            self.codeComp = codeComp
            
    def group_aggregate(self):
        # Process the tree into Before, During and After for the Group Aggregation operations
        Before = []
        During = []
        After = []
        
        # Track which class_ids to new_names we need to apply after the queue
        name_replaces = {}
        
        # Make a deepcopy of the tree and work on that
        local_exp_tree = copy.deepcopy(self.expression_tree)
        
        # Iterate down the tree, using a queue
        treeQueue = Queue()
        treeQueue.put(local_exp_tree)
        while (not treeQueue.empty()):
            treeNode = treeQueue.get()
            if treeNode == None:
                continue
            else:
                
                # Iterate on lower nodes
                    
                # If val in self.agg_funcs
                    # We export the below to pandas using evaluate, we add it as before_1 (use a counter) to before
                    # We set the during to be an val aggregation of before_1, name this: val_before_1
                    # We update the val node to be: val_before_1
                    # And don't add this node to the queue for checking
                
                # Act on the current node
                if treeNode.val in self.agg_funcs:
                    # The current node is a aggregation function
                    if treeNode.left == None:
                        # Work on right
                        # Count number of operators, of right tree
                        # Only do the before if we have operators in the tree
                        if self.count_operators(treeNode.right) > 0:
                            # Do the Before
                            self.before_counter += 1
                            evaluation = self.evaluate(treeNode.right)
                            
                            # Remove relations from evaluation
                            no_brackets_evaluation = str(evaluation.replace("(" , "").replace(")", "")).strip()
                            extract_relation = str(no_brackets_evaluation.split(".", 1)[0]).strip()
                            no_relation_evaluation = str(no_brackets_evaluation.replace(extract_relation+".", "")).strip()
                            # Discover if no_relation_evaluation is a column reference
                            if no_relation_evaluation in self.codeComp.sql.column_references:
                                before_name = self.codeComp.sql.column_references[no_relation_evaluation]
                            else:
                                # Make our own name up
                                before_name = "before_" + str(self.before_counter)
                            
                            Before.append([before_name, evaluation])
                        else:
                            # We don't do the before section
                            # Because we don't need at do any aggregations prior
                            # But we can use the same during code
                            # So we set before_name to the evaluation
                            
                            # But we need to remove the brackets
                            # And get the relation name
                            evaluation = str(self.evaluate(treeNode.right))
                            if (evaluation[0] == "(") and (evaluation[-1] == ")"):
                                evaluation = evaluation[1:-1]
                            
                            relation_name = str(evaluation.split(".")[1]).strip()
                            
                            before_name = str(relation_name)
                        
                        # Do the During
                        during_name = str(treeNode.val) + str("_") + str(before_name)
                        During.append([during_name, before_name, str(treeNode.val)])
                        
                        # Do the After setting, for the name that we need to replace
                        name_replaces[get_class_id(treeNode)] = during_name
                    
                    elif treeNode.right == None:
                        # Work on left                        
                        # Count number of operators, of left tree
                        # Only do the before if we have operators in the tree
                        if self.count_operators(treeNode.left) > 0:
                            # Do the Before
                            self.before_counter += 1
                            before_name = "before_" + str(self.before_counter)
                            
                            Before.append([before_name, self.evaluate(treeNode.left)])
                        else:
                            # We don't do the before section
                            # Because we don't need at do any aggregations prior
                            # But we can use the same during code
                            # So we set before_name to the evaluation
                            
                            # But we need to remove the brackets
                            # And get the relation name
                            evaluation = str(self.evaluate(treeNode.left))
                            if (evaluation[0] == "(") and (evaluation[-1] == ")"):
                                evaluation = evaluation[1:-1]
                            
                            relation_name = str(evaluation.split(".")[1]).strip()
                            
                            before_name = str(relation_name)
                        
                        # Do the During
                        during_name = str(treeNode.val) + str("_") + str(before_name)
                        During.append([during_name, before_name, str(treeNode.val)])
                        
                        # Do the After setting, for the name that we need to replace
                        name_replaces[get_class_id(treeNode)] = during_name
                    
                    else:
                        raise ValueError("Aggregation Function where both left and right of it are none.")
                    
                else:
                    # Else run on whatever nodes below there may be
                
                    # Add the below nodes into the queue
                    if treeNode.left != None:
                        treeQueue.put(treeNode.left)
                    if treeNode.right != None:
                        treeQueue.put(treeNode.right)
        
        # Let's carry out the name changes to: local_exp_tree
        self.rename_and_delete_tree_branches_by_id(local_exp_tree, name_replaces)
            
        # At the end we add the run evaluate on the deepcopy tree, with the val_before_1's hopefully
        # And add that to after
        After.append(self.evaluate(local_exp_tree))
        
        return Before, During, After        
        
    def count_operators(self, tree):
        count_operators = 0
        # Increment variable if in operator list
        if tree.val in list(self.prio.keys()):
            count_operators += 1
            
        # Run this function on below branches
        if tree.left != None:
            count_operators += self.count_operators(tree.left)
        if tree.right != None:
            count_operators += self.count_operators(tree.right)
        
        return count_operators
    
    def rename_and_delete_tree_branches_by_id(self, tree, name_replaces):
        # Name Replaces = {
        #    class_id = new_val,
        #    ...
        # }
        
        # Preorder Traversal
                
        # Check current node
        if name_replaces.get(get_class_id(tree), None) != None:
            # This is a node we have to change the name of
            
            # Set left and right of the node to none
            tree.left = None
            tree.right = None
            
            # Change val of tree
            tree.val = str(self.this_df) + "." + str(name_replaces.get(get_class_id(tree), None))
            
        else:
            # Run this function on below branches
            if tree.left != None:
                self.rename_and_delete_tree_branches_by_id(tree.left, name_replaces)
            if tree.right != None:
                self.rename_and_delete_tree_branches_by_id(tree.right, name_replaces)
            
    def evaluate(self, modified_start=None):
        if self.expression_tree is None:
            return 0
        elif modified_start != None:
            return self.internal_evaluate(modified_start)
        else:
            return self.internal_evaluate(self.expression_tree)

            
    def internal_evaluate(self, node):
        # Empty tree
        if node is None:
            return 0
        
        # Leaf of tree
        if node.isLeaf():
            return str(node.val)
        
        # Post order traversal
        right_value = self.internal_evaluate(node.right)
        left_value = self.internal_evaluate(node.left)
        
        # Handle the operators
        if node.val == "+":
            return "(" + str(left_value) + " + " + str(right_value) + ")"
        elif node.val == "-":
            return "(" + str(left_value) + " - " + str(right_value) + ")"
        elif node.val == "*":
            return "(" + str(left_value) + " * " + str(right_value) + ")"
        elif node.val == "/":
            return "(" + str(left_value) + " / " + str(right_value) + ")"
        elif node.val == "sum":
            if node.left == None:
                return str(right_value) + ".sum()"
            elif node.right == None:
                return str(left_value) + ".sum()"
            else:
                raise ValueError("Not recognised values for left_value: " + str(left_value) + " and right_value: " + str(right_value))
        elif node.val == "mean":
            if node.left == None:
                return str(right_value) + ".mean()"
            elif node.right == None:
                return str(left_value) + ".mean()"
            else:
                raise ValueError("Not recognised values for left_value: " + str(left_value) + " and right_value: " + str(right_value))
        elif node.val == "count":
            if node.left == None:
                if node.right.val == "distinct":
                    return "len(" + str(right_value) + ")"
                else:
                    return str(right_value) + ".count()"
            elif node.right == None:
                if node.left.val == "distinct":
                    return "len(" + str(left_value) + ")"
                else:
                    return str(left_value) + ".count()"
            else:
                raise ValueError("Not recognised values for left_value: " + str(left_value) + " and right_value: " + str(right_value))
        elif node.val == "min":
            if node.left == None:
                return str(right_value) + ".min()"
            elif node.right == None:
                return str(left_value) + ".min()"
            else:
                raise ValueError("Not recognised values for left_value: " + str(left_value) + " and right_value: " + str(right_value))
        elif node.val == "max":
            if node.left == None:
                return str(right_value) + ".max()"
            elif node.right == None:
                return str(left_value) + ".max()"
            else:
                raise ValueError("Not recognised values for left_value: " + str(left_value) + " and right_value: " + str(right_value))
        elif node.val == "distinct":
            if node.left == None:
                return str(right_value) + ".unique()"
            elif node.right == None:
                return str(left_value) + ".unique()"
            else:
                raise ValueError("Not recognised values for left_value: " + str(left_value) + " and right_value: " + str(right_value))
        else:
            raise ValueError("Unrecognised operator: " + str(node.val))  

            
    def remove_empty_strings(self, s_split):
        # Track the position of empty strings
        deletes = []
        for i in range(len(s_split)):
            if s_split[i] == "":
                deletes.append(i)
                
        # Reverse deletes
        deletes.reverse()
        
        # Carry out deletion
        for idx in deletes:
            del s_split[idx]
        
        return s_split
    
    
    def solve_adj_minuses(self, s_split):
        # Track the position of items we want to delete
        deletes = []
        for i in range(len(s_split) - 1):
            if s_split[i] == "-":
                # Check if i+1 == "-"
                if s_split[i+1] == "-":
                    # We need to delete this index
                    deletes.append(i+1)
                    # And set "i" to be a plus
                    # That's the operation we're carrying out here, two minuses make a plus
                    s_split[i] = "+"
                    
        # Reverse deletes
        deletes.reverse()
        
        # Carry out deletion
        for idx in deletes:
            del s_split[idx]
        
        return s_split
        

    def expTree(self, s):
        self.prio = {'(': 1, '+': 2, '-': 2, '*': 3, '/': 3, "sum": 4, "avg": 4, "count": 4, "max": 4, "min": 4, "distinct": 4}
        self.agg_funcs = {"sum", "avg", "count", "max", "min", "distinct", "mean"}
        self.agg_mapping = {"avg": "mean"}
        
        ops = []
        stack = []
        
        # Preprocess s
        # Strip out spaces
        s = s.replace(" ", "")
        # Convert to lower
        s = s.lower()
        # Split on regex pattern
        pattern = r'([\=\+\-\%\*\/\)\(])'
        s_split = re.split(pattern, s)
        
        s_split = self.remove_empty_strings(s_split)
        
        #  TODO: Once we implement minuses as unary operators, this won't be needed anymore
        # Check for adjacent negatives
        s_split = self.solve_adj_minuses(s_split)

        for ch in s_split:
            # print("Currently doing: " + str(ch))
            if ch == '(':
                ops.append(ch)
            # For floats, we offer to replace a single decimal point and now check if it contains only digits
            elif (ch.isdigit()) or (ch.replace(".", "", 1).isdigit()):
                stack.append(Expression_Tree_Node(ch))
            elif not any(char.isdigit() for char in ch) and self.prio.get(ch, None) == None and ch != "(" and ch != ")":
                # No digits and not in the priority dictionary
                # Create string for the name:
                name = "(" + self.prev_df + "." + ch + ")"
                stack.append(Expression_Tree_Node(name))
                # Increment column count, this let's us track the number of columns the tree has
                self.columns_count += 1
            elif ch == ')':
                while ops[-1] != '(':
                    self.combine(ops, stack)
                # pop left '('
                ops.pop()
            else:
                while ops and self.prio[ops[-1]] >= self.prio[ch]: 
                    # must be >=, for test case "1+2+3+4+5"
                    self.combine(ops, stack)

                ops.append(ch)

        while (len(stack) > 1) or (len(ops) >= 1):
            self.combine(ops, stack)

        return stack[0]

    def combine(self, ops, stack):
        if not ops:
            return
        
        # Map avg to mean at this point 
        pop_ops = ops.pop()
        if pop_ops in self.agg_mapping:
            pop_ops = self.agg_mapping[pop_ops]
        
        root = Expression_Tree_Node(pop_ops)
        if root.val in self.agg_funcs:
            # We have a special aggr function, only set the right
            root.right = stack.pop()
        else:
            # right first then left
            root.right = stack.pop()
            root.left = stack.pop()

        stack.append(root)

if __name__ == "__main__":
    simple_eqn = "11+2"
    more_complex_eqn = "731 * 1 / 5132 * 131 - 31"
    bracketed_eqn = "(712 * 2) - 12"
    sql_easy_eqn = "o_totalprice + 2"
    sql_basic_eqn = "sum(o_totalprice) / avg(o_custkey)"
    sql_complex_eqn = "(count(o_custkey) + avg(o_totalprice)) / (sum(o_orderkey) + min(o_shippriority)) * 25"
    sql_simple_eqn = "sum(o_totalprice)"
    sql_hard_eqn = "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax))"
    sql_mixed_eqn = "count(o_custkey * (o_totalprice / 2)) + avg(o_totalprice)"
    # "count(o_custkey * (o_totalprice / -1)) + avg(o_totalprice)"
    
    tree = Expression_Solver(sql_mixed_eqn, False, "PREV_DF")
    # pandas = tree.evaluate()
    # print(pandas)
    before, during, after = tree.group_aggregate()
    print(before)
    print(during)
    print(after)