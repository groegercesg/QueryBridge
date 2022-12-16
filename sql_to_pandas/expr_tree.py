import re
from visualising_tree import plot_exp_tree

class Expression_Tree_Node:
    def __init__(self, val):
        self.val = val
        self.left = None
        self.right = None
    
    def isLeaf(self):
        return self.left == None and self.right == None
        
class Expression_Solver:
    def __init__(self, s, visualise, previous_dataframe):
        # Count number of columns used, for group_aggregate
        self.columns_count = 0
        self.prev_df = previous_dataframe
        self.expression_tree = self.expTree(s)
        if visualise != False:
            plot_exp_tree(self.expression_tree, visualise)
            
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
        elif node.val == "avg":
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
        self.agg_funcs = {"sum", "avg", "count", "max", "min", "distinct"}
        
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
        
        # Check for adjacent negatives
        s_split = self.solve_adj_minuses(s_split)

        for ch in s_split:
            # print("Currently doing: " + str(ch))
            if ch == '(':
                ops.append(ch)
            elif ch.isdigit():
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
        
        root = Expression_Tree_Node(ops.pop())
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
    
    tree = Expression_Solver(sql_hard_eqn, False, "PREV_DF")
    pandas = tree.evaluate()
    print(pandas)