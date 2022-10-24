# Date conversion
import pandas as pd

# SQL Parsing
from sqlglot import parse_one, exp

# Clean up redundant brackets
import regex
import re
def clean_extra_brackets(s):
    for i in s:s=regex.sub('(\(|^)\K(\((((?2)|[^()])*)\))(?=\)|$)',r'\3',s)
    return s

# Functions shared between classes
def process_output(self, output, codecomphelper):
    for i in range(len(output)):
        cleaned_output = clean_extra_brackets(output[i])
        cleaned_output = clean_type_information(self, cleaned_output)
        # Remove brackets for comparison purposes
        brack_cleaned_output = cleaned_output.replace("(", "").replace(")", "")
        # Remove relations (and dot), if they exist
        if hasattr(codecomphelper, "relations"):
            for relation in codecomphelper.relations:
                if relation in brack_cleaned_output:
                    brack_cleaned_output = brack_cleaned_output.replace(relation+".", "")
                    # Change output[i], this is for the case where we have a relation in the output
                    # But we don't have a column reference
                    output[i] = brack_cleaned_output
        if brack_cleaned_output in codecomphelper.sql.column_references:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, codecomphelper.sql.column_references[brack_cleaned_output])
    return output

def remove_range(sentence, matches):
    return "".join(
        [sentence[0:matches[i][0]] if i == 0 else 
         sentence[matches[i - 1][1]:matches[i][0]] if i != len(matches) else 
         sentence[matches[i - 1][1]::] for i in range(len(matches) + 1)
         ])
    
def do_replaces(sentence, replaces):
    for replace in replaces:
        sentence = sentence.replace(replace[0], replace[1], 1)
        
    return sentence

def clean_type_information(self, content):
    # print(content)

    regex = r"::\w+(\s+\w+)*"
    matches = re.finditer(regex, content, re.MULTILINE)
    remove_ranges = []
    replaces = []

    for matchNum, match in enumerate(matches, start=1):
        
        # print("Match {matchNum} was found at {start}-{end}: {match}".format(matchNum = matchNum, start = match.start(), end = match.end(), match = match.group()))
        value = content[:match.start()].split("'")[-2]
        # print("Old Value: " + str(value))
        match_str = str(str(match.group())[2:])
        if "date" in match_str:
            new_value = "pd.Timestamp('"+str(pd.to_datetime(value, format='%Y-%m-%d'))+"')"
        elif "timestamp" in match_str:
            new_value = "pd.Timestamp('"+str(pd.to_datetime(value, format='%Y-%m-%d %H:%M:%S'))+"')"
        elif "numeric" in match_str:
            new_value = str(int(value))
        elif "bpchar" in match_str:
            new_value = str("'" + value + "'")
            # replace single equals with double equals
            # Use the existing replace method to do this
            replaces.append(("=", "=="))
        else:
            raise ValueError("In clean_type_information, it contain some information about datatype that we weren't able to parse appropriately")
        # print("New Value: " + str(new_value))
        
        # Add target values to arrays for processing
        remove_ranges.append((match.start(), match.end()))
        replaces.append(("'"+value+"'", new_value))
        
    if remove_range != [] and replaces != []:
        content = remove_range(content, remove_ranges)
        # print(content)
        content = do_replaces(content, replaces)
        # print(content)
    
    return content

# Classes for pandas instructions
class filter_node():
    def __init__(self, data, params, output):
        self.data = data
        self.params = self.clean_params(params)
        self.output = output
        
    def set_nodes(self, nodes):
        self.nodes = nodes
        
    def clean_params(self, params):  
        # Replace AND with & and convert to string
        filters = str(params.replace("AND", "&"))
        # Remove first and last brackets
        filters = filters[1:-1]
        
        line_split = filters.split("&")
        for i in range(len(line_split)):
            line_split[i] = clean_type_information(self, line_split[i])
        
        filters = "&".join(line_split)
        
        return filters
        
    def set_instructions(self, instructions):
        self.instructions = instructions
        
    def to_pandas(self, prev_df, this_df, codeCompHelper):
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        instructions = []
        
        # Edit params:
        params = self.params.replace(self.data, prev_df)
        statement1_string = "df_intermediate" + " = " + prev_df + "[" + str(params) + "]"
        instructions.append(statement1_string)
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        statement2_string = this_df + " = " + "df_intermediate[" + str(output_cols) + "]"
        instructions.append(statement2_string)
        
        return instructions
             
class sort_node():
    def __init__(self, output, sort_key):
        self.output = output
        self.sort_key = self.process_sort_key(sort_key)
        
    def set_nodes(self, nodes):
        self.nodes = nodes
    
    def process_sort_key(self, sort_key):
        keys = []
        ascendings = []
        
        for individual_sort in sort_key:
            sort_split = individual_sort.split()
            if len(sort_split) == 1:
                # No DESC/ASC, therefore is ASC (implied)
                keys.append(sort_split[0].split(".")[1])
                ascendings.append(True)
            else:
                # There is a DESC or ASC
                if sort_split[-1] == "DESC":
                    ascendings.append(False)
                elif sort_split[-1] == "ASC":
                    ascendings.append(True)
                else:
                    raise ValueError("Sorting type not recognised!")
                # Add the key
                keys.append(sort_split[0].split(".")[1])
        return keys, ascendings
    
    def to_pandas(self, prev_df, this_df, codeCompHelper):
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        instructions = []
        
        # Sorting to an intermediate dataframe
        columns, ascendings = self.sort_key
        statement1_string = "df_intermediate = " + prev_df + ".sort_values(by=" + str(columns) + ", ascending=" + str(ascendings) + ")"
        instructions.append(statement1_string)
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        statement2_string = this_df + " = " + "df_intermediate[" + str(output_cols) + "]"
        instructions.append(statement2_string)
        
        return instructions

class limit_node():
    def __init__(self, output, sql):
        self.amount = self.process_amount(sql)
        self.output = output
        
    def set_nodes(self, nodes):
        self.nodes = nodes
        
    def process_amount(self, sql):
        return sql.limit
    
    def to_pandas(self, prev_df, this_df, codeCompHelper):
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        instructions = []
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        statement1_string = this_df + " = " + prev_df + "[" + str(output_cols) + "]"
        instructions.append(statement1_string)
        
        # Show the new dataframe
        statement2_string = "print(" + str(this_df + ".head("+str(self.amount)+")") + ")"
        instructions.append(statement2_string)
        
        return instructions
    
def aggregate_sum(sum_string, s_group=None, df_group=None):
    cols = sum_string.split(" ")
                
    # Check if cols[j][0] or [-1] is a bracket
    # And split it up if it is
    insertBrackets = []
    for j in range(len(cols)):
        
        if cols[j][0] == "(":
            cols[j] = cols[j][1:]
            insertBrackets.append((j, "("))
            
        # Handle multiple brackets at the start of a string
        k = 0
        brack_count = 0
        while k < len(cols[j]):
            if cols[j][k] == "(":
                brack_count += 1
                k += 1
            else:
                # Break out of while
                k = len(cols[j]) * 2
                break
        
        if brack_count != 0:        
            cols[j] = cols[j][:-brack_count]
            insertBrackets.append((j, "("*brack_count))
            
        
        # Handle multiple brackets at the end of a string
        k = 1
        brack_count = 0
        while k < len(cols[j]):
            if cols[j][-k] == ")":
                brack_count += 1
                k += 1
            else:
                # Break out of while
                k = len(cols[j]) * 2
                break
        
        if brack_count != 0:        
            cols[j] = cols[j][:-brack_count]
            insertBrackets.append((j+1, ")"*brack_count))
            
    # Carry out insert Brackets
    for i, insert in enumerate(insertBrackets):
        cols.insert(insert[0]+i, insert[1])
    
    # Define acceptable characters
    from string import ascii_letters
    char_set = set(ascii_letters + "_")
    
    for j in range(len(cols)):
        if isinstance(cols[j], str):
            if all(c in char_set for c in cols[j]):
                if s_group != None:
                    cols[j] = 's["' + cols[j].strip() + '"]'
                elif df_group != None:
                    cols[j] = df_group + "." + cols[j].strip()
                else:
                    raise ValueError("In aggregate_sum, at least one of the two should be True!")
                
    inner_string = " ".join(cols)
    
    return inner_string
    
def do_group_aggregation(self, instructions, codeCompHelper):
    instructions = ["df_intermediate = df_intermediate.apply(lambda s: pd.Series({"]
    for i, col in enumerate(self.output):
        if isinstance(col, tuple):
            # This is a column with an alias
            if "sum" in col[0]:
                # The aggr operation is SUM!
                inner = str(col[0]).split("sum")[1]
                inner = clean_extra_brackets(inner)
                
                inner_string = aggregate_sum(inner, s_group=True)
                
                outer_string = "(" + inner_string + ").sum()"
                statement = '    "' + str(col[1]) + '": ' + outer_string + ","
            elif "avg" in col[0]:
                # The aggr operation is AVG!
                inner = str(col[0]).split("avg")[1]
                inner = clean_extra_brackets(inner)
                
                # Create statement
                statement = '    "' + str(col[1]) + '": (s["' + inner + '"]).mean(),'
            elif "count" in col[0]:
                # The aggr operation is to count!
                inner = str(col[0]).split("count")[1]
                inner = clean_extra_brackets(inner)
                
                if inner == "*":
                    # Length of prev_df
                    statement = '    "' + str(col[1]) + '": len(s.index),' 
                else:
                    # Length of a column of prev_df
                    statement = '    "' + str(col[1]) + '":  len(df_intermediate["' + inner + '"]),' 
            else:
                raise ValueError("Other types of aggr haven't been implemented yet!")
        else:
            # No alias, so just output!
            if "." in col:
                col_no_df = str(col.split(".")[1])
            else:
                col_no_df = None
            if "sum" in col:
                raise ValueError("SUM with no alias!")
            elif "avg" in col:
                raise ValueError("AVG with no alias!")
            elif "count" in col:
                raise ValueError("COUNT with no alias!")
            elif col in codeCompHelper.indexes:
                # The column is one of the indexes, we skip it!
                continue
            elif col_no_df in codeCompHelper.indexes:
                # We sometimes have columns that look like:
                # lineitem.l_orderkey
                # Where l_orderkey is a known index, so we take the section after the dot 
                # To compare it
                # The column is one of the indexes, we skip it!
                continue
            else:
                #raise ValueError("Other types of aggr haven't been implemented yet!")
                #statement = "df_intermediate['" + str(col) + "'] = " + prev_df + "['" + str(col) + "']"
                
                # In aggregate skip column no aggregation to be done
                raise ValueError("Col is: " + str(col) + " We don't recognise this, help!")
        # Append instructions
        instructions.append(statement)
        
    instructions.append("}))")
    return instructions

def do_aggregation(self, prev_df):
    local_instructions = []
    for col in self.output:
        if isinstance(col, tuple):
            if "sum" in col[0]:
                # The aggr operation is SUM!
                inner = str(col[0]).split("sum")[1]
                inner = clean_extra_brackets(inner)
                
                inner_string = aggregate_sum(inner, df_group=prev_df)
              
                outer_string = "(" + inner_string + ").sum()"
                
                local_instructions.append("df_intermediate['" + col[1] + "'] = [" + outer_string + "]")
            else:
                raise ValueError("Not coded!")
        else:
            raise ValueError("Not Implemented Error")

    return local_instructions


def choose_aliases(self, cCHelper):
    output = []
    if cCHelper.usePostAggr:
        for col in self.output:
            appendingCol = None
            if isinstance(col, tuple):
                appendingCol = col[1]
            else:
                appendingCol = col
            # Append the column, first check if it is in the indexes, if so, skip
            if appendingCol in cCHelper.indexes:
                continue
            else:
                output.append(appendingCol)    
    else:
        for col in self.output:
            appendingCol = None
            if isinstance(col, tuple):
                appendingCol = col[0]
            else:
                appendingCol = col
            # Append the column, first check if it is in the indexes, if so, skip
            if appendingCol in cCHelper.indexes:
                continue
            else:
                output.append(appendingCol)    
    return output

class group_aggr_node():
    def __init__(self, output, group_key):
        self.output = output
        self.group_key = self.process_group_key(group_key)
        
    def set_nodes(self, nodes):
        self.nodes = nodes
    
    def process_group_key(self, group_key):
        grouping_keys = []
        for key in group_key:
            grouping_keys.append(key.split(".")[1])
        return grouping_keys

    def to_pandas(self, prev_df, this_df, codeCompHelper):
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        # Set group keys as the codeCompHelper indexes
        codeCompHelper.setIndexes(self.group_key)
        
        instructions = []
        
        # Group the data based on the keys
        statement1_string = "df_intermediate" + " = " + prev_df + ".groupby(" + str(self.group_key) + ")"
        instructions.append(statement1_string)
        
        # Do aggr
        instructions += do_group_aggregation(self, instructions, codeCompHelper)
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        statement2_string = this_df + " = " + "df_intermediate[" + str(output_cols) + "]"
        instructions.append(statement2_string)
        
        return instructions   
    
class merge_node():
    def __init__(self, condition, output):
        self.condition = condition
        self.output = output

    def set_nodes(self, nodes):
        self.nodes = nodes

    def process_condition_into_merge(self, left_prev_df, right_prev_df):
        # Strip brackets
        self.condition = clean_extra_brackets(self.condition)
        
        # Split into left and right
        split_cond = str(self.condition).split(" = ")
        left_side = str(split_cond[0]).split(".")
        left_table = str(left_side[0])
        left_cond = str(left_side[1])
        right_side = str(split_cond[1]).split(".")
        right_table = str(right_side[0])
        right_cond = str(right_side[1])
        
        # Create statement
        statement = left_prev_df+'.merge('+right_prev_df+', left_on="'+left_cond+'", right_on="'+right_cond+'")'
        
        return str(statement)

    def to_pandas(self, prev_dfs, this_df, codeCompHelper):
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        # Set prefixes
        if not isinstance(prev_dfs, list):
            raise ValueError("Inputted prev_df is not a list!")
        elif len(prev_dfs) != 2:
            raise ValueError("Too few previous dataframes specified")
        instructions = ["df_intermediate = pd.DataFrame()"]
        
        instructions.append("df_intermediate = " + self.process_condition_into_merge(prev_dfs[0], prev_dfs[1]))
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        statement2_string = this_df + " = " + "df_intermediate[" + str(output_cols) + "]"
        instructions.append(statement2_string)
            
        return instructions

class aggr_node():
    def __init__(self, output):    
        self.output = output

    def set_nodes(self, nodes):
        self.nodes = nodes

    def to_pandas(self, prev_df, this_df, codeCompHelper):
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        instructions = ["df_intermediate = pd.DataFrame()"]
        
        instructions += do_aggregation(self, prev_df)
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        statement2_string = this_df + " = " + "df_intermediate[" + str(output_cols) + "]"
        instructions.append(statement2_string)
            
        return instructions

class sql_class():
    def __init__(self, sql_file):
        self.file_content = self.read_sql_file_for_information(sql_file)
        
        # Dictionary of column aliases
        # Get column projection aliases from SQL file, 
        self.column_references = self.get_col_refs()
        
        # Value of limit
        # TODO: Hardcoded to a single limit statement, what about multiple?
        self.limit = self.get_limit()
        
    def get_limit(self):
        limit_amount = 0
        for limit in parse_one(self.file_content).find_all(exp.Limit):
            limit_amount = int(limit.expression.alias_or_name)
        return limit_amount
        
    def get_col_refs(self):
        column_references = dict()
        for select in parse_one(self.file_content).find_all(exp.Select):
            for projection in select.expressions:
                if str(projection) != str(projection.alias_or_name):
                    projection_original = " ".join(str(projection).split()[:-2]).lower()
                    # Remove all brackets for comparison purposes
                    projection_original = projection_original.replace("(", "").replace(")", "")
                    if projection_original in column_references:
                        raise ValueError("We are trying to process a SQL but finding multiple identical projections")
                    else:
                        column_references[projection_original] = str(projection.alias_or_name)

        return column_references
        
    def read_sql_file_for_information(self, sql_file):
        f = open(sql_file, "r")
        file = f.read()
        file = ' '.join(file.split())
        return file

# Functions to make pandas tree of classes
def make_pandas_tree(class_tree, sql_file):
    """Function to make a pandas tree, a tree data structure of pandas operations
    from a class tree (built from the explain output on a sql file)

    Args:
        class_tree (plan_to_explain_tree): Class tree, build from explain output of sql file
        sql_file (sql file): serialised input of sql file
    """
    
    # Process SQL file into information for LIMIT and AS, in a class structure
    sql = sql_class(sql_file)
    
    pandas_tree = create_tree(class_tree, sql)
    
    return pandas_tree

def create_tree(class_tree, sql_class):
    current_node = class_tree
    node_type = current_node.node_type
    
    node_class = None
    # Catch all the different options
    if node_type == "Limit":
        node_class = limit_node(current_node.output, sql_class)
    elif node_type == "Aggregate":
        if current_node.partial_mode == "Simple":
            # Mode is Simple, we can add this
            node_class = aggr_node(current_node.output)
    elif node_type == "Seq Scan":
        if hasattr(current_node, "filters"):
            # Check if is a filter type of Seq Scan
            node_class = filter_node(current_node.relation_name, current_node.filters, current_node.output)
    elif node_type == "Sort":
        node_class = sort_node(current_node.output, current_node.sort_key)
    elif node_type == "Group Aggregate":
        node_class = group_aggr_node(current_node.output, current_node.group_key)
    elif node_type == "Hash Join":
        node_class = merge_node(current_node.hash_cond, current_node.output)
    elif node_type == "Nested Loop":
        # Make a nested loop into a merge node
        if hasattr(current_node, "merge_cond"):
            node_class = merge_node(current_node.merge_cond, current_node.output)
        else:
            raise ValueError("We need our nested loop to have a merge condition, this should have been added by traversal")
    elif node_type == "Index Scan":
        # Make an index scan into a filter node
        node_class = filter_node(current_node.relation_name, current_node.filter, current_node.output)
    else:
        raise ValueError("The node: " + str(current_node.node_type) + " is not recognised. Not all node have been implemented")
    
    if current_node.plans != None:
        current_node_plans = []
        for individual_plan in current_node.plans:
            current_node_plans.append(create_tree(individual_plan, sql_class))
        node_class.set_nodes(current_node_plans)
        
    return node_class
