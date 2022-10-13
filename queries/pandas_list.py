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
def process_output(self, output, sql):
    for i in range(len(output)):
        cleaned_output = clean_extra_brackets(output[i])
        cleaned_output = clean_type_information(self, cleaned_output)
        # Remove brackets for comparison purposes
        brack_cleaned_output = cleaned_output.replace("(", "").replace(")", "")
        if brack_cleaned_output in sql.column_references:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, sql.column_references[brack_cleaned_output])
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
    def __init__(self, data, params, output, sql):
        self.data = data
        self.params = self.clean_params(params)
        self.output = process_output(self, output, sql)
        
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
        
    def to_pandas(self, prev_df, this_df):
        instructions = []
        
        # Edit params:
        params = self.params.replace(self.data, prev_df)
        statement1_string = "df_intermediate" + " = " + prev_df + "[" + str(params) + "]"
        instructions.append(statement1_string)
        
        # Limit df_filter to output columns
        statement2_string = this_df + " = df_intermediate[" + str(self.output) + "]"
        instructions.append(statement2_string)
        
        return instructions
             
class sort_node():
    def __init__(self, data, output, sort_key, sql):
        self.data = data
        self.output = process_output(self, output, sql)
        self.sort_key = self.process_sort_key(sort_key)
    
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
                if sort_split[1] == "DESC":
                    ascendings.append(False)
                elif sort_split[1] == "ASC":
                    ascendings.append(True)
                else:
                    raise ValueError("Sorting type not recognised!")
                # Add the key
                keys.append(sort_split[0].split(".")[1])
        return keys, ascendings
    
    def to_pandas(self, prev_df, this_df):
        instructions = []
        
        # Sorting to an intermediate dataframe
        columns, ascendings = self.sort_key
        statement1_string = "df_intermediate" + " = " + prev_df + ".sort_values(by=" + str(columns) + ", ascending=" + str(ascendings) + ")"
        instructions.append(statement1_string)
        
        # Limiting to output columns
        statement2_string = this_df + " = df_intermediate[" + str(self.output) + "]"
        instructions.append(statement2_string)
        
        return instructions

class limit_node():
    def __init__(self, data, output, sql):
        self.data = data
        self.amount = self.process_amount(sql)
        self.output = process_output(self, output, sql)
        
    def process_amount(self, sql):
        return sql.limit
    
    def to_pandas(self, prev_df, this_df):
        instructions = []
        
        # Limit to output columns
        statement1_string = this_df + " = " + prev_df + "[" + str(self.output) + "]"
        instructions.append(statement1_string)
        
        # Show the new dataframe
        statement2_string = "print(" + str(this_df + ".head("+str(self.amount)+")") + ")"
        instructions.append(statement2_string)
        
        return instructions
    
def do_aggregation(self, instructions, prev_df):
    for i, col in enumerate(self.output):
        if isinstance(col, tuple):
            # This is a column with an alias
            if "sum" in col[0]:
                # TODO: Only add "prev_df." to columns, don't add it to numbers!
                # The aggr operation is SUM!
                inner = str(col[0]).split("sum")[1]
                inner = clean_extra_brackets(inner)
                cols = inner.split("*")
                for j in range(len(cols)):
                    cols[j] = prev_df + "." + cols[j].strip()
                inner_string = " * ".join(cols)
                
                outer_string = "(" + inner_string + ").sum()"
                statement = "intermediate_df['" + str(col[1]) + "'] = [" + outer_string + "]"
            elif "avg" in col[0]:
                # The aggr operation is AVG!
                inner = str(col[0]).split("avg")[1]
                inner = clean_extra_brackets(inner)
                
                # Create statement
                statement = "intermediate_df['" + str(col[1]) + "'] = " + prev_df + "['" + inner + "'].mean()"
            elif "count" in col[0]:
                # The aggr operation is to count!
                inner = str(col[0]).split("count")[1]
                inner = clean_extra_brackets(inner)
                
                if inner == "*":
                    # Length of prev_df
                    statement = "intermediate_df['" + str(col[1]) + "'] = len(" + prev_df + ".index)" 
                else:
                    # Length of a column of prev_df
                    statement = "intermediate_df['" + str(col[1]) + "'] = len(" + prev_df + "['" + inner + "'])" 
            else:
                raise ValueError("Other types of aggr haven't been implemented yet!")
        else:
            # No alias, so just output!
            if "sum" in col:
                raise ValueError("SUM with no alias!")
            elif "avg" in col:
                raise ValueError("AVG with no alias!")
            elif "count" in col:
                raise ValueError("COUNT with no alias!")
            else:
                #raise ValueError("Other types of aggr haven't been implemented yet!")
                statement = "intermediate_df['" + str(col) + "'] = " + prev_df + "['" + str(col) + "']"
        # Append instructions
        instructions.append(statement)
        
    return instructions

def choose_aliases(self):
    print(self.output)
    
    return self.output

class group_aggr_node():
    def __init__(self, data, output, group_key, sql):
        self.data = data        
        self.output = process_output(self, output, sql)
        self.group_key = self.process_group_key(group_key)
    
    def process_group_key(self, group_key):
        grouping_keys = []
        for key in group_key:
            grouping_keys.append(key.split(".")[1])
        return grouping_keys

    def to_pandas(self, prev_df, this_df, usePostAggr):
        
        
        instructions = []
        
        # Group the data based on the keys
        statement1_string = "intermediate_df" + " = " + prev_df + ".groupby(" + str(self.group_key) + ")"
        instructions.append(statement1_string)
        
        # Do aggr
        instructions += do_aggregation(self, instructions, prev_df)
        
        # Limit to output columns
        statement2_string = this_df + " = " + "intermediate_df[" + str(self.output) + "]"
        instructions.append(statement2_string)
        
        choose_aliases(self)
        # print(instructions)
        # print(self.output)
        
        return instructions   
        
class aggr_node():
    def __init__(self, data, output, sql):
        self.data = data        
        self.output = process_output(self, output, sql)

    def to_pandas(self, prev_df, this_df):
        instructions = ["intermediate_df = pd.Dataframe()"]
        
        instructions += do_aggregation(self, instructions, prev_df)
        
        # Limit to output columns
        statement2_string = this_df + " = " + "intermediate_df[" + str(self.output) + "]"
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
def make_pandas_list(class_tree, sql_file):
    """Function to make a pandas tree, a tree data structure of pandas operations
    from a class tree (built from the explain output on a sql file)

    Args:
        class_tree (plan_to_explain_tree): Class tree, build from explain output of sql file
        sql_file (sql file): serialised input of sql file
    """
    
    # Process SQL file into information for LIMIT and AS, in a class structure
    sql = sql_class(sql_file)
    
    pandas_list = []
    create_list(class_tree, pandas_list, sql)
    
    # Reverse to get instructions in order
    pandas_list.reverse()
    return pandas_list
  
def create_list(class_tree, pandas_list, sql_class):
    stillPlans = True
    currentTreeNode = class_tree
    
    while stillPlans:
        # Process the node
        if currentTreeNode.node_type == "Limit":
            pandas_list.append(limit_node(None, currentTreeNode.output, sql_class))
        elif currentTreeNode.node_type == "Aggregate":
            if currentTreeNode.partial_mode == "Simple":
                # Mode is Simple, we can add this
                pandas_list.append(aggr_node(None, currentTreeNode.output, sql_class))
        elif currentTreeNode.node_type == "Seq Scan":
            if hasattr(currentTreeNode, "filters"):
                # Check if is a filter type of Seq Scan
                pandas_list.append(filter_node(currentTreeNode.relation_name, currentTreeNode.filters, currentTreeNode.output, sql_class))
        elif currentTreeNode.node_type == "Sort":
            pandas_list.append(sort_node(None, currentTreeNode.output, currentTreeNode.sort_key, sql_class))
        elif currentTreeNode.node_type == "Group Aggregate":
            pandas_list.append(group_aggr_node(None, currentTreeNode.output, currentTreeNode.group_key, sql_class))
        else:
            raise ValueError("The node: " + str(currentTreeNode.node_type) + " is not recognised. Not all node have been implemented")
        
        if currentTreeNode.plans == None:
            stillPlans = False
            break
        else:
            currentTreeNode = currentTreeNode.plans 
        
