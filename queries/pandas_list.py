# Date conversion
import pandas as pd

# SQL Parsing
from sqlglot import parse_one, exp

# Clean up redundant brackets
import regex
def clean_extra_brackets(s):
    for i in s:s=regex.sub('(\(|^)\K(\((((?2)|[^()])*)\))(?=\)|$)',r'\3',s)
    return s

# Classes for pandas instructions
class filter_node():
    def __init__(self, data, params, output, sql):
        self.data = data
        self.params = self.clean_params(params)
        self.output = self.process_output(output, sql)
        
    def process_output(self, output, sql):
        for i in range(len(output)):
            cleaned_output = clean_extra_brackets(output[i])
            if cleaned_output in sql.column_references:
                # We have an item in output that needs to be changed
                output_original_value = cleaned_output
                output[i] = (output_original_value, sql.column_references[output_original_value])
        return output
        
    def clean_params(self, params):  
        # Replace AND with & and convert to string
        filters = str(params.replace("AND", "&"))
        # Remove first and last brackets
        filters = filters[1:-1]
        
        import re
        regex = re.compile(r"::\w+(\s+\w+)*\)", flags=re.MULTILINE)
        
        # Iterate through filters, remove strings that start with ::
        line_split = filters.split("&")
        for i in range(len(line_split)):
            regex_search = regex.search(line_split[i])
            line_split[i] = regex.sub(")", line_split[i])
            if regex_search != None:
                # This provides information about the datatype the comparator should be
                parse_info = str(regex_search.group())[2:-1]
                text_splitted = line_split[i].split("'")
                value = text_splitted[1]
                if "date" in parse_info:
                    value = "pd.Timestamp('"+str(pd.to_datetime(value, format='%Y-%m-%d'))+"')"
                elif "timestamp" in parse_info:
                    value = "pd.Timestamp('"+str(pd.to_datetime(value, format='%Y-%m-%d %H:%M:%S'))+"')"
                elif "numeric" in parse_info:
                    value = str(int(value))
                else:
                    raise ValueError("In Filter of the Explain JSON, it contain some information about datatype that we weren't able to parse appropriately")
                line_split[i] = text_splitted[0] + value + ") "
        
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
        
class limit_node():
    def __init__(self, data, output, sql):
        self.data = data
        self.amount = self.process_amount(sql)
            
        self.output = self.process_output(output, sql)
        
    def process_output(self, output, sql):
        for i in range(len(output)):
            cleaned_output = clean_extra_brackets(output[i])
            if cleaned_output in sql.column_references:
                # We have an item in output that needs to be changed
                output_original_value = cleaned_output
                output[i] = (output_original_value, sql.column_references[output_original_value])
        return output
    
    def process_amount(self, sql):
        return sql.limit
        
    def set_instructions(self, instructions):
        self.instructions = instructions
        
    def get_amount(self, file):
        if "LIMIT" in file:
            intermediate = file.split("LIMIT")[1].strip()
            numeric_filter = filter(str.isdigit, intermediate)
            numeric_string = "".join(numeric_filter)
        elif "limit" in file:
            intermediate = file.split("limit")[1].strip()
            numeric_filter = filter(str.isdigit, intermediate)
            numeric_string = "".join(numeric_filter)
        else:
            raise ValueError("No Limit statement detected, is this really a LIMIT node?")
        
        return int(numeric_string)
    
    def to_pandas(self, prev_df, this_df):
        # print(self.output)
        return ["print(" + str(prev_df + ".head("+str(self.amount)+")") + ")"]
        
class aggr_node():
    def __init__(self, data, output, sql):
        self.data = data        
        self.output = self.process_output(output, sql)
        
    def process_output(self, output, sql):
        for i in range(len(output)):
            cleaned_output = clean_extra_brackets(output[i])
            if cleaned_output in sql.column_references:
                # We have an item in output that needs to be changed
                output_original_value = cleaned_output
                output[i] = (output_original_value, sql.column_references[output_original_value])
        return output

    def to_pandas(self, prev_df, this_df):
        instructions = [this_df + " = pd.Dataframe()"]
        
        for i, col in enumerate(self.output):
            if isinstance(col, tuple):
                # This is a column with an alias
                if "sum" in col[0]:
                    # The aggr operation is SUM!
                    inner = str(col[0]).split("sum")[1]
                    inner = clean_extra_brackets(inner)
                    cols = inner.split("*")
                    for j in range(len(cols)):
                        cols[j] = prev_df + "." + cols[j].strip()
                    inner_string = " * ".join(cols)
                    
                    outer_string = "(" + inner_string + ").sum()"
                    statement = this_df + "['" + str(col[1]) + "'] = [" + outer_string + "]"
                else:
                    raise ValueError("Other types of aggr haven't been implemented yet!")
            else:
                # No alias, so just output!
                if "sum" in col:
                    # The aggr operation is SUM!
                    inner = str(col).split("sum")[1]
                    inner = clean_extra_brackets(inner)
                    cols = inner.split("*")
                    for j in range(len(cols)):
                        cols[j] = prev_df + "." + cols[j].strip()
                    inner_string = " * ".join(cols)
                    
                    outer_string = "(" + inner_string + ").sum()"
                    statement = this_df + "['col_"+ str(i) + "'] = [" + outer_string + "]"
                else:
                    raise ValueError("Other types of aggr haven't been implemented yet!")
            # Append instructions
            instructions.append(statement)
            
        return instructions

class sql_class():
    def __init__(self, sql_file):
        self.file_content = self.read_sql_file_for_information(sql_file)
        
        # Dictionary of column aliases
        # Get column projection aliases from SQL file, 
        self.column_references = self.get_col_refs()
        
        # Value of limit
        # TODO: Hardcoded to 1 limit, what about multiple?
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
        
        if currentTreeNode.plans == None:
            stillPlans = False
            break
        else:
            currentTreeNode = currentTreeNode.plans 
        
