# Date conversion
import pandas as pd

# Clean up redundant brackets
import regex
def clean_extra_brackets(s):
    for i in s:s=regex.sub('(\(|^)\K(\((((?2)|[^()])*)\))(?=\)|$)',r'\3',s)
    return s

# Classes for pandas instructions
class filter_node():
    def __init__(self, data, params):
        self.data = data
        self.params = self.clean_params(params)
        
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
        # "(lineitem.l_shipdate >= '1993-01-01'::date) &
        # (lineitem.l_shipdate < '1994-01-01 00:00:00'::timestamp without time zone) &
        # (lineitem.l_discount >= 0.07) &
        # (lineitem.l_discount <= 0.09) &
        # (lineitem.l_quantity < '25'::numeric)"
        
        filters = "&".join(line_split)
        
        return filters
        
    def set_instructions(self, instructions):
        self.instructions = instructions
        
    def to_pandas(self, prev_df):
        # Edit params:
        params = self.params.replace(self.data, prev_df)
        
        return str("["+str(params)+"]"), None
        
class limit_node():
    def __init__(self, data, amount, file):
        self.data = data
        
        if amount == None:
            self.amount = self.get_amount(file)
        else:
            self.amount = amount
        
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
    
    def to_pandas(self, prev_df):
        return str(prev_df + ".head("+str(self.amount)+")"), None
        
class aggr_node():
    def __init__(self, data, aggr, file):
        self.data = data
        self.aggr = clean_extra_brackets(aggr)
        self.column_name = self.get_aggr_name(file)
        
    def set_instructions(self, instructions):
        self.instructions = instructions
        
    def get_aggr_name(self, file):
        # Split on the aggregation, should be:
        # AGGREGATION as AGGR_NAME
        # Therefore, split on aggregation, take the part after the aggr
        # String leading whitespace, split again (this time on spaces)
        # And take the second element, to return AGGR_NAAME
        return str((file.split(self.aggr)[1]).strip().split()[1])
    
    def to_pandas(self, prev_df):
        # 'sum(l_extendedprice * l_discount)'
        # result = (df1['col1'] * df2['col3']).sum()
        if "sum" in self.aggr:
            # The aggr operation is SUM!
            inner = str(self.aggr).split("sum")[1]
            inner = clean_extra_brackets(inner)
            cols = inner.split("*")
            for i in range(len(cols)):
                cols[i] = prev_df + "." + cols[i].strip()
            inner_string = " * ".join(cols)
            
            outer_string = "(" + inner_string + ").sum()"
        
        return str(self.column_name) + " = " + outer_string, self.column_name

# Functions to make pandas tree of classes
def make_pandas_list(class_tree, sql_file):
    """Function to make a pandas tree, a tree data structure of pandas operations
    from a class tree (built from the explain output on a sql file)

    Args:
        class_tree (plan_to_explain_tree): Class tree, build from explain output of sql file
        sql_file (sql file): serialised input of sql file
    """
    #read_sql_file_for_information(sql_file)
    
    pandas_list = []
    create_list(class_tree, pandas_list, read_sql_file_for_information(sql_file))
    
    # Reverse to get instructions in order
    pandas_list.reverse()
    return pandas_list
  
def create_list(class_tree, pandas_list, sql_file):
    stillPlans = True
    currentTreeNode = class_tree
    
    while stillPlans:
        # Process the node
        if currentTreeNode.node_type == "Limit":
            pandas_list.append(limit_node(None, None, sql_file))
        elif currentTreeNode.node_type == "Aggregate":
            if currentTreeNode.partial_mode == "Finalize":
                # Mode is finalize, we can add this
                pandas_list.append(aggr_node(None, currentTreeNode.output[0], sql_file))
        elif currentTreeNode.node_type == "Seq Scan":
            if hasattr(currentTreeNode, "filters"):
                # Check if is a filter type of Seq Scan
                pandas_list.append(filter_node(currentTreeNode.relation_name, currentTreeNode.filters))
        
        if currentTreeNode.plans == None:
            stillPlans = False
            break
        else:
            currentTreeNode = currentTreeNode.plans 
        
def read_sql_file_for_information(sql_file):
    f = open(sql_file, "r")
    file = f.read()
    file = ' '.join(file.split())
    return file