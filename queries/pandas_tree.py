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
        # store replaces for later
        replaces = []
        if hasattr(codecomphelper, "relations"):
            for relation in codecomphelper.relations:
                if relation in brack_cleaned_output:
                    replaces.append(relation+".")
                    brack_cleaned_output = brack_cleaned_output.replace(relation+".", "")
                    # Change output[i], this is for the case where we have a relation in the output
                    # But we don't have a column reference
                    #output[i] = brack_cleaned_output
        # Make lowercase as well            
        brack_cleaned_lower_output = brack_cleaned_output.lower()
        if brack_cleaned_output in codecomphelper.sql.column_references:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, codecomphelper.sql.column_references[brack_cleaned_output])
        elif brack_cleaned_lower_output in codecomphelper.sql.column_references:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, codecomphelper.sql.column_references[brack_cleaned_lower_output])
        else:
            output[i] = cleaned_output
        if replaces != []:
            for replace_relation in replaces:
                if isinstance(output[i], tuple):
                    # Convert out of tuple
                    output_tup_list = list(output[i])
                    output_tup_list[0] = str(output_tup_list[0]).replace(replace_relation, "")
                    output[i] = tuple(output_tup_list)
                else:
                    output[i] = str(output[i]).replace(replace_relation, "")
                    
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

def count_char(char, string):
    counter = 0
    for i in range(len(string)):
        if string[i] == char:
            counter += 1
            
    return counter
            
def clean_type_information(self, content):
    regex = r"::(\w+\[*\]*)(\s+\w+)*"
    matches = re.finditer(regex, content, re.MULTILINE)
    remove_ranges = []
    replaces = []

    for matchNum, match in enumerate(matches, start=1):
        
        # print("Match {matchNum} was found at {start}-{end}: {match}".format(matchNum = matchNum, start = match.start(), end = match.end(), match = match.group()))
        
        # TODO: This whole section for deciding the value could be firmed up and improved
        valueFind = None
        if "'" in content[:match.start()]:
            valueFind = "Quotes"
            value = content[:match.start()].split("'")[-2]
        else:
            valueFind = "Brackets"
            # Count types of brackets and go with which has more
            open_bracket_count = count_char("(", content[:match.start()])
            close_bracket_count = count_char(")", content[:match.start()])
            if open_bracket_count > close_bracket_count:
                # Use Open
                value = content[:match.start()].split("(")[-1]
                value = value.replace("(", "").replace(")", "")
            elif open_bracket_count < close_bracket_count:
                # Use Close
                value = content[:match.start()].split(")")[-1]
                value = value.replace("(", "").replace(")", "")
            else:
                # Equal number of open and close, use Open
                value = content[:match.start()].split("(")[-1]
                value = value.replace("(", "").replace(")", "")
        
        # print("Old Value: " + str(value))
        match_str = str(str(match.group())[2:])
        if "date" in match_str:
            new_value = "pd.Timestamp('"+str(pd.to_datetime(value, format='%Y-%m-%d'))+"')"
        elif "timestamp" in match_str:
            new_value = "pd.Timestamp('"+str(pd.to_datetime(value, format='%Y-%m-%d %H:%M:%S'))+"')"
        elif "numeric" in match_str:
            new_value = str(int(value))
        elif "text" in match_str:
            new_value = str(value)
        # ' (part.p_container = ANY (\'{"SM CASE","SM BOX","SM PACK","SM PKG"}\'::bpchar[])) '
        elif "bpchar[]" in match_str:
            edit_value = value[1:-1]
            # Require edit_value to be properly formatted
            if edit_value[0] != '"':
                # Split on commas
                split_edit = edit_value.split(",")
                for i in range(len(split_edit)):
                    if split_edit[i][0] != '"':
                        split_edit[i] = '"' + split_edit[i]
                    if split_edit[i][-1] != '"':
                        split_edit[i] = split_edit[i] + '"'
                        
                # Join back together
                edit_value = ",".join(split_edit)
            new_value = str("[" + edit_value + "]")
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
        if valueFind == "Quotes":
            replaces.append(("'"+value+"'", new_value))
        elif valueFind == "Brackets":
            replaces.append(("("+value+")", new_value))
        else:
            raise ValueError("Unrecognised value for valueFind: " + str(valueFind))
        
    if remove_range != [] and replaces != []:
        content = remove_range(content, remove_ranges)
        # print(content)
        content = do_replaces(content, replaces)
        # print(content)
    
    return content

def clean_filter_params(self, params):  
    # Replace AND with & and convert to string
    filters = str(params.replace("AND", "&"))
    filters = str(filters.replace("OR", "|"))
    # Remove first and last brackets
    filters = filters[1:-1]
    
    # Split on & and |, keep in original split
    line_split = re.split('([&|])',filters)
    for i in range(len(line_split)):
        # Don't try to clean type information if we have a bare "and" or "or"
        if line_split[i] != "&" and line_split[i] != "|":
            line_split[i] = clean_type_information(self, line_split[i])
            
            # If line_split[i] contains an "ANY", the replace with ".isin"
            if " = ANY " in line_split[i]:
                line_split[i] = line_split[i].replace(" = ANY ", ".isin")
    
    # Reassemble line_split
    # Join on nothing, should have spaces still in it
    filters = "".join(line_split)
    
    return filters

# Classes for pandas instructions
class filter_node():
    def __init__(self, data, params, output):
        self.data = data
        if params != None:
            self.params = clean_filter_params(self,params)
        else:
            self.params = None
        self.output = output
        
    def set_nodes(self, nodes):
        self.nodes = nodes
    
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
        if self.params != None:
            params = self.params.replace(self.data, prev_df)
            statement1_string = this_df + " = " + prev_df + "[" + str(params) + "]"
            instructions.append(statement1_string)
        
            output_cols = choose_aliases(self, codeCompHelper)
            
            # Limit to output columns
            statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
            instructions.append(statement2_string)
        else:            
            output_cols = choose_aliases(self, codeCompHelper)
            
            # Limit to output columns
            statement2_string = this_df + " = " + prev_df + "[" + str(output_cols) + "]"
            instructions.append(statement2_string)
            
        return instructions
             
class sort_node():
    def __init__(self, output, sort_key):
        self.output = output
        self.sort_key = sort_key
        
    def set_nodes(self, nodes):
        self.nodes = nodes
    
    def process_sort_key(self, codeCompHelper):
        keys = []
        ascendings = []
        
        for individual_sort in self.sort_key:
            # Clean individual_sort with clean_type_info
            individual_sort = clean_type_information(self, individual_sort)
            sort_split = individual_sort.split()
            if len(sort_split) == 1:
                # No DESC/ASC, therefore is ASC (implied)
                # Use process_output to clear the relation if present
                column = (process_output(self, [sort_split[0]], codeCompHelper))[0]
                keys.append(column)
                ascendings.append(True)
            else:
                # There is a DESC or ASC
                column = None
                if sort_split[-1] == "DESC":
                    ascendings.append(False)
                    column = individual_sort.split(" DESC")[0]
                elif sort_split[-1] == "ASC":
                    ascendings.append(True)
                    column = individual_sort.split(" ASC")[0]
                else:
                    raise ValueError("Sorting type not recognised!")
                
                # Check if the column is a reference from SQL
                # Use the process_output module
                column = (process_output(self, [column], codeCompHelper))[0]
                # If has tuple
                if isinstance(column, tuple):
                    if codeCompHelper.usePostAggr:
                        column = column[1]
                    else:
                        column = column[0]
                # Add the key
                keys.append(column)
        return keys, ascendings
    
    def to_pandas(self, prev_df, this_df, codeCompHelper):
        # Set sort_keys
        self.sort_key = self.process_sort_key(codeCompHelper)
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        instructions = []
        
        # Sorting to an intermediate dataframe
        columns, ascendings = self.sort_key
        statement1_string = this_df + " = " + prev_df + ".sort_values(by=" + str(columns) + ", ascending=" + str(ascendings) + ")"
        instructions.append(statement1_string)
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
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
        
        
        
        if codeCompHelper.column_ordering:
            output_cols = choose_aliases(self, codeCompHelper, final_output=True)
            # Undo axes to normal columns
            if codeCompHelper.indexes != []:
                statement1_string = this_df + " = " + prev_df + ".rename_axis(" + str(codeCompHelper.indexes) + ").reset_index()"
                instructions.append(statement1_string)
            
            # Limit to output columns
            if codeCompHelper.indexes != []:
                statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
                instructions.append(statement2_string)
            else:
                statement2_string = this_df + " = " + prev_df + "[" + str(output_cols) + "]"
                instructions.append(statement2_string)
        else:
            output_cols = choose_aliases(self, codeCompHelper, final_output=False)
            statement_string = this_df + " = " + prev_df + "[" + str(output_cols) + "]"
            instructions.append(statement_string)
                    
        statement3_string = "result = " + str(this_df) + ".head("+str(self.amount)+")"
        instructions.append(statement3_string)
        
        statement4_string = "return result"
        instructions.append(statement4_string)
        
        return instructions
    
def check_aggregate(string, s_group=None, df_group=None):
    cols = string.split(" ")
                
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
    
    # Track number of columns we encounter
    column_count = 0
    for j in range(len(cols)):
        if isinstance(cols[j], str):
            if all(c in char_set for c in cols[j]):
                if s_group != None:
                    cols[j] = 's["' + cols[j].strip() + '"]'
                    column_count += 1
                elif df_group != None:
                    cols[j] = df_group + "." + cols[j].strip()
                    column_count += 1
                else:
                    raise ValueError("In aggregate_sum, at least one of the two should be True!")
                
    inner_string = " ".join(cols)
    
    return inner_string, column_count
    
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
                    cols[j] = s_group + '["' + cols[j].strip() + '"]'
                elif df_group != None:
                    cols[j] = df_group + "." + cols[j].strip()
                else:
                    raise ValueError("In aggregate_sum, at least one of the two should be True!")
                
    inner_string = " ".join(cols)
    
    return inner_string

def inner_aggregation(string, prev_df):
    if "sum" in string:
        # The aggr operation is SUM!
        inner = str(string).split("sum")[1:]
        
        # Is the start of string important?
        before_string = str(string).split("sum")[0]
        if ("*" or "+" or "/" or "-" in before_string) and before_string != "":
            outer_string = []
            
            before_string = before_string.strip()
            
            # Clean bad brackets
            if before_string[0] == "(" and before_string[-1] == ")":
                # This is bracket and start and end so fine:
                pass
            else:
                if before_string[0] == "(":
                    # Therefore not one at end
                    before_string = before_string[1:]
                elif before_string[-1] == ")":
                    # Therefore not one at start
                    before_string = before_string[:-1]
                else:
                    # No brackets, fine
                    pass
            
            # Add to outer string
            outer_string.append(before_string.strip())
        
        # If it's a list then join it all back up together
        if isinstance(inner, list):
            inner = "".join(inner)
        inner = clean_extra_brackets(inner)
        
        if "CASE WHEN" and "THEN" and "ELSE" in inner:
            # Do case aggregation
            inner_string = aggregate_case(inner, prev_df)
        else:
            inner_string = aggregate_sum(inner, df_group=prev_df)
        
        # Check if variable exists
        if 'outer_string' in locals():
            if isinstance(outer_string, list):
                outer_string.append("(" + inner_string + ").sum()")
            else:
                outer_string = "(" + inner_string + ").sum()"
        else:
            # Doesn't exist, must be a string set then
            outer_string = "(" + inner_string + ").sum()"
        
    else:
        raise ValueError("Other types of aggregate not implemented")
    
    return outer_string

def aggregate_case(inner_string, prev_df):
    else_value = inner_string.split("ELSE")[1]
    when_value = inner_string.split("CASE WHEN")[1].split("THEN")[0]
    then_value = inner_string.split("THEN")[1].split("ELSE")[0]
    
    # Clean values
    # Put values in the order of the pandas expression
    values = [then_value, when_value, else_value]
    for i in range(len(values)):
        values[i] = values[i].strip()
        if values[i][0] == "(" and values[i][-1] == ")":
            # This is bracket and start and end so fine:
            pass
        else:
            if values[i][0] == "(":
                # Therefore not one at end
                values[i] = values[i][1:]
            elif values[i][-1] == ")":
                # Therefore not one at start
                values[i] = values[i][:-1]
            else:
                # No brackets, fine
                pass
    
    # Transform values into pandas equivalents
    for i in range(len(values)):
        # contains ~~
        # Starts with
        # https://www.postgresql.org/docs/current/functions-matching.html
        if "~~" in values[i]:
            values[i] = clean_extra_brackets(values[i])
            split_starts = values[i].split(" ~~ ")
            # First element of split_starts should be the column,
            # Second should be the comparator
            if len(split_starts) != 2:
                raise ValueError("Split Starts for case Aggregation should be of length 2")
            
            if "%" in split_starts[1]:
                split_starts[1] = split_starts[1].replace("%", "")
            else:
                raise ValueError("Aggregate Case, not sure what to clean")
            
            values[i] = 'x["' + split_starts[0] + '"].startswith("' + split_starts[1] + '")'
            
        # Hand this off to the s_group function
        elif ("*" or "-" in values[i]) and (len(values[i]) > 1):
            values[i] = aggregate_sum(values[i], s_group = "x")
    
    # li_pa_join.apply(lambda x: x["l_extendedprice"] * (1 - x["l_discount"]) if x["p_type"].startswith("PROMO") else 0, axis=1)
    inner_string = prev_df + '.apply(lambda x: ' + str(values[0]) + ' if ' + str(values[1]) + ' else ' + str(values[2]) + ', axis=1)'        
    
    return inner_string

def do_aggregation(self, prev_df, current_df):
    local_instructions = []
    for col in self.output:
        if isinstance(col, tuple):
            if "/" in col[0]:
                # Split on this, handle agg of both sides individually
                split_col = col[0].split("/")
                aggr_results = []
                for current_col in split_col:
                    inner_agg_results = inner_aggregation(current_col.strip(), prev_df)
                    if isinstance(inner_agg_results, list):
                        aggr_results += inner_agg_results
                    else:
                        aggr_results.append(inner_agg_results)
                
                # Join aggr results
                outer_string = ""
                for i in range(len(aggr_results)):
                    # Add to outer_string
                    # if not the last one add with a divide
                    add_value = ""
                    if i < len(aggr_results) - 1:
                        add_value = " / "
                        
                    if aggr_results[i][-1] == "/" or aggr_results[i][-1] == "*":
                        add_value = " "
                        
                    outer_string += aggr_results[i] + add_value
                    
                local_instructions.append(current_df + "['" + col[1] + "'] = [" + outer_string + "]")
            
            elif "sum" in col[0]:
                # The aggr operation is SUM!
                inner = str(col[0]).split("sum")[1]
                inner = clean_extra_brackets(inner)
                
                inner_string = aggregate_sum(inner, df_group=prev_df)
              
                outer_string = "(" + inner_string + ").sum()"
                
                local_instructions.append(current_df + "['" + col[1] + "'] = [" + outer_string + "]")
            else:
                raise ValueError("Not other types of aggregation haven't been implemented yet!")
        else:
            # We need to handle cases in here
            if "/" in col:
                # Split on this, handle agg of both sides individually
                split_col = col.split("/")
                aggr_results = []
                for col in split_col:
                    inner_agg_results = inner_aggregation(col.strip(), prev_df)
                    if isinstance(inner_agg_results, list):
                        aggr_results += inner_agg_results
                    else:
                        aggr_results.append(inner_agg_results)
                  
                # '(df_merge_1.apply(lambda x: ( s["l_extendedprice"] * ( 1 - s["l_discount"] )) if x["p_type].startswith("PROMO") else 0, axis=1)).sum() / (df_merge_1.l_extendedprice * ( 1 - df_merge_1.l_discount )).sum()'
                # '(100.00 * / (df_merge_1.apply(lambda x: ( s["l_extendedprice"] * ( 1 - s["l_discount"] )) if x["p_type"].startswith("PROMO") else 0, axis=1)).sum() /  / (df_merge_1.l_extendedprice * ( 1 - df_merge_1.l_discount )).sum()'
                
                # Join aggr results
                outer_string = ""
                for i in range(len(aggr_results)):
                    # Add to outer_string
                    # if not the last one add with a divide
                    add_value = ""
                    if i < len(aggr_results) - 1:
                        add_value = " / "
                    
                    if aggr_results[i][-1] == "/" or aggr_results[i][-1] == "*":
                        add_value = " "
                        
                    outer_string += aggr_results[i] + add_value
                    
                local_instructions.append(current_df + " = [" + outer_string + "]")
            elif "max" in col:
                # max(sum(l_extendedprice * (1 - l_discount)))
                 
                # The aggr operation is MAX!
                inner = str(col).split("max")[1]
                inner = clean_extra_brackets(inner)
                
                if "sum" in inner:
                    # The aggr operation is SUM!
                    inner_inner = str(inner).split("sum")[1]
                    inner_inner = clean_extra_brackets(inner_inner)
                    
                    inner_inner_string = aggregate_sum(inner_inner, df_group=prev_df)
                
                    inner = "(" + inner_inner_string + ").sum()"
                    
                outer_string = "(" + inner + ").max()"
                
                local_instructions.append(current_df + "['" + col[1] + "'] = [" + outer_string + "]")
            
            else:
                raise ValueError("Not Implemented Error, for col: " + str(col))

    return local_instructions


def choose_aliases(self, cCHelper, final_output=False):
    output = []
    if cCHelper.usePostAggr:
        for col in self.output:
            appendingCol = None
            if isinstance(col, tuple):
                appendingCol = col[1]
            else:
                appendingCol = col
        
            # Check if first and last characters are brackets
            if appendingCol[0] == "(" or appendingCol[-1] == ")":
                # It has brackets, let's clean them, then check if we need to use bracket replace
                new_appendingCol = clean_extra_brackets(appendingCol)
                
                # Check if new_appendingCol is in the cCHelper bracket_replace
                if cCHelper.bracket_replace.get(new_appendingCol, None) != None:
                    # This col is in the dict, use the replacement as the column in forward
                    appendingCol = cCHelper.bracket_replace[new_appendingCol]
                
            # Append the column, first check if it is in the indexes, if so, skip
            if final_output:
                output.append(appendingCol)
            elif appendingCol in cCHelper.indexes:
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
                
            # Check if appendingCol is in the cCHelper bracket_replace
            if cCHelper.bracket_replace.get(appendingCol, None) != None:
                # This col is in the dict, use the replacement as the column in forward
                appendingCol = cCHelper.bracket_replace(appendingCol)
            
            # Append the column, first check if it is in the indexes, if so, skip
            if final_output:
                output.append(appendingCol)
            if appendingCol in cCHelper.indexes:
                continue
            else:
                output.append(appendingCol)    
    return output

def handle_complex_aggregations(self, data, codeCompHelper, prev_df):
    # Array to hold decisions
    # Before Aggrs:
        # Array for aggregations that need to happen before grouping
        # Format: [column name, instruction]
    before_aggrs = []
    
    # After Aggrs:
        # Array for aggregations (simple ones) that can happen after grouping
        # Format: [column name, uses column, aggr type]
    after_aggrs = []
    
    # For each output in self.output
    # Determine how many columns it uses
            # Split off the sum avg or count
    for i, col in enumerate(data):
        aggr_type = None
        if isinstance(col, tuple):
            if "sum" in col[0]:
                aggr_type = "sum"
                inner = str(col[0]).split("sum")[1]
            elif "avg" in col[0]:
                aggr_type = "avg"
                inner = str(col[0]).split("avg")[1]
            elif "count" in col[0]:
                aggr_type = "count"
                inner = str(col[0]).split("count")[1]   
            else:
                raise ValueError("Other types of aggr not implemented. Such as: " + str(col[0]))  
            
            # At the moment we only look to aggr if we have a name for a column
            inner = clean_extra_brackets(inner)
            cleaned_inner, count = check_aggregate(inner, df_group=prev_df)
            # If count > 1
                # We need to do the inner part before, add to a before list
                # Save the after part in after list
            if count > 1:
                # Add to before_aggrs
                before_aggrs.append([col[1], cleaned_inner])
                # Add to after_aggrs
                after_aggrs.append([col[1], col[1], aggr_type])
            # If count <= 1
                # Save in after part list
            else:
                # We don't use cleaned_inner, it's junk.
                # We just use the inner originally
                # Add to after_aggrs
                after_aggrs.append([col[1], inner, aggr_type])
            
        else:
            if "." in col:
                col_no_df = str(col.split(".")[1])
            else:
                col_no_df = None   
            
            # Check if it's an index
            if col in codeCompHelper.indexes:
                    # The column is one of the indexes, we skip it!
                continue
            elif col_no_df in codeCompHelper.indexes:
                # We sometimes have columns that look like:
                # lineitem.l_orderkey
                # Where l_orderkey is a known index, so we take the section after the dot 
                # To compare it
                # The column is one of the indexes, we skip it!
                continue
            elif "sum" in col:
                #raise ValueError("SUM with no alias!")
                aggr_type = "sum"
                inner = str(col).split("sum")[1]
                inner = clean_extra_brackets(inner)
                
                after_aggrs.append([col, inner, aggr_type])
            elif "avg" in col:
                raise ValueError("AVG with no alias!")
            elif "count" in col:
                raise ValueError("COUNT with no alias!")
            else:
                # If these are columns with no aggregations
                # That are not indexes
                # In that case we should be grouping by them
                self.group_key.append(col)
                #raise ValueError("Col is: " + str(col) + ". We don't recognise this, help!")
     
    # Handle and rearrange output
    return before_aggrs, after_aggrs

class group_aggr_node():
    def __init__(self, output, group_key):
        self.output = output
        self.group_key = self.process_group_key(group_key)
        
    def add_filter(self, in_filter):
        self.filter = clean_filter_params(self, in_filter)
        
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
        
        if hasattr(self, "filter"):
            # We have a filter operation to do
            proc_filter = None
            if isinstance(self.filter, str):
                proc_filter = process_output(self, [self.filter], codeCompHelper)
            elif isinstance(self.filter, list):
                proc_filter = process_output(self, self.filter, codeCompHelper)
            else:
                raise ValueError("Unrecognised type of filter for the Group Aggregation Node, filter: " + str(self.filter))
        
            
            if len(proc_filter) == 1:
                self.filter = proc_filter[0]
            else:
                raise ValueError("Group Aggregation Node not written for multiple Filters")
            
            # Catch equality
            cut_filter = None
            filter_amount = None
            filter_type = None
            if "<" in self.filter:
                filter_type = "<"
                cut_filter = str(str(self.filter).split("<")[0]).strip()
                filter_amount = str(str(self.filter).split("<")[1]).strip()
            elif ">" in self.filter:
                filter_type = ">"
                cut_filter = str(str(self.filter).split(">")[0]).strip()
                filter_amount = str(str(self.filter).split(">")[1]).strip()
            elif "=" in self.filter:
                filter_type = "="
                cut_filter = str(str(self.filter).split("=")[0]).strip()
                filter_amount = str(str(self.filter).split("=")[1]).strip()
            else:
                raise ValueError("Haven't written Filter for Group Aggregation to manage filters like: " + str(self.filter))
            
            filter_steps = [[cut_filter, filter_type, filter_amount]]
            
            if isinstance(self.filter, str):
                before_filter, after_filter = handle_complex_aggregations(self, [cut_filter], codeCompHelper, prev_df)
            elif isinstance(self.filter, list):
                before_filter, after_filter = handle_complex_aggregations(self, cut_filter, codeCompHelper, prev_df)
            else:
                raise ValueError("Unrecognised type of filter for the Group Aggregation Node, filter: " + str(self.filter))
        
        instructions = []
        
        # Out of self.output determine which of these are complex aggregations
        # I.e. ones like: 'sum(l_extendedprice * (1 - l_discount))'
        # This is complex because it uses more than one column
        # We want to perform the inner part of this, the column multiplication
        # Prior to grouping
        
        # Note: We decide to let the "before" aggregations happen to the previous_df
        before_group, after_group = handle_complex_aggregations(self, self.output, codeCompHelper, prev_df)
        
        # Combine after_filter and before filter
        if hasattr(self, "filter"):
            before_group += before_filter
            after_group += after_filter
        
        # Handle before
        for before_name, before_command in before_group:
            instructions.append(prev_df + "['" + before_name + "'] = " + before_command)
        
        # Handle group
        instructions.append(this_df + " = " + prev_df + " \\")
        instructions.append("    .groupby(" + str(self.group_key) + ") \\")
        instructions.append("    .agg(")
        
        
        # Handle After
        #if aggr_type == "count" and inner == "*":
        #   after_aggrs.append([col[1], "len(s.index)", aggr_type])
        for after_name, after_col, after_operation in after_group:      
            # Handle brackets in output
            if "(" or ")" in after_name:
                # print("After name: " + str(after_name) + " has brackets in it.")
                # Replace these
                new_after_name = str(str(after_name).replace("(", "")).replace(")", "")
                codeCompHelper.add_bracket_replace(after_name, new_after_name)
                # Set to after_name for use now
                after_name = new_after_name    
            
            if after_operation == "sum":
                instructions.append('        ' + after_name + '=("' + after_col + '", "' + after_operation + '"),')
            elif after_operation == "avg":
                instructions.append('        ' + after_name + '=("' + after_col + '", "mean"),')
            elif after_operation == "count":
                if after_col == "*":
                    if len(codeCompHelper.indexes) < 1:
                        raise ValueError("No indexes in CodeCompHelper")
                    instructions.append('        ' + after_name + '=("' + codeCompHelper.indexes[0] + '", "count"),')
                else:
                    instructions.append('        ' + after_name + '=("' + after_col + '", "count"),')
            else:
                raise ValueError("Operation: " + str(after_operation) + " not recognised!")
            
        # Add closing bracket
        instructions.append("    )")
        
        # Apply post_filters
        if hasattr(self, "filter"):
            # check codeCompHelper.bracket_replace
            # df_group_1 = df_group_1[df_group_1.sum_quantity > 300]
            for name, type, amount in filter_steps:
                # Check if used by bracket replaces
                if hasattr(codeCompHelper, "bracket_replace"):
                    if codeCompHelper.bracket_replace.get(name, None) != None:
                        name = codeCompHelper.bracket_replace.get(name)
                    
                instructions.append(this_df + " = " + this_df + "[" + this_df + "." + str(name) + " " + str(type) + " " + str(amount) + "]")
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        if output_cols != []:
            statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
            instructions.append(statement2_string)
        
        return instructions   
    
class merge_node():
    def __init__(self, condition, output, join, filters=None):
        self.condition = condition
        self.output = output
        if filters != None:
            self.filter = clean_filter_params(self, filters)
        else:
            self.filter = None
        self.join_type = join

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
        if self.join_type == "Semi":
            # Add support for a semi join
            # df_merge_1 =  df_filter_1[df_filter_1.o_orderkey.isin(df_filter_2["l_orderkey"])]
            statement = left_prev_df + '[' + left_prev_df + '.' + left_cond + '.isin(' + right_prev_df + '["' + right_cond + '"])]'
        else:
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
        instructions = []
        
        instructions.append(this_df + " = " + self.process_condition_into_merge(prev_dfs[0], prev_dfs[1]))
        
        # After merge, we filter if we have some
        if self.filter != None:
            # We need to replace any relation name with this_df, use codeComp to know these
            for relation in codeCompHelper.relations:
                self.filter = self.filter.replace(relation, this_df)
            statement = this_df + " = " + this_df + "[" + str(self.filter) + "]"
            instructions.append(statement)
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
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
        instructions = [this_df + " = pd.DataFrame()"]
        
        instructions += do_aggregation(self, prev_df, this_df)
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
        instructions.append(statement2_string)
            
        return instructions
    
class rename_node():
    def __init__(self, output, alias):    
        self.output = output
        self.alias = alias
    
    def set_nodes(self, nodes):
        self.nodes = nodes

    def to_pandas(self, prev_df, this_df, codeCompHelper):
        # Add the relation we are renaming to
        codeCompHelper.add_relation(self.alias)
        
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        instructions = []
        
        # Reset indexes to normal columns so we can reference them
        instructions.append(prev_df + " = " + prev_df + ".rename_axis(" + str(codeCompHelper.indexes) + ").reset_index()")
        
        # Create the current dataframe
        instructions.append(this_df + " = pd.DataFrame()")
        
        # Choose output columns        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Get the previous output columns
        prev_output = self.nodes[0].output
        # Reverse these
        prev_output.reverse() 
        
        # Create the rename dataframe, set previous columns to current column
        for i, output in enumerate(output_cols):
            statement_string = this_df + "['" + str(output) + "'] = " + prev_df + "['" + str(prev_output[i]) + "']"
            instructions.append(statement_string)
        
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
                        # Like replacement
                        if "like" in projection_original:
                            projection_original = projection_original.replace("like", "~~")
                        
                        # Get rid of end
                        if "where" and "then" and "else" and "end" in projection_original:
                            projection_original = projection_original.replace("end ", "")
                            
                        # Get rid of apostrophes
                        if "'" in projection_original:
                            projection_original = projection_original.replace("'", "")
                            
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
    elif node_type == "Incremental Sort":
        if set(current_node.presorted_key).issubset(current_node.sort_key):
            node_class = sort_node(current_node.output, current_node.sort_key)
        else:
            raise ValueError("Unexpected Values from Incremental Sort Node.")
    elif node_type == "Group Aggregate":
        node_class = group_aggr_node(current_node.output, current_node.group_key)
        if hasattr(current_node, "filter"):
            node_class.add_filter(current_node.filter)
    elif node_type == "Hash Join":
        node_class = merge_node(current_node.hash_cond, current_node.output, join=current_node.join_type)
    elif node_type == "Merge Join":
        node_class = merge_node(current_node.merge_cond, current_node.output, join=current_node.join_type)
    elif node_type == "Nested Loop":
        # Make a nested loop into a merge node
        if hasattr(current_node, "merge_cond"):
            if hasattr(current_node, "filter"):
                node_class = merge_node(current_node.merge_cond, current_node.output, join=current_node.join_type, filters=current_node.filter)
            else:
                node_class = merge_node(current_node.merge_cond, current_node.output, join=current_node.join_type)
        else:
            raise ValueError("We need our nested loop to have a merge condition, this should have been added by traversal")
    elif node_type == "Index Scan":            
        # Make an index scan into a filter node
        if hasattr(current_node, "filter"):
            node_class = filter_node(current_node.relation_name, current_node.filter, current_node.output)
        else:
            node_class = filter_node(current_node.relation_name, None, current_node.output)
    elif node_type == "Subquery Scan":
        # Make a Subquery Scan into a "rename node"
        node_class = rename_node(current_node.output, current_node.alias)
    else:
        raise ValueError("The node: " + str(current_node.node_type) + " is not recognised. Not all node have been implemented")
    
    if current_node.plans != None:
        current_node_plans = []
        for individual_plan in current_node.plans:
            current_node_plans.append(create_tree(individual_plan, sql_class))
        node_class.set_nodes(current_node_plans)
        
    return node_class
