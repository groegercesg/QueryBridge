# Date conversion
import pandas as pd

# SQL Parsing
from sqlglot import parse_one, exp

# Clean up redundant brackets
import regex
import re

# Expression Parsing
from expr_tree import Expression_Solver

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
        # Create a variable for cleaned output but without relations
        relation_cleaned_output = cleaned_output
        replaces = []
        if hasattr(codecomphelper, "relations"):
            for relation in codecomphelper.relations:
                if relation in brack_cleaned_output:
                    replaces.append(relation+".")
                    brack_cleaned_output = brack_cleaned_output.replace(relation+".", "")
                    relation_cleaned_output = relation_cleaned_output.replace(relation+".", "")
                    # Change output[i], this is for the case where we have a relation in the output
                    # But we don't have a column reference
                    #output[i] = brack_cleaned_output
        # Make lowercase as well
        # Do section that uses the codeCompHelper bracket_replace
        is_complex, brack_replace_output = complex_name_solve(brack_cleaned_output) 
        if hasattr(codecomphelper, "bracket_replace"):
            for x in list(codecomphelper.bracket_replace):
                if x in relation_cleaned_output:
                    if x == relation_cleaned_output:
                        # This is the same, already in the dictionary
                        # Don't add it again, we'll get it later
                        pass
                    else:
                        # Relation is not already represented in the dictionary
                        # We have a bracket_replace that is subset within our output_value
                        # We need to extract the parts that are not substring
                        # Add a new bracket replace and solve parts
                        
                        left_over_parts = relation_cleaned_output.split(x)
                        solved_parts = []
                        for j in range(len(left_over_parts)):
                            is_comp, new_elem = complex_name_solve(left_over_parts[j])
                            solved_parts.append(new_elem)
                    
                        # Join in back together
                        new_key = left_over_parts[0] + x + left_over_parts[1]
                        
                        # Check these are equal, for sanity
                        if new_key != relation_cleaned_output:
                            raise ValueError("Something went wrong: " + str(new_key) + " versus " + str(relation_cleaned_output))
                        
                        new_value = solved_parts[0] + codecomphelper.bracket_replace[x] + solved_parts[1]
                        
                        # Add to dictionary
                        codecomphelper.bracket_replace[new_key] = new_value
        
        brack_cleaned_lower_output = brack_cleaned_output.lower()
        if brack_cleaned_output in codecomphelper.sql.column_references:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, codecomphelper.sql.column_references[brack_cleaned_output])
        elif brack_cleaned_lower_output in codecomphelper.sql.column_references:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, codecomphelper.sql.column_references[brack_cleaned_lower_output])
        elif relation_cleaned_output in codecomphelper.bracket_replace:
            output_original_value = cleaned_output
            output[i] = (output_original_value, codecomphelper.bracket_replace[relation_cleaned_output])
        else:
            output[i] = cleaned_output
        
        # Performance the replaces to the output, removing the relations
        # This is crucial
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
            # Bug: '(supplier.s_nationkey)::numeric < 27.3'
            # Sometimes the postgres planner refers to the relation as "numeric"
            # We obviously shouldn't try to cast this as an integer
            
            # If there are no digits in the string, then simply cast it as a string
            if sum(c.isdigit() for c in match_str) == 0:
                new_value = str(value)
            else:
                new_value = str(int(value))
        elif "text[]" in match_str:
            edit_value = value[1:-1]
            if edit_value.count(",") > 0:
                # There are commas in the string
                # We split on these
                edit_split = edit_value.split(",")
                for k in range(len(edit_split)):
                    # Add a quote to the start and end of every string
                    edit_split[k] = "\'" + edit_split[k] + "\'"
                
                # Join back up together with commas
                edit_value = ",".join(edit_split)
            else:
                edit_value = "\'" + edit_value + "\'"
            new_value = str("[" + edit_value + "]")
        elif "text" in match_str:
            new_value = str(value)
            if matchNum > 1:
                # If we have the second match, i.e. the first is the relation
                # Then on the second we can add in quotes
                new_value = "\'" + str(value) + "\'"
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
        # " (part.p_size = ANY ('{49,14,23,45,19,3,36,9}'::integer[]))"
        elif "integer[]" in match_str:
            edit_value = value[1:-1]
            new_value = str("[" + edit_value + "]")
        elif "bpchar" in match_str:
            new_value = str("'" + value + "'")
            # replace single equals with double equals
            # Use the existing replace method to do this
            replaces.append(("=", "=="))
        elif "integer" in match_str:
            new_value = str(int(value))
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

from difflib import SequenceMatcher

def similar_string(a, b):
    return SequenceMatcher(None, a, b).ratio()

def clean_subplan_params(self, in_filters, prev_df, this_df):
    # Check this node is subplan appropriate
    if hasattr(self, "nodes"):
        if len(self.nodes) >= 1:
            if hasattr(self.nodes[0], "subplan_name"):
                pass
            else:
                raise ValueError("Incorrect subplan formulation")
        else:
            raise ValueError("Incorrect subplan formulation")
    else:
        raise ValueError("Incorrect subplan formulation")
    
    # Split on & and |, keep in original split
    line_split = re.split('([&|])', in_filters)
    for i in range(len(line_split)):
        if "NOT " in line_split[i]:
            line_split[i] = line_split[i].replace("NOT ", "")
            inner = clean_extra_brackets(line_split[i])
            inner_split = inner.split("SubPlan ")
            
            if len(inner_split) != 2:
                raise ValueError("Unexpected formulation of inner_split: " + str(inner_split))
            
            subplan_name = "SubPlan " + inner_split[1]
            
            if subplan_name != self.nodes[0].subplan_name:
                raise ValueError("The SubPlan names are different, desired name: " + str(subplan_name) + " but the detected SubPlan had name: " + str(self.nodes[0].subplan_name))
            
            # Decide which column to do the equivalance for
            if len(self.output) == 1:
                chosen_column = self.output[0]
            else:
                # Choose which column to use
                best_similarity = 0.0
                best_column = None
                
                # Iterate through columns
                for j in range(len(self.output)):
                    # Calculate the score for a string
                    similar_score = similar_string(self.output[j], self.nodes[0].output[0])
                    # If better than the current best score
                    if similar_score > best_similarity:
                        # Set as new score
                        best_similarity = similar_score
                        # And update best column
                        best_column = self.output[j]
                        
                chosen_column = best_column
            
            line_split[i] = this_df + '[~' + this_df + '.' + chosen_column + '.isin(' + prev_df + '["' + self.nodes[0].output[0] + '"])]'
            
        else:
            raise ValueError("Not implemented")
        
        
    # Reassemble line_split
    # Join on nothing, should have spaces still in it
    filters = "".join(line_split)
    
    return filters

_special_regex_chars = {
    ch : '\\'+ch
    for ch in '.^$*+?{}[]|()\\'
}

def sql_like_fragment_to_regex_string(fragment):
    safe_fragment = ''.join([
        _special_regex_chars.get(ch, ch)
        for ch in fragment
    ])
    return '^' + safe_fragment.replace('%', '.*?').replace('_', '.') + '$'
    

def clean_filter_params(self, params):  
    # Replace AND with & and convert to string
    filters = str(params.replace(" AND ", " & "))
    filters = str(filters.replace(" OR ", " | "))
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
                
            # Do replacements for all
            if " <> ALL " in line_split[i]:
                line_split[i] = line_split[i].replace(" <> ALL ", ".isin")
                line_split[i] = "~" + line_split[i]
                
            # Handle not equals case
            if " <> " in line_split[i]:
                line_split[i] = line_split[i].replace("<>", "!=")
                
            # The presence of "~~", be it as part of "!~~" (NOT LIKE) or "~~" (LIKE)
            # Means that we have to convert this to a like statement
            # We can do this generally using a regex, this catches all cases 100% of the time
            if "~~" in line_split[i]:
                # Variable for what type of like we have
                not_equal_like = False
                
                # Strip and clean
                line_split[i] = line_split[i].strip()
                if (line_split[i][0] == "(") and (line_split[i][-1] == ")"):
                    line_split[i] = clean_extra_brackets(line_split[i])
                    
                # Not equal
                if " !~~ " in line_split[i]:
                    not_equal_like = True
                    split_like = line_split[i].split(" !~~ ")
                else:
                    split_like = line_split[i].split(" ~~ ") 
                
                # Catch potential error
                if len(split_like) != 2:
                    raise ValueError("Expected only 2 parts to this statement")
                
                # Clear quotes if exist
                if (split_like[1][0] == "'") and (split_like[1][-1] == "'"):
                    split_like[1] = split_like[1][1:-1]
                    
                data_name = split_like[0].strip()
                regex_cmd = sql_like_fragment_to_regex_string(split_like[1])
                
                # Assemble the line
                line_split[i] = '(' + data_name + '.str.contains("' + str(regex_cmd) +'", regex=True)'
                
                if not_equal_like == True:
                    line_split[i] = line_split[i] + " == False)"
                else:
                    # Otherwise, just close the bracket
                    line_split[i] = line_split[i] + ")"
                    
            # Do equals
            if " = " in line_split[i]:
                line_split[i] = line_split[i].replace(" = ", " == ")
              
        # Clear leading/trailing spaces
        line_split[i] = line_split[i].strip()
        
        if (line_split[i] == "&") or (line_split[i] == "|"):
            line_split[i] = " " + line_split[i] + " "
    
    # Reassemble line_split
    # Join on nothing, should have spaces still in it
    filters = "".join(line_split)
    
    return filters

# Classes for pandas instructions
class filter_node():
    def __init__(self, data, output):
        self.data = data
        self.output = output
        self.params = None
        
    def set_params(self, in_params):
        self.params = clean_filter_params(self, in_params)
        
    def set_nodes(self, nodes):
        self.nodes = nodes
        
    def add_subplan_name(self, in_name):
        self.subplan_name = in_name
    
    def set_instructions(self, instructions):
        self.instructions = instructions
        
    def set_index_cond(self, index_cond):
        self.index_cond = clean_filter_params(self, index_cond)
        
    def to_pandas(self, prev_df, this_df, codeCompHelper, treeHelper):
        subplan_mode = False

        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        
        # Check if "SubPlan"
        if self.params != None:
            if "SubPlan" in self.params:
                subplan_mode = True
                self.params = clean_subplan_params(self, self.params, prev_df, self.data)
        
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        instructions = []
        
        # Edit params:
        if self.params != None:
            if subplan_mode == True:
                # Do output for subplan mode
                instructions.append(this_df + " = " + self.params)
                
                output_cols = choose_aliases(self, codeCompHelper)
                
                # Limit to output columns
                if codeCompHelper.column_limiting:
                    statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
                    instructions.append(statement2_string)
            
            else:
                params = self.params.replace(self.data, prev_df)
                statement1_string = this_df + " = " + prev_df + "[" + str(params) + "]"
                instructions.append(statement1_string)
                
                if hasattr(self, "index_cond"):
                    instructions.append(self.index_cond)
            
                output_cols = choose_aliases(self, codeCompHelper)
                
                # Limit to output columns
                if codeCompHelper.column_limiting:
                    statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
                    instructions.append(statement2_string)
        else:            
            output_cols = choose_aliases(self, codeCompHelper)
            
            
            
            # Limit to output columns
            if codeCompHelper.column_limiting:
                statement2_string = this_df + " = " + prev_df + "[" + str(output_cols) + "]"
                instructions.append(statement2_string)
            else:
                statement2_string = this_df + " = " + prev_df
                instructions.append(statement2_string)
                
            if hasattr(self, "index_cond"):
                instructions.append(this_df + " = " + this_df + "['" + str(self.index_cond) + "']")
            
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
                # Use process_output to      the relation if present
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
    
    def to_pandas(self, prev_df, this_df, codeCompHelper, treeHelper):
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
        if codeCompHelper.column_limiting:
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
    
    def to_pandas(self, prev_df, this_df, codeCompHelper, treeHelper):
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
            if codeCompHelper.column_limiting:
                output_cols = choose_aliases(self, codeCompHelper, final_output=False)
                statement_string = this_df + " = " + prev_df + "[" + str(output_cols) + "]"
                instructions.append(statement_string)
            else:
                statement_string = this_df + " = " + prev_df
                instructions.append(statement_string)
                    
        statement3_string = str(this_df) + " = " + str(this_df) + ".head("+str(self.amount)+")"
        instructions.append(statement3_string)
        
        return instructions

class unique_node():
    def __init__(self, output):
        self.output = output
        
    def set_nodes(self, nodes):
        self.nodes = nodes
    
    def to_pandas(self, prev_df, this_df, codeCompHelper, treeHelper):
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        instructions = []
            
        # Reset indexes to normal columns so we can reference them
        if codeCompHelper.indexes != []:
            instructions.append(prev_df + " = " + prev_df + ".rename_axis(" + str(codeCompHelper.indexes) + ").reset_index()")
        
        # Create the current dataframe
        instructions.append(this_df + " = pd.DataFrame()")
        
        # Choose output columns        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Create the rename dataframe, set previous columns to current column
        for i, output in enumerate(output_cols):
            statement_string = this_df + "['" + str(output) + "'] = " + prev_df + "['" + str(output) + "'].unique()"
            instructions.append(statement_string)
            
        # It appears that unique also needs to sort the columns
        ascendings = [True] * len(output_cols)
        statement1_string = this_df + " = " + this_df + ".sort_values(by=" + str(output_cols) + ", ascending=" + str(ascendings) + ")"
        instructions.append(statement1_string)
        
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

def do_aggregation(self, prev_df, current_df, codeCompHelper, treeHelper):
    local_instructions = []
    for col in self.output:
        
        if isinstance(col, tuple):
            # Where we have an alias
            # Set output_name
            output_name = None
            if not treeHelper.bench:
                output_name = str(treeHelper.expr_output_path)+str(treeHelper.expr_tree_tracker)
            else:
                output_name = False
            
            tree = Expression_Solver(str(col[0]), output_name, prev_df)
            pandas = tree.evaluate()
            code_line = str(current_df) + "['" + str(col[1]) + "'] = [" + str(pandas) + "]"
            local_instructions.append(code_line)
            
            # Increment treeHelper
            treeHelper.expr_tree_tracker += 1
        else:
            # Where we have don't have an alias, use bracket replace
            
            # Set output_name
            output_name = None
            if not treeHelper.bench:
                output_name = str(treeHelper.expr_output_path)+str(treeHelper.expr_tree_tracker)
            else:
                # Means no visualisation will be created
                output_name = False
            
            tree = Expression_Solver(str(col), output_name, prev_df)
            pandas = tree.evaluate()
            
            # Handle complex names in output
            is_complex, new_name = complex_name_solve(col)
            if is_complex == True:
                # Replace these
                codeCompHelper.add_bracket_replace(col, new_name)
            
            code_line = str(current_df) + "['" + str(new_name) + "'] = [" + str(pandas) + "]"
            local_instructions.append(code_line)
            
            # Increment treeHelper
            treeHelper.expr_tree_tracker += 1
        
        """
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
            
            elif "max" in col[0]:
                # max(sum(l_extendedprice * (1 - l_discount)))
                
                 
                # The aggr operation is MAX!
                inner = str(col[0]).split("max")[1]
                inner = clean_extra_brackets(inner)
                
                if "sum" in inner:
                    # Determine if inner is in the codeCompHelper.bracket_replace
                    if hasattr(codeCompHelper, "bracket_replace"):
                        if inner in codeCompHelper.bracket_replace:
                            skip = True
                            inner = codeCompHelper.bracket_replace[inner]
                    
                    if not skip:
                        # The aggr operation is SUM!
                        inner_inner = str(inner).split("sum")[1]
                        inner_inner = clean_extra_brackets(inner_inner)
                        
                        inner_inner_string = aggregate_sum(inner_inner, df_group=prev_df)
                    
                        inner = "(" + inner_inner_string + ").sum()"
                    
                    # Reset skip for safety
                    skip = False
                    
                outer_string = "(" + prev_df + "." + inner + ").max()"
                
                local_instructions.append(current_df + "['" + col[1] + "'] = [" + outer_string + "]")
                
            elif "min" in col[0]:
                # The aggr operation is MIN!
                inner = str(col[0]).split("min")[1]
                inner = clean_extra_brackets(inner)
                
                outer_string = "(" + prev_df + "." + inner + ").min()"
                
                local_instructions.append(current_df + "['" + col[1] + "'] = [" + outer_string + "]")
            
            
            elif "sum" in col[0]:
                # The aggr operation is SUM!
                inner = str(col[0]).split("sum")[1]
                inner = clean_extra_brackets(inner)
                
                inner_string = aggregate_sum(inner, df_group=prev_df)
              
                outer_string = "(" + inner_string + ").sum()"
                
                local_instructions.append(current_df + "['" + col[1] + "'] = [" + outer_string + "]")
                
            elif "count" in col[0]:
                # The aggr operation is COUNT!
                inner = str(col[0]).split("count")[1]
                inner = clean_extra_brackets(inner)
                
                outer_string = "(" + prev_df + "." + inner + ").count()"
                
                local_instructions.append(current_df + "['" + col[1] + "'] = [" + outer_string + "]")
                
            elif "avg" in col[0]:
                # The aggr operation is MIN!
                inner = str(col[0]).split("avg")[1]
                inner = clean_extra_brackets(inner)
                
                outer_string = "(" + prev_df + "." + inner + ").mean()"
                
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
                skip = False
                # max(sum(l_extendedprice * (1 - l_discount)))
                 
                # The aggr operation is MAX!
                inner = str(col).split("max")[1]
                inner = clean_extra_brackets(inner)
                
                if "sum" in inner.lower():
                    # The aggr operation is SUM!
                    inner_inner = str(inner).split("sum")[1]
                    inner_inner = clean_extra_brackets(inner_inner)
                    
                    inner_inner_string = aggregate_sum(inner_inner, df_group=prev_df)
                
                    inner = "(" + prev_df + "." + inner_inner_string + ").sum()"
                    skip = True
                    
                if skip == True:                    
                    outer_string = "(" + inner + ").max()"
                else:
                    outer_string = "(" + prev_df + "." + inner + ").max()"
                    
                # Handle complex names in output
                is_complex, new_name = complex_name_solve(col)
                if is_complex == True:
                    # Replace these
                    codeCompHelper.add_bracket_replace(col, new_name)
                
                local_instructions.append(current_df + "['" + new_name + "'] = [" + outer_string + "]")
            elif "min" in col:
                # The aggr operation is MIN!
                inner = str(col).split("min")[1]
                inner = clean_extra_brackets(inner)
                
                outer_string = "(" + prev_df + "." + inner + ").min()"
                
                # Handle complex names in output
                is_complex, new_name = complex_name_solve(col)
                if is_complex == True:
                    # Replace these
                    codeCompHelper.add_bracket_replace(col, new_name)
                
                local_instructions.append(current_df + "['" + new_name + "'] = [" + outer_string + "]")
                
            elif "avg" in col:
                # The aggr operation is MIN!
                inner = str(col).split("avg")[1]
                inner = clean_extra_brackets(inner)
                
                outer_string = "(" + prev_df + "." + inner + ").mean()"
                
                # Handle complex names in output
                is_complex, new_name = complex_name_solve(col)
                if is_complex == True:
                    # Replace these
                    codeCompHelper.add_bracket_replace(col, new_name)
                
                local_instructions.append(current_df + "['" + new_name + "'] = [" + outer_string + "]")
            
            elif "count" in col:
                # The aggr operation is MAX!
                inner = str(col).split("count")[1]
                inner = clean_extra_brackets(inner)
                
                if "distinct" in inner.lower():
                    # The aggr operation is distinct!
                    inner_inner = str(inner).split("DISTINCT")[1]
                    inner_inner = clean_extra_brackets(inner_inner).strip()
                
                    outer_string = "(" + prev_df + "['" + inner_inner + "']).nunique()"
                    
                    # Handle complex names in output
                    is_complex, new_name = complex_name_solve(col)
                    if is_complex == True:
                        # Replace these
                        codeCompHelper.add_bracket_replace(col, new_name)
                        
                    local_instructions.append(current_df + "['" + new_name + "'] = [" + outer_string + "]")
                    
                else:
                
                    outer_string = "(" + prev_df + "." + inner + ").count()"
                    
                    # Handle complex names in output
                    is_complex, new_name = complex_name_solve(col)
                    if is_complex == True:
                        # Replace these
                        codeCompHelper.add_bracket_replace(col, new_name)
                    
                    local_instructions.append(current_df + "['" + new_name + "'] = [" + outer_string + "]")
                
            elif "sum" in col:
                # The aggr operation is SUM!
                inner = str(col).split("sum")[1]
                inner = clean_extra_brackets(inner)
                
                inner_string = aggregate_sum(inner, df_group=prev_df)
              
                outer_string = "(" + inner_string + ").sum()"
                
                # Handle complex names in output
                is_complex, new_name = complex_name_solve(col)
                if is_complex == True:
                    # Replace these
                    codeCompHelper.add_bracket_replace(col, new_name)
                
                local_instructions.append(current_df + "['" + new_name + "'] = [" + outer_string + "]")
                
                
            else:
                raise ValueError("Not Implemented Error, for col: " + str(col))
        """
        

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
                if "distinct" in col[0].lower():
                    # Set Aggr type as Distinct
                    aggr_type = "count_distinct"
                    
                    inner = clean_extra_brackets(inner)
                    inner = str(inner).split("DISTINCT ")[1]
                    inner = clean_extra_brackets(inner)
                    
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
                
                cleaned_inner, count = check_aggregate(inner, df_group=prev_df)
                # If count > 1
                    # We need to do the inner part before, add to a before list
                    # Save the after part in after list
                if count > 1:
                    # Add to before_aggrs
                    before_aggrs.append([inner, cleaned_inner])
                    # Add to after_aggrs
                    after_aggrs.append([col, inner, aggr_type])
                # If count <= 1
                    # Save in after part list
                else:
                    # We don't use cleaned_inner, it's junk.
                    # We just use the inner originally
                    # Add to after_aggrs
                    after_aggrs.append([col, inner, aggr_type])
                
            elif "avg" in col:
                raise ValueError("AVG with no alias!")
            elif "count" in col:
                inner = str(col).split("count")[1]
                inner = clean_extra_brackets(inner)
                if "DISTINCT" in inner:
                    aggr_type = "distinct"
                    inner = str(inner).split("DISTINCT ")[1]
                    inner = clean_extra_brackets(inner)
                    cleaned_inner, count = check_aggregate(inner, df_group=prev_df)
                    
                    after_aggrs.append([col, inner, aggr_type])
                else:
                    raise ValueError("COUNT with no alias!")
            else:
                # If these are columns with no aggregations
                # That are not indexes
                # In that case we should be grouping by them
                self.group_key.append(col)
     
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
    
    def to_pandas(self, prev_df, this_df, codeCompHelper, treeHelper):
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
                filter_type = "=="
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
            # Only append if not already present
            for b_filt in before_filter:
                if b_filt not in before_group:
                    before_group.append(b_filt)
            
            # Only append if not already present
            for a_filt in after_filter:
                if a_filt not in after_group:
                    after_group.append(a_filt)
        
        # Handle before
        for before_name, before_command in before_group:
            # Handle complex names in output
            is_complex, new_name = complex_name_solve(before_name)
            if is_complex == True:
                # Replace these
                codeCompHelper.add_bracket_replace(before_name, new_name)
                # Find the before_name in after_group
                # If before_name == after_group[i][1], the after_col, we can replace with new_name.
                for i in range(len(after_group)):
                    if before_name == after_group[i][1]:
                        after_group[i][1] = new_name
                
                # Set to after_name for use now
                before_name = new_name
                
            
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
            is_complex, new_name = complex_name_solve(after_name)
            if is_complex == True:
                # Replace these
                codeCompHelper.add_bracket_replace(after_name, new_name)
                # Set to after_name for use now
                after_name = new_name
            
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
            elif after_operation == "count_distinct":
                instructions.append('        ' + after_name + '=("' + after_col + '", lambda x: x.nunique()),')
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
        if output_cols != [] and codeCompHelper.column_limiting:
            statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
            instructions.append(statement2_string)
        
        return instructions   
    
def complex_name_solve(in_name):
    complex_items = ["*", "-", "(", ")", " ", "/", "+", "-", "*"]
    is_complex = False
    new_name = None
    
    if any(item in in_name for item in complex_items):
        # We have one of these items in our string
        is_complex = True
        new_name = in_name
        for item in complex_items:
            new_name = new_name.replace(item, "")
    
    # returns
    #   is_complex  -  True or False as to whether complex
    #   new_name  -  The name that we have after this function has done it's magic
    return is_complex, new_name
    
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

    def to_pandas(self, prev_dfs, this_df, codeCompHelper, treeHelper):
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
                self.filter = self.filter.replace(relation+".", this_df+".")
            statement = this_df + " = " + this_df + "[" + str(self.filter) + "]"
            instructions.append(statement)
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        if codeCompHelper.column_limiting:
            statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
            instructions.append(statement2_string)
            
        return instructions

class aggr_node():
    def __init__(self, output):    
        self.output = output

    def set_nodes(self, nodes):
        self.nodes = nodes

    def to_pandas(self, prev_df, this_df, codeCompHelper, treeHelper):
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        instructions = [this_df + " = pd.DataFrame()"]
        
        instructions += do_aggregation(self, prev_df, this_df, codeCompHelper, treeHelper)
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        if codeCompHelper.column_limiting:
            statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
            instructions.append(statement2_string)
            
        return instructions
    
class rename_node():
    def __init__(self, output, alias):    
        self.output = output
        self.alias = alias
    
    def set_nodes(self, nodes):
        self.nodes = nodes

    def to_pandas(self, prev_df, this_df, codeCompHelper, treeHelper):
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
        # Iterate through these, replace with entries from codeCompHelper.bracket_replace if present
        for i in range(len(prev_output)):
            if prev_output[i] in codeCompHelper.bracket_replace:
                # This is in it, so set to the replaced version
                prev_output[i] = codeCompHelper.bracket_replace[prev_output[i]]
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
        local_file = self.file_content.lower()
        if "limit" in local_file.lower():
            # limit_amount = 0
            limit_amount = local_file.split("limit")[1].split(";")[0].strip()
            # for limit in parse_one(self.file_content).find_all(exp.Limit):
            #     limit_amount = int(limit.expression.alias_or_name)
        else:
            limit_amount = None
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
        node_class = filter_node(current_node.relation_name, current_node.output)
    
        if hasattr(current_node, "filters"):
            # Check if is a filter type of Seq Scan
            node_class.set_params(current_node.filters)
        
        # Add in subplan information
        if hasattr(current_node, "subplan_name"):
            node_class.add_subplan_name(current_node.subplan_name)
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
        node_class = filter_node(current_node.relation_name, current_node.output)
            
        if hasattr(current_node, "filter"):
            # Check if is a filter type of Seq Scan
            node_class.set_params(current_node.filter)
        elif hasattr(current_node, "filters"):
            # Check if is a filter type of Seq Scan
            node_class.set_params(current_node.filters)
            
        if hasattr(current_node, "index_cond"):
            if current_node.index_cond != None:
                node_class.set_index_cond(current_node.index_cond)
    elif node_type == "Subquery Scan":
        # Make a Subquery Scan into a "rename node"
        node_class = rename_node(current_node.output, current_node.alias)
    elif node_type == "Index Only Scan":
        # Make an index scan into a filter node
        node_class = filter_node(current_node.relation_name, current_node.output)
            
        if hasattr(current_node, "filters"):
            # Check if is a filter type of Seq Scan
            node_class.set_params(current_node.filters)
    elif node_type == "Bitmap Heap Scan":
        node_class = filter_node(current_node.relation_name, current_node.recheck_cond, current_node.output)
    elif node_type == "Unique":
        node_class = unique_node(current_node.output)
    else:
        raise ValueError("The node: " + str(current_node.node_type) + " is not recognised. Not all node have been implemented")
    
    if current_node.plans != None:
        current_node_plans = []
        for individual_plan in current_node.plans:
            current_node_plans.append(create_tree(individual_plan, sql_class))
        node_class.set_nodes(current_node_plans)
        
    return node_class
