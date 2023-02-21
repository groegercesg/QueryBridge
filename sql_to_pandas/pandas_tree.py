# Date conversion
import pandas as pd

# SQL Parsing
from sqlglot import parse_one, exp

# Clean up redundant brackets
import regex
import re

# String parsing
from string import digits

# Expression Parsing
from expr_tree import Expression_Solver

from collections import defaultdict

def rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)

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
                        
                        if solved_parts[1] == None:
                            new_value = solved_parts[0] + codecomphelper.bracket_replace[x]
                        else:
                            new_value = solved_parts[0] + codecomphelper.bracket_replace[x] + solved_parts[1]
                        
                        # Add to dictionary
                        codecomphelper.bracket_replace[new_key] = new_value
        
        brack_cleaned_lower_output = brack_cleaned_output.lower()
        # A brack cleaned option that removes multiple equals into one, and get's rid of quotes and dots
        brack_cleaned_equal_quote_lower_output = brack_cleaned_lower_output.replace(" == ", " = ").replace("'", "")
        brack_cleaned_equal_quote_lower_output_no_end = rreplace(rreplace(brack_cleaned_equal_quote_lower_output, "end", " ", 1).strip(), " else", "", 1).strip()
        brack_cleaned_equal_quote_lower_output_no_end_spaced = rreplace(rreplace(brack_cleaned_equal_quote_lower_output, " end ", "  ", 1).strip(), " else", "", 1).strip()
        brack_cleaned_equal_quote_lower_output_no_end_spaced_like = brack_cleaned_equal_quote_lower_output_no_end_spaced.replace(" like ", " ~~ ")
        brack_cleaned_equal_quote_lower_output_dots = brack_cleaned_equal_quote_lower_output.replace("." , "")
        brack_cleaned_equal_quote_lower_output_dots_no_end = rreplace(rreplace(brack_cleaned_equal_quote_lower_output_dots, "end", " ", 1).strip(), " else", "", 1).strip()
        dot_cleaned_output = cleaned_output.replace(".", "").strip()
        substring_format = brack_cleaned_lower_output.replace("from ", "").replace("for ", "").strip()

        # Make a copy of column_references, with the replaces(minus final .) made
        # As well as the ones before
        col_ref_complete = dict(codecomphelper.sql.column_references)
        
        # New, old
        new_keys = []
        for key in col_ref_complete.keys():
            # Iterate through replaces
            for repl in replaces:
                if repl[:-1] in key:
                    new_key = key.replace(repl[:-1], "")
                    new_keys.append([new_key, key])
                    
        for new, old in new_keys:
            col_ref_complete[new] = col_ref_complete[old]

        if dot_cleaned_output in col_ref_complete:
            output_original_value = cleaned_output
            # Make a special 3 element tuple for this situation
            if codecomphelper.useAlias.get(dot_cleaned_output, None) != None:
                output[i] = (codecomphelper.useAlias.get(dot_cleaned_output, None), col_ref_complete[dot_cleaned_output], dot_cleaned_output)
            else:
                output[i] = (output_original_value, col_ref_complete[dot_cleaned_output], dot_cleaned_output)        
        elif brack_cleaned_output in col_ref_complete:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, col_ref_complete[brack_cleaned_output])
        elif brack_cleaned_lower_output in col_ref_complete:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, col_ref_complete[brack_cleaned_lower_output])
        elif brack_cleaned_equal_quote_lower_output in col_ref_complete:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, col_ref_complete[brack_cleaned_equal_quote_lower_output])
        elif brack_cleaned_equal_quote_lower_output_dots in col_ref_complete:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, col_ref_complete[brack_cleaned_equal_quote_lower_output_dots])
        elif brack_cleaned_equal_quote_lower_output_dots_no_end in col_ref_complete:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, col_ref_complete[brack_cleaned_equal_quote_lower_output_dots_no_end])
        elif brack_cleaned_equal_quote_lower_output_no_end_spaced_like in col_ref_complete:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, col_ref_complete[brack_cleaned_equal_quote_lower_output_no_end_spaced_like])
        elif brack_cleaned_equal_quote_lower_output_no_end in col_ref_complete:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, col_ref_complete[brack_cleaned_equal_quote_lower_output_no_end])
        elif brack_cleaned_equal_quote_lower_output_no_end_spaced in col_ref_complete:
            # We have an item in output that needs to be changed
            output_original_value = cleaned_output
            output[i] = (output_original_value, col_ref_complete[brack_cleaned_equal_quote_lower_output_no_end_spaced])
        elif relation_cleaned_output in codecomphelper.bracket_replace:
            output_original_value = cleaned_output
            output[i] = (output_original_value, codecomphelper.bracket_replace[relation_cleaned_output])
        elif substring_format in col_ref_complete:
            output_original_value = cleaned_output
            output[i] = (output_original_value, col_ref_complete[substring_format])
        else:
            if "distinct" in cleaned_output.lower():
                # Function to delete at particular index
                def del_at_index(strObj, index):
                    # Slice string to remove character at index 5
                    if len(strObj) > index:
                        strObj = strObj[0 : index : ] + strObj[index + 1 : :]
                    return strObj

                def insert_at_index(strObj, index, insert):
                    return strObj[:index] + str(insert) + strObj[index:]
                                    
                
                # Handle a distinct in the output, we need to insert brackets
                distinct_output = cleaned_output.lower()
                # Find the position after the distinct
                distinct_loc = distinct_output.find("distinct") + len("distinct")
                
                # Handle not found case
                if distinct_loc == -1:
                    raise ValueError("Distinct not found in output, despite appearing to be there")
                
                # If there was a space in the thing then delete it
                if distinct_output[distinct_loc] == " ":
                    # Delete at particular index
                    distinct_output = del_at_index(distinct_output, distinct_loc)
                
                # Insert a open bracket at distinct_loc
                distinct_output = insert_at_index(distinct_output, distinct_loc, "(")

                # Determine where to insert the close bracket
                close_position = distinct_output.find(')', distinct_loc)
                # Insert a close bracket at close_position
                distinct_output = insert_at_index(distinct_output, close_position, ")")
                output[i] = distinct_output
            else:
                output[i] = cleaned_output
        
        # Perform the replaces to the output, removing the relations
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
        
        # Final interrupt to catch count(*)
        if isinstance(output[i], tuple):
            if "count(*)" in output[i][0].lower():
                # Pick a column we know is going to exist after the group by
                # This is therefore one that we want to output, we take the first
                """
                if hasattr(self, "group_key"):
                    this_group_key = str(self.group_key[0])
                    # Clean it of relations
                    for rel in codecomphelper.relations:
                        if rel + "." in this_group_key:
                            this_group_key = this_group_key.replace(rel+".", "")
                    
                    chosen_column = str(this_group_key)
                else:
                """
                if self.output == []:
                    # Hasattr sort_key
                    if hasattr(self, "sort_key"):
                        k = 0
                        while self.sort_key[k].split(" ")[0] == "count(*)":
                            k = k + 1
                        chosen_column = str(self.sort_key[k].split(" ")[0]).strip()
                    else:
                        raise Exception("Node has no output columns and no sort_key, what should we use to count on?")
                else:
                    chosen_column = str(self.output[0])

                output[i] = ("count(" + str(chosen_column) + ")", output[i][1])
            elif "extract" in output[i][0].lower():
                output[i] = (handle_extract(output[i][0]), output[i][1])
            elif "substring" in output[i][0].lower():
                output[i] = (handle_substring(output[i][0]), output[i][1])
        else:
            if "count(*)" in output[i].lower():
                # Pick a column we know is going to exist after the group by
                # This is therefore one that we want to output, we take the first
                chosen_column = str(self.group_key[0])
                
                output[i] = "count(" + str(chosen_column) + ")"
            elif "extract" in output[i].lower():
                output[i] = handle_extract(output[i])
            elif "substring" in output[i].lower():
                output[i] = handle_substring(output[i])
        
        # Catch if is in codeCompHelper.useAlias
        if isinstance(output[i], tuple):
            if codecomphelper.useAlias.get(output[i][0], None) != None:
                # Check that the alias is the same
                if output[i][1] != codecomphelper.useAlias.get(output[i][0], None):
                    raise ValueError("Aliases are unexpectedly different")
                # This key is in useAlias, so we should cut it down to a simple alias
                output[i] = codecomphelper.useAlias.get(output[i][0], None)
        else:
            if codecomphelper.useAlias.get(output[i], None) != None:
                # This key is in useAlias, so we should cut it down to a simple alias
                output[i] = codecomphelper.useAlias.get(output[i], None)
    
    return output

def handle_extract(string):
    # String: EXTRACT(year FROM o_orderdate)
    # Split into param and source
    if "EXTRACT" in string:
        param = str(string.split("EXTRACT(")[1].split(" FROM")[0]).strip().lower()
        source = str(string.split("FROM ")[1].split(")")[0]).strip().lower()
    elif "extract" in string:
        param = str(string.split("extract(")[1].split(" from")[0]).strip().lower()
        source = str(string.split("from ")[1].split(")")[0]).strip().lower()
    else:
        raise Exception("Couldn't determine EXTRACT/extract in an allegedly extract string")    
    
    return str(source) + ".dt." + str(param)

def handle_substring(string):
    # String: SUBSTRING(c_phone FROM 1 FOR 2)
    # Split into where, from and for
    
    # Check is actually a substring
    if "SUBSTRING" not in string:
        raise Exception("How did we get here!")
    
    where_value = str(str(string[10:]).split(" ")[0]).strip()
    from_value = int(str(str(string.split(" FROM ")[1]).split(" FOR ")[0]).strip())
    for_value = int(str(str(string.split(" FOR ")[1]).split(")")[0]).strip())
    
    return str(where_value) + ".str.slice(" + str(from_value - 1) + ", " + str((from_value - 1) + for_value) + ")"

def remove_range(sentence, matches):
    return "".join(
        [sentence[0:matches[i][0]] if i == 0 else 
         sentence[matches[i - 1][1]:matches[i][0]] if i != len(matches) else 
         sentence[matches[i - 1][1]::] for i in range(len(matches) + 1)
         ])
    
def do_replaces(sentence, replaces):
    # We have a list of replacements to make, this is in order
    # We make a dictionary of replaces and occurances, i.e. number of times to replace
    replace_dict = defaultdict(int)
    for replace in replaces:
        replace_dict[replace] += 1
    
    for key in replace_dict:
        sentence = sentence.replace(key[0], key[1], replace_dict[key])
        
    return sentence

def count_char(char, string):
    counter = 0
    for i in range(len(string)):
        if string[i] == char:
            counter += 1
            
    return counter
            
def clean_type_information(self, content):
    # Replace "timestamp without time zone", with just timestamp
    if "::timestamp without time zone" in content:
        content = content.replace("timestamp without time zone", "timestamp")
    
    regex = r"::(\w+\[*\]*)(\ \s+\w+)*"
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
                
        if value == "NULL":
            print("NULL Value detected!")
        
        # print("Old Value: " + str(value))
        match_str = str(str(match.group())[2:])
        if "date" in match_str:
            # Hesam suggestion:
            new_value = "'" + str(pd.to_datetime(value, format='%Y-%m-%d')) + "'"
            #new_value = "pd.Timestamp('"+str(pd.to_datetime(value, format='%Y-%m-%d'))+"')"
        elif "timestamp" in match_str:
            # Hesam suggestion:
            new_value = "'" + str(pd.to_datetime(value, format='%Y-%m-%d %H:%M:%S')) + "'"
            #new_value = "pd.Timestamp('"+str(pd.to_datetime(value, format='%Y-%m-%d'))+"')"
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
            # BUG: This replacement here, with the brackets, sometimes causes the string to be woefully unsuitable
            if value != new_value:
                replaces.append(("("+value+")", new_value))
        else:
            raise ValueError("Unrecognised value for valueFind: " + str(valueFind))
        
    if remove_ranges != [] or replaces != []:
        if remove_ranges != []:
            content = remove_range(content, remove_ranges)
        if replaces != []:
            content = do_replaces(content, replaces)
    
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
            inner_split = inner.split("SubPlan")
            
            if len(inner_split) != 2:
                raise ValueError("Unexpected formulation of inner_split: " + str(inner_split))
            
            subplan_name = str("SubPlan") + str(" ") + str(inner_split[1]).strip()
            
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
    

def clean_filter_params(self, params, codeCompHelper, prev_df):  
    
    # Clean params with codeCompHelper
    # Iterate through all relations
    for key in codeCompHelper.aliasRelationPairs:
        targetRelation = str(key + ".")
        if targetRelation in params:
            params = params.replace(targetRelation, str(codeCompHelper.aliasRelationPairs[key] + "."))
    
    # We try and squeeze in any bracket replaces we might have
    # Check if we have any bracket replaces
    # Create a relation removed version
    # If bracket replace is not empty
    if codeCompHelper.bracket_replace != {}:
        # Strip outer brackets if exist 
        bracket_replace_params = params
        if (bracket_replace_params[0] == "(") and (bracket_replace_params[-1] == ")"):
            bracket_replace_params = bracket_replace_params[1:-1]
        
        # Split into keys, on Top level brackets
        keys = [match.group() for match in regex.finditer(r"(?:(\((?>[^()]+|(?1))*\))|\S)+", bracket_replace_params)]
        
        # Populate replace_dict
        replace_dict = {}
        
        for key in keys:
            process_key = key
            # For each key, remove relations
            capture_relation = None
            for relation in codeCompHelper.relations:
                process_key = process_key.replace(relation + ".", "")
                capture_relation = relation + "."
                    
            # Remove extraneous brackets if needed
            while (process_key[0] == "(") and (process_key[-1] == ")"):
                process_key = process_key[1:-1]
            
            replaced = False
            # See is is in bracket replace
            for br_key in codeCompHelper.bracket_replace:
                if br_key == process_key:
                    process_key = codeCompHelper.bracket_replace[br_key]
                    replaced = True
                
            # If is add to replace dict    
            if replaced == True:
                # Add in relation
                if capture_relation == None:
                    raise Exception("No relation has been captured at this point, unexpected!")
                replace_dict[key] = str(capture_relation) + str(process_key)
                
        
        # At end, carry out replace dict on params
        for key in replace_dict.keys():
            params = params.replace(key, replace_dict[key])
            
    
    # Discover columns that are in useAlias and replace them
    # Iterate through relations
    if codeCompHelper.useAlias != {}:
        # Gather replaces into array
        replaces = []
        for relation in codeCompHelper.relations:
            if relation in params:
                relation_positions = [m.start() for m in re.finditer(relation, params)]
                
                # Iterate through relation positions
                for i in range(len(relation_positions)):
                    # From the position to the next space, extract this all
                    space_split = str(params[relation_positions[i]:].split(" ")[0]).strip()
                    remove_dot = space_split.replace(".", "")
                    # Remove dot is our lookup value in useAlias
                    if codeCompHelper.useAlias.get(remove_dot, None) != None:
                        # This is in useAlias
                        new_column_value = str(space_split.split(".")[0]).strip() + "." + codeCompHelper.useAlias.get(remove_dot, None)
                        replaces.append([space_split, new_column_value])
                        
        # Iterate through useAlias, substitute in if exists
        for key in codeCompHelper.useAlias:
            if key in params:
                if params.count(key) > 1:
                    # We have multiple, do "_x and _y"
                    original_count = params.count(key)
                    for i in range(original_count):
                        if i % 2 == 0:
                            params = params.replace(key, prev_df + "." + codeCompHelper.useAlias[key]+"_x", 1)
                        else:
                            params = params.replace(key, prev_df + "." + codeCompHelper.useAlias[key]+"_y", 1)
                            
                    # Add to self.renames, only the first
                    if original_count == 2:
                        if hasattr(self, "renames"):
                            self.renames.append((codeCompHelper.useAlias[key] + "_x", codeCompHelper.useAlias[key]))
                        else:
                            self.renames = [(codeCompHelper.useAlias[key] + "_x", codeCompHelper.useAlias[key], )]
                        
                else:
                    params = params.replace(key, prev_df + "." + codeCompHelper.useAlias[key])
                 
        # Carry out our Replaces
        if replaces != []:
            for replacement in replaces:
                # We want whole relations so match the replace on a string after it
                params = params.replace(replacement[0] + " ", replacement[1] + " ", 1)

    # Replace AND with & and convert to string
    filters = str(params.replace(" AND ", " & "))
    filters = str(filters.replace(" OR ", " | "))
    # Remove first and last brackets
    if (filters[0] == "(") and (filters[-1] == ")"):
        filters = filters[1:-1]
    
    # Split on & and |, keep in original split
    line_split = re.split('([&|])',filters)
    for i in range(len(line_split)):
        # Don't try to clean type information if we have a bare "and" or "or"
        if line_split[i] != "&" and line_split[i] != "|":
            # Clean type information
            line_split[i] = clean_type_information(self, line_split[i])
            
            line_split[i] = str(line_split[i]).strip()
            
            # Process inequalities
            if (">=" in line_split[i]) or (">" in line_split[i]) or ("<=" in line_split[i]) or ("<" in line_split[i]):
                inequals = [">=", ">", "<=", "<"]
                brk_dodge = None
                
                local_ln_sp = line_split[i].strip()
                if (local_ln_sp[0] == "(") and (local_ln_sp[-1] == ")"):
                    local_ln_sp = local_ln_sp[1:-1]
                    brk_dodge = True
                for eql in inequals:
                    if eql in local_ln_sp:
                        local_split = local_ln_sp.split(eql)
                        # Check if second half has two dashes
                        if local_split[1].count("-"):
                            # Date
                            if (local_split[1][0] != "'") and (local_split[1][-1] != "'"):
                                local_split[1] = "'" + local_split[1].strip() + "'"
                                local_ln_sp = eql.join(local_split)
                                # Break after inequality
                                break
                            
                # Complete bracket dodge
                if brk_dodge == True:
                    line_split[i] = "(" + local_ln_sp + ")"
                else:
                    line_split[i] = local_ln_sp
            
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
                line_split[i] = do_like_handling(line_split[i])
                    
            # Do equals
            if " = " in line_split[i]:
                line_split[i] = line_split[i].replace(" = ", " == ")
                
                # Do right hand side quotes replace if needed
                eq_split = line_split[i].split(" == ")
                if len(eq_split) != 2:
                    raise ValueError("Unexpected number of values")
                
                # Check if RHS has quotes
                if (eq_split[1][0] == "'") and (eq_split[1][-1] == "'"):
                    pass
                # Else if relation in RHS then don't add quotes back in
                elif any((x+".") in eq_split[1] for x in codeCompHelper.relations):
                    pass
                # Else if we have a valid subquery variable, so we don't add the quotes
                elif valid_subquery_variable(eq_split[1]):
                    pass
                # Else if all are not alphabetical characters
                elif all(x.isalpha() == False for x in eq_split[1]):
                    pass
                else:
                    # Assume add both back in
                    eq_split[1] = "'" + str(eq_split[1]).strip() + "'"
                    
                    # Then rejoin and set as line_split
                    line_split[i] = " == ".join(eq_split)
                    
            # Do substring
            if "SUBSTRING" in line_split[i]:
                substring_part = "SUBSTRING" + str(str(str(line_split[i].split("SUBSTRING")[1]).split(")")[0]).strip()) + ")"
                substring_replace = handle_substring(substring_part)
                line_split[i] = line_split[i].replace(substring_part, substring_replace)
                
            # Do is not NULL
            if " IS NOT NULL" in line_split[i]:
                line_split[i] = str(str(line_split[i]).replace("IS NOT NULL", "")).strip()
                line_split[i] = "~" + line_split[i] + ".isnull()"
                
        # Clear leading/trailing spaces
        line_split[i] = line_split[i].strip()
        
        if (line_split[i] == "&") or (line_split[i] == "|"):
            line_split[i] = " " + line_split[i] + " "
        else:
            # Add in brackets around conditions if don't exist
            if (line_split[i][0] != "(") and ((line_split[i][-1] != ")") or ((line_split[i][-2] == "(") and (line_split[i][-1] == ")"))):
                line_split[i] = "(" + line_split[i] + ")"
    
    # Reassemble line_split
    # Join on nothing, should have spaces still in it
    filters = "".join(line_split)
    
    return filters

def valid_subquery_variable(string):
    if string[-1].isdigit() != True:
        return False
    
    if string[-2] != "_":
        return False
    
    return True

def do_like_handling(string):
    # Variable for what type of like we have
    not_equal_like = False
    
    # Strip and clean
    string = string.strip()
    if (string[0] == "(") and (string[-1] == ")"):
        string = clean_extra_brackets(string)
        
    # Not equal
    if " !~~ " in string:
        not_equal_like = True
        split_like = string.split(" !~~ ")
    else:
        split_like = string.split(" ~~ ") 
    
    # Catch potential error
    if len(split_like) != 2:
        raise ValueError("Expected only 2 parts to this statement")
    
    for i in range(len(split_like)):
        if (split_like[i][0] == "(") and (split_like[i][-1] == ")"):
            split_like[i] = split_like[i][1:-1]
    
    # Clear quotes if exist
    if (split_like[1][0] == "'") and (split_like[1][-1] == "'"):
        split_like[1] = split_like[1][1:-1]
        
    data_name = split_like[0].strip()
    regex_cmd = sql_like_fragment_to_regex_string(split_like[1])
    
    # Assemble the line
    string = '(' + data_name + '.str.contains("' + str(regex_cmd) +'",regex=True)'
    
    if not_equal_like == True:
        string = string + " == False)"
    else:
        # Otherwise, just close the bracket
        string = string + ")"
    
    return string

# Classes for pandas instructions
class filter_node():
    def __init__(self, data, output):
        self.data = data
        self.output = output
        self.params = None
        
    def set_params(self, in_params):
        self.params = in_params
        
    def set_nodes(self, nodes):
        self.nodes = nodes
        
    def set_alias(self, alias):
        self.alias = alias
        
    def add_subplan_name(self, in_name):
        self.subplan_name = in_name
    
    def set_instructions(self, instructions):
        self.instructions = instructions
        
    def set_index_cond(self, index_cond):
        self.index_cond = index_cond
        
    def to_pandas(self, prev_df, this_df, codeCompHelper, treeHelper):
        # Sometimes, we have a param that is in the output, that gets processed into something different
        # Therefore we need to change it
        
        param_output_indexes = []
        if hasattr(self, "params") and self.params != None:
            for i in range(len(self.output)):
                    # TODO: sometimes the output[i] has brackets around it, which prevents 
                    # us from making a fair and accurate comparison
                    bracket_stripped_params = None
                    if (self.params[0] == "(") and (self.params[-1] == ")"):
                        bracket_stripped_params = self.params[1:-1]
                    
                    if bracket_stripped_params != None:
                        equal_split_params = str(bracket_stripped_params.split(" = ")[0]).strip()
                    else:
                        equal_split_params = None
                    
                    if self.params == self.output[i]:
                        raise ValueError("Unexpected param")
                    elif (bracket_stripped_params != None) and (bracket_stripped_params == self.output[i]):
                        raise ValueError("Unexpected param")
                    elif (equal_split_params != None) and (equal_split_params == self.output[i]):
                        param_output_indexes.append((equal_split_params, i))
        
        subplan_mode = False

        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        instructions = []
        
        # Check for cases!
        case_tracker = "a"
        for i in range(len(self.output)):
            # Don't choose the alias
            is_tuple = False
            if isinstance(self.output[i], tuple):
                current_output = self.output[i][0]
                is_tuple = True
            else:
                current_output = self.output[i]
                        
            if "CASE" in current_output:
                original_look = current_output
                # Find position of CASE
                case_position = current_output.find("CASE")
                
                # This means that the case is the ONLY thing in this line of output
                if case_position == 0:
                    # Extract the entire current output
                    extract_case = current_output
                    
                    # The case name is the tuple value
                    if is_tuple != True:
                        raise ValueError("Case only but no tuple")
                    
                    case_name = self.output[i][1]
                
                # Check that character before is open bracket
                elif current_output[case_position - 1] == "(":
                    else_position = current_output.find("ELSE", case_position)
                    next_bracket = current_output.find(")", else_position)
                    
                    # Extract the entire case statement
                    extract_case = current_output[case_position : next_bracket]
                    
                    # Create case_name
                    case_name = "case_" + str(case_tracker)
                    # Increment case_tracker
                    case_tracker = chr(ord(case_tracker) + 1)
                
                else:
                    raise ValueError("Unexpectedly formatted CASE")
                
                # Do case aggregation
                case_string = aggregate_case(extract_case, prev_df, this_df, treeHelper)
                
                # Append to instructions
                statement = str(prev_df) + "['" + case_name + "'] = " + case_string
                instructions.append(statement)
                
                # Replace in self.output the extract_case with the new case_string
                if is_tuple == True:
                    self.output[i] = (self.output[i][0].replace(extract_case, case_name), self.output[i][1])
                else:
                    self.output[i] = self.output[i].replace(extract_case, case_name)
                    
                # Add to useAlias
                # The search parameter will be:
                    # extract_case
                # # The Alias will be: case_name
                codeCompHelper.useAlias[extract_case] = case_name
        
        # Replace the 1st index with the self.output[2nd index] value
        # Set group_key to new thing
        if param_output_indexes != []:
            for i in range(len(param_output_indexes)):
                # Replace
                # Get replacement_value
                replacement_value = self.output[param_output_indexes[i][1]]
                # This might be a tuple
                if isinstance(replacement_value, tuple):
                    # Validate these are equal
                    if replacement_value[0] != replacement_value[1]:
                        raise ValueError("Tuple values are not equal, not good!")
                    else:
                        replacement_value = replacement_value[0]
                        
                # Add the this_df
                replacement_value = str(prev_df) + "." + str(replacement_value)
                
                self.params = self.params.replace(param_output_indexes[i][0], replacement_value)
                
        # Clean params
        if hasattr(self, "params") and self.params != None:
            self.params = clean_filter_params(self, self.params, codeCompHelper, prev_df)
        if hasattr(self, "index_cond") and self.index_cond != None:
            self.index_cond = clean_filter_params(self, self.index_cond, codeCompHelper, prev_df)
        
        # Check if "SubPlan"
        if self.params != None:
            if "subplan" in self.params.lower():
                subplan_mode = True
                self.params = clean_subplan_params(self, self.params, prev_df, self.data)
                
        # Edit params:
        if self.params != None:
            if subplan_mode == True:
                # Do output for subplan mode
                instructions.append(this_df + " = " + self.params)
                
                if hasattr(self, "index_cond"):
                    instructions.append(self.index_cond)
                
                output_cols = choose_aliases(self, codeCompHelper)
                
                # Limit to output columns
                if codeCompHelper.column_limiting and (output_cols != []):
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
                if codeCompHelper.column_limiting and (output_cols != []):
                    statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
                    instructions.append(statement2_string)
        else:            
            output_cols = choose_aliases(self, codeCompHelper)
            
            # Limit to output columns
            if codeCompHelper.column_limiting and (output_cols != []):
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
                # This may return a tuple, with the name and an alias, handle this appropriately
                proc_output = process_output(self, [sort_split[0]], codeCompHelper)
                if isinstance(proc_output[0], tuple):
                    column = proc_output[0][0]
                else:
                    column = proc_output[0]
                    
                keys.append(column)
                ascendings.append(True)
            else:
                # TODO: Temporary skip, remove after support for substring working
                if sort_split[0][:9] == "SUBSTRING":
                    ascendings.append(True)
                    keys.append(individual_sort)
                    continue
                
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
        # Variable to store all the instructions we generate
        instructions = []
        
        # TODO: Sometimes, we have a sort_key that is in the output, that gets processed into something different
        sort_output_indexes = []
        # Compare the sort key with each of the outputs
        for j in range(len(self.sort_key)):
            for i in range(len(self.output)):
                # sort_key might have desc or asc in it
                sort_key_stripped = str(str(self.sort_key[j]).replace("DESC", "").replace("ASC", "")).strip()
                if self.sort_key[j] == self.output[i]:
                    sort_output_indexes.append((j, i))
                elif sort_key_stripped == self.output[i]:
                    # Determine what type of sorting it is
                    if "DESC" == self.sort_key[j][-4:]:
                        sort_output_indexes.append((j, i, "DESC"))
                    elif "ASC" == self.sort_key[j][-3:]:
                        sort_output_indexes.append((j, i, "ASC"))
                    else:
                        raise ValueError("Unknown sorting request")

        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        
        # Set group_key to new thing
        if sort_output_indexes != []:
            for sort, *output in sort_output_indexes:
                # Unspool output
                if len(output) == 1:
                    sort_direction = None
                    output = output[0]
                elif len(output) == 2:
                    # Set these in reverse order to what feels natural so we don't overwrite values
                    # that we will soon need
                    sort_direction = output[1]
                    output = output[0]
                else:
                    raise ValueError("Unexpected number of values in the unpacked output")
                
                # Set sort key to this index
                self.sort_key[sort] = self.output[output]
                
                # Is it a tuple, set to [0]
                if isinstance(self.sort_key[sort], tuple):
                    # Check that this is not in useAlias already
                    if codeCompHelper.useAlias.get(self.sort_key[sort], None) != None:
                        # This is already in useAlias
                        # Validate aliases are the same
                        if self.sort_key[sort][1] != codeCompHelper.useAlias.get(self.sort_key[sort], None):
                            raise ValueError("Aliases not the same")
                        
                        # Set to index [1]
                        self.sort_key[sort] = self.sort_key[sort][1]                        
                        
                    else:
                        # Not in useAlias, we can add it
                    
                        # If we have a sort_key with a alias, we should rename the column to the alias, and sort by that
                        # df_sort_4['nation'] = (df_sort_4.n_name)
                        # Use the previous dataframe as this is happening before the sort, so we can use the alias in the sort
                        statement = prev_df + "['" + str(self.sort_key[sort][1]) + "'] = " + str(prev_df) + "." + str(self.sort_key[sort][0])
                        instructions.append(statement)
                        # Set in codeComp useAlias, to track when we should use the alias
                        codeCompHelper.useAlias[self.sort_key[sort][0]] =  self.sort_key[sort][1]
                        # Set to index [1]
                        self.sort_key[sort] = self.sort_key[sort][1]
                
                # It's a tuple, check if it's in use_alias
                if codeCompHelper.useAlias != {}:
                    for i in range(len(self.sort_key)):
                        if codeCompHelper.useAlias.get(self.sort_key[i], None) != None:
                            # In useAlias
                            self.sort_key[i] = codeCompHelper.useAlias.get(self.sort_key[i], None)                  
                        
                # If we have a sorting order then add it onto the end
                if sort_direction != None:
                    self.sort_key[sort] = str(self.sort_key[sort] + " " + sort_direction).strip()
              
        # Set sort_keys
        self.sort_key = self.process_sort_key(codeCompHelper)  
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        
        # Sorting to an intermediate dataframe
        columns, ascendings = self.sort_key
        statement1_string = this_df + " = " + prev_df + ".sort_values(by=" + str(columns) + ", ascending=" + str(ascendings) + ")"
        instructions.append(statement1_string)
        
        # Choose aliases
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        if codeCompHelper.column_limiting and (output_cols != []):
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
        
        # Sometimes our output looks like:
        #       df_aggr_1 = df_aggr_1[['avg_yearly']]
        #       df_limit_1 = df_aggr_1[['avg_yearly']]
        # Where we have a repeated line
        # Check only has one children
        if len(self.nodes) != 1:
            raise Exception("Limit node with multiple children, something is malformed here")
        
        # Try and determine no column out
        no_column_output = True
        
        # Check length the same
        if len(self.nodes[0].output) != len(self.output):
            no_column_output = False
        
        # Check that all nodes are the same
        child_output_list = []
        for output_tag in list(self.nodes[0].output):
            if isinstance(output_tag, tuple):
                child_output_list.append(output_tag[1])
            else:
                child_output_list.append(output_tag)
        child_output = dict.fromkeys(child_output_list)
        
        # Iterate through all keys
        for current_key in list(self.output):
            if child_output.get(current_key, "Not Found") == "Not Found":
                # Not in dictionary
                no_column_output = False
                break      
        
        if self.output != []:
            if not no_column_output:
                if codeCompHelper.column_ordering:
                    output_cols = choose_aliases(self, codeCompHelper, final_output=True)
                    # Undo axes to normal columns
                    if codeCompHelper.indexes != []:
                        if codeCompHelper.indexes != []:
                            instructions.append(prev_df + " = " + prev_df + ".rename_axis(" + str(codeCompHelper.indexes) + ").reset_index()")
            
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
        else:
            no_column_output = True
         
        if not no_column_output:           
            statement3_string = str(this_df) + " = " + str(this_df) + ".head("+str(self.amount)+")"
            instructions.append(statement3_string)
        else:
            # No column output, use prev_df in the head
            statement3_string = str(this_df) + " = " + str(prev_df) + ".head("+str(self.amount)+")"
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

# TODO: Does this even need to be here
def inner_aggregation(string, prev_df, this_df, treeHelper):
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
            if treeHelper.use_numpy == True:
                # Numpy case
                inner_string = aggregate_case(inner, prev_df, this_df, treeHelper)
            else:
                # Pandas only
                inner_string = pandas_aggregate_case(inner, prev_df)
                
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

def aggregate_case_worker(values, prev_df, this_df, treeHelper):
    # Clean values
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
            values[i] = do_like_handling(values[i])
            
            if (values[i][0] == "(") and (values[i][-1] == ")"):
                values[i] = values[i][1:-1]
                
            values[i] = str(prev_df) + "." + str(values[i])
            
        # Hand this off to the s_group function
        elif any(x in values[i] for x in [" * ", " - ", " / ", " + "]):  
            # If any numerical operators present
            # TODO: This is when the expression tree parsing should do
            
            # Set output_name
            output_name = None
            if not treeHelper.bench:
                output_name = str(treeHelper.expr_output_path)+str(treeHelper.expr_tree_tracker)
            else:
                output_name = False
            
            tree = Expression_Solver(str(values[i]), output_name, prev_df, this_df)
            values[i] = tree.evaluate()
            
            # Increment treeHelper
            treeHelper.expr_tree_tracker += 1
            
            # Old method, using aggregate_sum
            # values[i] = aggregate_sum(values[i], s_group = prev_df)
        
        # Make sure it's a boolean AND/OR and it contains the right kind of operators
        elif ((" or " in values[i].lower()) or (" and " in values[i].lower())) and any(x in values[i] for x in [" = ", " == ", " <> ", " > ", " >= ", " < ", " <= "]):
            # Shuck the value
            if (values[i][0] == "(") and (values[i][-1] == ")"):
                values[i] = values[i][1:-1]
            
            if " OR " in values[i]:
                split_on = values[i].split(" OR ")
                join_on = " | "
            elif " or " in values[i]:
                split_on = values[i].split(" or ")
                join_on = " | "
            elif " AND " in values[i]:
                split_on = values[i].split(" AND ")
                join_on = " & "
            elif " and " in values[i]:
                split_on = values[i].split(" and ")
                join_on = " & "
            else:
                raise ValueError("Not sure what we want to split the string on")
            
            for j in range(len(split_on)):
                # If contained in brackets
                if (split_on[j][0] == "(") and (split_on[j][-1] == ")"):
                    split_on[j] = split_on[j][1:-1]
                
                if " = " in split_on[j]:
                    split_on_eq = split_on[j].split(" = ")
                    re_assemble = " == "
                elif " == " in split_on[j]:
                    split_on_eq = split_on[j].split(" == ")
                    re_assemble = " == "
                elif " <> " in split_on[j]:
                    split_on_eq = split_on[j].split(" <> ")
                    re_assemble = " != "
                elif " > " in split_on[j]:
                    split_on_eq = split_on[j].split(" > ")
                    re_assemble = " > "
                elif " >= " in split_on[j]:
                    split_on_eq = split_on[j].split(" >= ")
                    re_assemble = " >= "
                elif " <= " in split_on[j]:
                    split_on_eq = split_on[j].split(" <= ")
                    re_assemble = " <= "
                elif " < " in split_on[j]:
                    split_on_eq = split_on[j].split(" < ")
                    re_assemble = " < "
                else:
                    raise ValueError("Unknown parameter we're trying to split on")
                    
                # Should be of the form:
                # o_orderpriority == '1-URGENT'
                split_on[j] = "(" + prev_df + "['" + str(split_on_eq[0]) + "']" + str(re_assemble) + str(split_on_eq[1]) + ")"
               
            values[i] = str(join_on.join(split_on)).strip()     
            
        elif (len(values[i]) > 1): 
            if values[i] == "NULL":
                pass
            else:
                if any(x in values[i] for x in [" = ", " == ", " <> ", " > ", " < ", " >= ", " <= "]):
                    # Who is this for parsing?
                    values[i] = aggregate_sum(values[i], s_group = prev_df)
                
                if " = " in values[i]:
                    values[i] = values[i].replace(" = ", " == ")
                
                if " <> " in values[i]:
                    values[i] = values[i].replace(" <> ", " != ")
                
        # Cleaning validation
        values[i] = values[i].strip()
        if (values[i][0] == "(") and (values[i][1] == " ") and (values[i][-1] == ")") and (values[i][-2] == " "):
            values[i] = values[i][2:-2]
            
        if (values[i][0] == "'") and (values[i][-1] == "'"):
            values[i] = values[i][1:-1]
            
        if (values[i][0] == '"') and (values[i][-1] == '"'):
            values[i] = values[i][1:-1]
            
    return values

def aggregate_case(inner_string, prev_df, this_df, treeHelper):
    # TODO: Need to support the new CASE type:
    #   "CASE 
    #       WHEN (s_nationkey = 17) THEN BOTTLE 
    #       WHEN (s_nationkey = 5) THEN 'BAG' 
    #       ELSE 'NEITHER' 
    #   END"
    
    when_count = inner_string.lower().count("when")
    then_count = inner_string.lower().count("then")
    
    if (when_count == then_count) and (when_count == 1):
        # We have only 1 condition
        # We can use NP.WHERE for this, as it's more efficient
        
        # Don't make it to lower, messes up values
        if ("ELSE" in inner_string) and ("END" in inner_string):
            else_value = str(inner_string.split("ELSE")[1].split("END")[0]).strip()
        elif ("else" in inner_string) and ("end" in inner_string):
            else_value = str(inner_string.split("else")[1].split("end")[0]).strip()
        else:
            raise ValueError("Inconsistent capitalisation")    
        
        if ("CASE WHEN" in inner_string) and ("THEN" in inner_string):
            when_value = str(inner_string.split("CASE WHEN")[1].split("THEN")[0]).strip()
        elif ("case when" in inner_string) and ("then" in inner_string):
            when_value = str(inner_string.split("case when")[1].split("then")[0]).strip()
        else:
            raise ValueError("Inconsistent capitalisation")    
        
        if ("THEN" in inner_string) and ("ELSE" in inner_string):
            then_value = str(inner_string.split("THEN")[1].split("ELSE")[0]).strip()
        elif ("then" in inner_string) and ("else" in inner_string):
            then_value = str(inner_string.split("then")[1].split("else")[0]).strip()
        else:
            raise ValueError("Inconsistent capitalisation")      
    
        # Put into an array, Put values in the order of the pandas expression
        values = aggregate_case_worker([when_value, then_value, else_value], prev_df, this_df, treeHelper)
        
        # Create the output string,
        # li_pa_join.apply(lambda x: x["l_extendedprice"] * (1 - x["l_discount"]) if x["p_type"].startswith("PROMO") else 0, axis=1)
        #inner_string = prev_df + '.apply(lambda x: ' + str(values[0]) + ' if ' + str(values[1]) + ' else ' + str(values[2]) + ', axis=1)'        
        # np.where(df['col2']<9, 'value1','value4')
        inner_string = "np.where(" + str(values[0]) + ", " + str(values[1]) + ", " + str(values[2]) + ")"
        
    elif (when_count == then_count) and (when_count > 1):
        # We have multiple WHEN and THEN conditions
        # We use NP.SELECT for this, as it's more efficient for many conditions and has much better readability
                
        # "CASE WHEN (s_nationkey = 17) THEN BOTTLE WHEN (s_nationkey = 5) THEN 'BAG' ELSE 'NEITHER' END"
        
        if "else" in inner_string.lower():
            # We have an else
            if "ELSE" in inner_string:
                else_value = str(inner_string.split("ELSE")[1].split("END")[0]).strip()
            else:
                else_value = str(inner_string.split("else")[1].split("end")[0]).strip()
            
            # Split on Else, to simplfy later steps
            if "ELSE" in inner_string:
                inner_string = str(inner_string.split("ELSE")[0]).strip()
            else:
                inner_string = str(inner_string.split("else")[0]).strip()
                               
            # Get rid of CASE as well
            if "CASE" in inner_string:
                inner_string = str(inner_string.split("CASE ", 1)[1]).strip()
            else:
                inner_string = str(inner_string.split("case ", 1)[1]).strip()
            
        else:
            else_value = None
            # Get rid of CASE and END
            if "CASE" in inner_string:
                inner_string = str(inner_string.split("CASE")[1]).strip()
            else:
                inner_string = str(inner_string.split("case")[1]).strip()
                
            if "END" in inner_string:
                inner_string = str(inner_string.split("END")[0]).strip()
            else:
                inner_string = str(inner_string.split("end")[0]).strip()
        
        expression_case = False
        # Split on WHEN
        if "WHEN" in inner_string:
            split_on_when = inner_string.split("WHEN")
            
            if (inner_string.strip()).split(" ")[0].strip() == "WHEN":
                expression_case = False
            else:
                expression_case = (inner_string.strip()).split(" ")[0].strip()
            
        elif "when" in inner_string:
            split_on_when = inner_string.split("when")
            
            if (inner_string.strip()).split(" ")[0].strip() == "when":
                expression_case = False
            else:
                expression_case = (inner_string.strip()).split(" ")[0].strip()
        else:
            raise ValueError("WHEN/when not found in Aggregation case")
        
        # Check if we have an expression case, which look like:
        # CASE p_container
        #     WHEN 'JUMBO PKG' THEN 'Jumbo Package'
        #     WHEN 'SM PKG' THEN 'Small Package'
        #     ELSE 'Other'
        # END as container_annotation
        # We can do this by checking whether the "WHEN" or "when" is the first word
        if expression_case != False:
            # Let's remove the first element of split_on_when
            del split_on_when[0]
        
        # Iterate through split_when, cleaning out empty elements and segmenting into whens and thens
        whens = []
        thens = []
        for i in range(len(split_on_when)):
            split_on_when[i] = split_on_when[i].strip()
            
            # If not an empty string, let's split on THEN
            if split_on_when[i] != "":
                if "THEN" in split_on_when[i]:
                    split_on_then = split_on_when[i].split("THEN")
                elif "then" in split_on_when[i]:
                    split_on_then = split_on_when[i].split("then")
                else:
                    raise ValueError("THEN/then not found in Aggregation case")
                
                if len(split_on_then) != 2:
                    raise ValueError("Unexpected Split on Then")
                
                # Append when and then
                whens.append(split_on_then[0].strip())
                thens.append(split_on_then[1].strip())
                
        # Validation
        if len(whens) != when_count:
            raise ValueError("CASE not enough WHEN conditions found")
        elif len(thens) != then_count:
            raise ValueError("CASE not enough THEN conditions found")
        
        # np.select([
        #     (df.S == 1) & (df.A == 1),
        #     (df.S == 1) & (df.A == 0),
        #     (df.S == 2) & (df.A == 1),
        #     (df.S == 2) & (df.A == 0)
        # ], [1, 0, 0, 1], ELSE)
        
        # TODO: What if it's not equals!
        # Expression case, add the expression case and == to all of the whens
        if expression_case != False:
            for i in range(len(whens)):
                whens[i] = prev_df + "['" + str(expression_case) + "'] == " + str(whens[i])
        
        # We use aggregate_case_worker to process them, then for process_whens
        # We need to remove quotes, as these should be testable boolean constructs
        process_whens = str(aggregate_case_worker(whens, prev_df, this_df, treeHelper))[1:-1]
        # Split on comma between conditions
        split_whens = process_whens.split(", ")
        for i in range(len(split_whens)):
            split_whens[i] = split_whens[i].strip()
            # Strip if true
            if (split_whens[i][0] == "'") and (split_whens[i][-1] == "'"):
                split_whens[i] = split_whens[i][1:-1]
            if (split_whens[i][0] == '"') and (split_whens[i][-1] == '"'):
                split_whens[i] = split_whens[i][1:-1]   
            split_whens[i] = split_whens[i].strip()
        process_whens = "[" + str(", ".join(split_whens)) + "]"
        
        process_thens = aggregate_case_worker(thens, prev_df, this_df, treeHelper)
        
        # We may not have an else!
        if else_value == None:
            # No else
            inner_string = "np.select(" + str(process_whens) + ", " + str(process_thens) + ")"
        else:
            # We have an else
            # Use the aggregate_case_worker, input as a single element list, then pop it out
            process_else = aggregate_case_worker([else_value], prev_df, this_df, treeHelper)[0]
            
            # Add back in quote marks if it doesn't have them
            if (process_else[0] != "'") and (process_else[-1] != "'"):
                process_else = "'" + process_else + "'"
            
            inner_string = "np.select(" + str(process_whens) + ", " + str(process_thens) + ", " + str(process_else) + ")"
    
    else:
        raise ValueError("Unexpected counting from aggregation string!")
    
    return inner_string

def do_aggregation(self, prev_df, this_df, codeCompHelper, treeHelper):
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
            
            tree = Expression_Solver(str(col[0]), output_name, prev_df, this_df)
            pandas = tree.evaluate()
            if any([True for agg in tree.agg_funcs if agg+"()" in pandas]):
                code_line = str(this_df) + "['" + str(col[1]) + "'] = [" + str(pandas) + "]"
            else:
                code_line = str(this_df) + "['" + str(col[1]) + "'] = " + str(pandas)
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
            
            tree = Expression_Solver(str(col), output_name, prev_df, this_df)
            pandas = tree.evaluate()
            
            # Handle complex names in output
            is_complex, new_name = complex_name_solve(col)
            if is_complex == True:
                # Replace these
                codeCompHelper.add_bracket_replace(col, new_name)
                
            # Patch for just copying
            if new_name == None:
                new_name = col
            
            if any([True for agg in tree.agg_funcs if agg+"()" in pandas]):
                code_line = str(this_df) + "['" + str(new_name) + "'] = [" + str(pandas) + "]"
            else:
                code_line = str(this_df) + "['" + str(new_name) + "'] = " + str(pandas)
            local_instructions.append(code_line)
            
            # Increment treeHelper
            treeHelper.expr_tree_tracker += 1       

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
                appendingCol = clean_extra_brackets(appendingCol)
                
            # Check if new_appendingCol is in the cCHelper bracket_replace
            if cCHelper.bracket_replace.get(appendingCol, None) != None:
                # This col is in the dict, use the replacement as the column in forward
                appendingCol = cCHelper.bracket_replace[appendingCol]
                
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
                
            # Check if appendingCol is in the ccHelper useAlias
            if cCHelper.useAlias.get(appendingCol, None) != None:
                # The col is in the dict, use the alias stored there 
                appendingCol = cCHelper.useAlias.get(appendingCol, None)
            
            # Append the column, first check if it is in the indexes, if so, skip
            if final_output:
                output.append(appendingCol)
            if appendingCol in cCHelper.indexes:
                continue
            else:
                output.append(appendingCol)
                
    # TODO: add to cCHelper useAlias
    
    # If any two items in output are the same
    potential_dupe_list = duplicates_in_list(output)
    if potential_dupe_list != []:
        print("Potential_dupe_list: " + str(potential_dupe_list))
        # We have duplicates
        # Iterate through dupes
        # TODO: We need a MUCH better way of determining in which order these should go!
        
        # We need to look at our current node children
        replace_options = ["_y", "_x"] 
        for dupe_item in potential_dupe_list:
            replace_counter = 0
            # Iterate through output items
            for i in range(0, len(output)):
                # We have a duplicate!
                if output[i] == dupe_item:
                    if replace_counter > 1:
                        raise ValueError("Too many replaces")
                    else:
                        # Add to useAlias
                        if isinstance(self.output[i], tuple):
                            if len(self.output[i]) == 3:
                                
                                search_output = self.output[i]
                                
                                # We need to search for this in the output of children
                                # If in child[0], then _x, if in child[1] then _y
                                
                                if len(self.nodes) != 2:
                                    raise Exception("Not enough children of node")
                                
                                # Search left, then right, to determine the tag
                                add_tag = None
                                for output_tag in self.nodes[0].output:
                                    if output_tag == search_output:
                                        add_tag = "_x"
                                        break
                                for output_tag in self.nodes[1].output:
                                    if output_tag == search_output:
                                        add_tag = "_y"
                                        break   
                                    
                                if (add_tag == None):
                                    raise Exception("We didn't find an add_tag")
                                
                                output[i] = output[i] + add_tag
                                
                                # We are in this special circumstance
                                # Use the 2nd idx, which should be: n2n_name (no dot!)
                                cCHelper.useAlias[self.output[i][2]] = output[i]
                            else:
                                output[i] = output[i] + replace_options[replace_counter]
                                cCHelper.useAlias[self.output[i][0]] = output[i]
                        else:
                            output[i] = output[i] + replace_options[replace_counter]
                            cCHelper.useAlias[self.output[i]] = output[i]
                        replace_counter += 1

    return output

def duplicates_in_list(seq_list):
    seen = set()
    seen_add = seen.add
    # adds all elements it doesn't know yet to seen and all other to seen_twice
    seen_twice = set( x for x in seq_list if x in seen or seen_add(x) )
    # turn the set into a list (as requested)
    return list( seen_twice )

def handle_complex_aggregations(self, data, codeCompHelper, treeHelper, prev_df, this_df, cases):
    # Array to hold decisions
    # Before Aggrs:
        # Array for aggregations that need to happen before grouping
        # Format: [column name, instruction]
    before_aggrs = []
    
    # During Aggrs:
        # Array for aggregations (simple ones) that can happen during grouping
        # Format: [column name, uses column, aggr type]
    during_aggrs = []
    
    # After Aggrs:
    after_aggrs = []
    
    # Initialise BeforeCounter
    beforeCounter = 0
    
    # For each output in self.output
    # Determine how many columns it uses
            # Split off the sum avg or count
    for i, col in enumerate(data):
        
        if isinstance(col, tuple):
            if ("count" in str(col[0]).lower()) and ("distinct" in str(col[0]).lower()):
                # TODO: Make this more elegant
                # Hardcoded support for count distinct
                
                # Don't add any before
                during_aggrs.append([str(col[1]), col[0].lower().split("distinct ")[1].replace("(", "").replace(")", "").strip(), "count_distinct"])
                            
                # Don't run rest of this iteration
                continue
        
            # rename
            if ".dt." in str(col[0]).lower():
                before_aggrs.append([col[1], prev_df + "." + col[0]])
                
                # Don't run rest of this iteration
                continue
            
            # Set output_name
            output_name = None
            if not treeHelper.bench:
                output_name = str(treeHelper.expr_output_path)+str(treeHelper.expr_tree_tracker)
            else:
                # Means no visualisation will be created
                output_name = False
            
            tree = Expression_Solver(str(col[0]), output_name, prev_df, this_df, beforeCounter, codeComp=codeCompHelper)
            # Increment treeHelper
            treeHelper.expr_tree_tracker += 1
            
            if not any([True for agg in tree.agg_funcs if agg in str(col[0])]):
                if not any([True for agg in [" + ", " - ", " / ", " * "] if agg in str(col[0])]):
                    before_aggrs.append([col[1], prev_df + "." + col[0]])
                
                    # Don't run rest of this iteration
                    continue        
            
            before, during, after = tree.group_aggregate()
            # Edit after array
            new_after = []
            for item in after:
                new_after.append([str(col[1]), item])
            
            # Efficiency improval
            # TODO: Hardcoded for a single element at the moment
            if (len(new_after) == 1) and (len(during) == 1):
                # get_temporary column_value
                temp_col_val = str(new_after[0][1].split(".")[1]).strip()
                if any(x in temp_col_val for x in ["*", "-", "/", "+"]):
                    # If any aggregation function in the col, val, then don't do the efficiency boost
                    pass
                elif temp_col_val == during[0][0]:
                    # Then we can do the replacement
                    during[0][0] = new_after[0][0]
                    del new_after[0]
                    
            # Column rename, for index
            # It will have no before, during and only 1 after
            if (before == []) and (during == []) and (len(new_after) == 1):
                # Strip outer brackets off if they're present
                new_after_col = new_after[0][1]
                if (new_after_col[0] == "(") and (new_after_col[-1] == ")"):
                    new_after_col = new_after_col[1:-1]
                temp_col_var = str(new_after_col.split(".")[1]).strip()
                # If it's an index
                if temp_col_var in codeCompHelper.indexes:
                    if len(codeCompHelper.indexes) != 1:
                        # TODO: Column rename code
                        #raise ValueError("Need to write column rename code")
                        pass
                    else:
                        # We should make the col an index
                        codeCompHelper.setIndexes(new_after[0][0])
                        new_after[0] = ["", this_df + str(".index.rename('") + str(new_after[0][0]) +"')"]
            
            before_aggrs += before
            during_aggrs += during
            after_aggrs += new_after
            
            # Set back beforeCounter
            beforeCounter = tree.before_counter
                
        else:
            # No alias for this column
            if "." in col:
                col_no_df = str(col.split(".")[1])
            else:
                col_no_df = None   
            
            # Initialise a tree to access the aggregation functions that it recognises
            tree = Expression_Solver(str(col), False, prev_df, this_df)
            
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
            # No aggregation functions in the string
            elif not any([agg in col for agg in tree.agg_funcs]):
                # If these are aggregations in the column
                # That are not indexes
                # We should add them to during to use last
                during_aggrs.append([col, col, "last"])
            else:
                # We can process this as an expression tree
                # Set output_name
                output_name = None
                if not treeHelper.bench:
                    output_name = str(treeHelper.expr_output_path)+str(treeHelper.expr_tree_tracker)
                else:
                    # Means no visualisation will be created
                    output_name = False
                    
                tree = Expression_Solver(str(col), output_name, prev_df, this_df, beforeCounter, codeComp=codeCompHelper)
                # Increment treeHelper
                treeHelper.expr_tree_tracker += 1
                
                before, during, after = tree.group_aggregate()
                before_aggrs += before
                during_aggrs += during
                # Edit after array
                new_after = []
                for item in after:
                    
                    # Handle complex names in output
                    is_complex, new_name = complex_name_solve(col)
                    if is_complex == True:
                        if cases != {}:
                            if cases.get(col, None) != None:
                                # This col is in cases, this means we have to add it as it's cases form
                                codeCompHelper.add_bracket_replace(cases.get(col, None), new_name)
                        
                        # It's okay that we add multiple, we need all of these                   
                        codeCompHelper.add_bracket_replace(col, new_name)
                    new_after.append([str(new_name), item])
                
                after_aggrs += new_after
                
                # Set back beforeCounter
                beforeCounter = tree.before_counter
     
    # Handle and rearrange output
    return before_aggrs, during_aggrs, after_aggrs

def pandas_aggregate_case(inner_string, prev_df):
    else_value = str(inner_string.split("ELSE")[1].split("END")[0]).strip()
    when_value = str(inner_string.split("CASE WHEN")[1].split("THEN")[0]).strip()
    then_value = str(inner_string.split("THEN")[1].split("ELSE")[0]).strip()
    
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
            
            # Iterate through split_starts, cleaning the parts that are ill-formatted
            for j in range(len(split_starts)):
                # Remove brackets at start and end of string
                if (split_starts[j][0] == "(") and (split_starts[j][-1] == ")"):
                    split_starts[j] = split_starts[j][1:-1]
                    
                # Remove quotes at start and end of string
                if (split_starts[j][0] == "'") and (split_starts[j][-1] == "'"):
                    split_starts[j] = split_starts[j][1:-1]
            
            values[i] = 'x["' + split_starts[0] + '"].startswith("' + split_starts[1] + '")'
            
        # Hand this off to the s_group function
        elif ("*" in values[i]):  # and (len(values[i]) > 1)
            values[i] = aggregate_sum(values[i], s_group = "x")
            
        elif ("OR" in values[i]) or ("AND" in values[i]):
            # Shuck the value
            if (values[i][0] == "(") and (values[i][-1] == ")"):
                values[i] = values[i][1:-1]
            
            if " OR " in values[i]:
                split_on = values[i].split(" OR ")
                join_on = " | "
            elif " AND " in values[i]:
                split_on = values[i].split(" AND ")
                join_on = " & "
            else:
                raise ValueError("Not sure what we want to split the string on")
            
            for j in range(len(split_on)):
                # If contained in brackets
                if (split_on[j][0] == "(") and (split_on[j][-1] == ")"):
                    split_on[j] = split_on[j][1:-1]
                
                if " = " in split_on[j]:
                    split_on_eq = split_on[j].split(" = ")
                    re_assemble = " == "
                elif " == " in split_on[j]:
                    split_on_eq = split_on[j].split(" == ")
                    re_assemble = " == "
                elif " <> " in split_on[j]:
                    split_on_eq = split_on[j].split(" <> ")
                    re_assemble = " != "
                elif " != " in split_on[j]:
                    split_on_eq = split_on[j].split(" != ")
                    re_assemble = " != "
                else:
                    raise ValueError("Unknown parameter we're trying to split on")
                    
                # Should be of the form:
                # o_orderpriority == '1-URGENT'
                split_on[j] = "( x['" + str(split_on_eq[0]) + "']" + str(re_assemble) + str(split_on_eq[1]) + " )"
                
            values[i] = str(join_on.join(split_on)).strip()     
            
        elif (len(values[i]) > 1):
            values[i] = aggregate_sum(values[i], s_group = "x")      
            
        # Catch equals and replace
        if " = " in values[i]:
            values[i] = values[i].replace(" = ", " == ")    
    
    # li_pa_join.apply(lambda x: x["l_extendedprice"] * (1 - x["l_discount"]) if x["p_type"].startswith("PROMO") else 0, axis=1)
    inner_string = prev_df + '.apply(lambda x: ' + str(values[0]) + ' if ' + str(values[1]) + ' else ' + str(values[2]) + ', axis=1)'        
    
    return inner_string

class group_aggr_node():
    def __init__(self, output, group_key):
        self.output = output
        self.group_key = group_key
    
    def add_filter(self, in_filter):
        self.filter = in_filter
        
    def set_nodes(self, nodes):
        self.nodes = nodes
    
    def process_group_key(self, group_key):
        grouping_keys = []
        for key in group_key:
            if "." in key:
                grouping_keys.append(key.split(".")[1])
            else:
                grouping_keys.append(key)
        return grouping_keys
    
    def to_pandas(self, prev_df, this_df, codeCompHelper, treeHelper):
        # Clean filters
        if hasattr(self, "filter") and self.filter != None:
            self.filter = clean_filter_params(self, self.filter, codeCompHelper, prev_df)
        
        # TODO: Sometimes, we have a group_key that is in the output, that gets processed into something different
        group_output_indexes = []
        for i in range(len(self.output)):
            for j in range(len(self.group_key)):
                # TODO: sometimes the output[i] has brackets around it, which prevents 
                # us from making a fair and accurate comparison
                bracket_stripped_output = None
                if (self.output[i][0] == "(") and (self.output[i][-1] == ")"):
                    bracket_stripped_output = self.output[i][1:-1]
                
                if self.group_key[j] == self.output[i]:
                    group_output_indexes.append((j, i))
                elif (bracket_stripped_output != None) and (self.group_key[j] == bracket_stripped_output):
                    group_output_indexes.append((j, i))
        
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        
        # Set group_key to new thing
        if group_output_indexes != []:
            for group, output in group_output_indexes:
                # Set sort key to this index
                self.group_key[group] = self.output[output]
                # Is it a tuple, set to [0]
                if isinstance(self.group_key[group], tuple):
                    # If it's a column rename, set to renamed value
                    #if ".dt." in self.group_key[group][0]:
                    #    self.group_key[group] = self.group_key[group][1]
                    #else:
                    
                    # Patch: Always set as index [1], is this correct?
                    self.group_key[group] = self.group_key[group][1]
        
        # Process group key
        self.group_key = self.process_group_key(self.group_key)
        
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
            elif " == " in self.filter:
                filter_type = "=="
                cut_filter = str(str(self.filter).split(" == ")[0]).strip()
                filter_amount = str(str(self.filter).split(" == ")[1]).strip()
            else:
                raise ValueError("Haven't written Filter for Group Aggregation to manage filters like: " + str(self.filter))
            
            filter_steps = [[cut_filter, filter_type, filter_amount]]
            
            if isinstance(self.filter, str):
                before_filter, during_filter, after_filter = handle_complex_aggregations(self, [cut_filter], codeCompHelper, treeHelper, prev_df, this_df, {})
            elif isinstance(self.filter, list):
                before_filter, during_filter, after_filter = handle_complex_aggregations(self, cut_filter, codeCompHelper, treeHelper, prev_df, this_df, {})
            else:
                raise ValueError("Unrecognised type of filter for the Group Aggregation Node, filter: " + str(self.filter))
        
        instructions = []
        
        # Track the replaces of cases
        # DICT: [[original_look, new_look], ...]
        case_replaces = {}
        case_tracker = "a"
        distinct_replaces = {}
        case_locations = {}
        distinct_tracker = "a"
        # Check for cases and distincts!
        for i in range(len(self.output)):
            # Don't choose the alias
            is_tuple = False
            if isinstance(self.output[i], tuple):
                current_output = self.output[i][0]
                is_tuple = True
            else:
                current_output = self.output[i]
            
            # Distinct handling
            # TODO: Remove this for now
            """
            if "distinct" in current_output.lower():
                original_look = current_output
                # Find position of distinct
                distinct_position = current_output.find("DISTINCT")
                # Check that character before is open bracket
                if current_output[distinct_position - 1] != "(":
                    raise ValueError("Unexpectedly formatted DISTINCT")
                next_bracket = current_output.find(")", distinct_position)
                
                # Extract distinct statement
                extract_distinct = current_output[distinct_position : next_bracket]
                
                # Do the creation of the distinct
                target_column = str(extract_distinct.split("DISTINCT ")[1]).strip()
                
                # Create distinct_name
                distinct_name = "distinct_" + str(distinct_tracker)
                # Append to instructions
                statement = str(prev_df) + "['" + distinct_name + "'] = " + str(prev_df) + "." + str(target_column) + ".unique()"
                instructions.append(statement)
                
                # Replace in self.output the extract_distinct with the new distinct_name
                if is_tuple == True:
                    self.output[i] = (self.output[i][0].replace(extract_distinct, distinct_name), self.output[i][1])
                    
                    # Add to distinct_replaces
                    distinct_replaces[self.output[i][0]] = original_look
                else:
                    self.output[i] = self.output[i].replace(extract_distinct, distinct_name)
                    
                    # Add to distinct_replaces
                    distinct_replaces[self.output[i]] = original_look
                
                # Increment distinct_tracker
                distinct_tracker = chr(ord(distinct_tracker) + 1)
            """
            
            if "CASE WHEN" and "THEN" and "ELSE" in current_output:
                original_look = current_output
                # Find position of CASE
                case_position = current_output.find("CASE")
                # Check that character before is open bracket
                if current_output[case_position - 1] != "(":
                    raise ValueError("Unexpectedly formatted CASE")
                else_position = current_output.find("ELSE", case_position)
                next_bracket = current_output.find(")", else_position)
                
                # Extract the entire case statement
                extract_case = current_output[case_position : next_bracket]
                # Do case aggregation
                if treeHelper.use_numpy == True:
                    # Numpy case
                    case_string = aggregate_case(extract_case, prev_df, this_df, treeHelper)
                else:
                    # Pandas only
                    case_string = pandas_aggregate_case(extract_case, prev_df)
                
                # Create case_name
                case_name = "case_" + str(case_tracker)
                # Append to instructions
                statement = str(prev_df) + "['" + case_name + "'] = " + case_string
                instructions.append(statement)
                
                case_locations[extract_case] = case_name
                # Replace in self.output the extract_case with the new case_string
                if is_tuple == True:
                    self.output[i] = (self.output[i][0].replace(extract_case, case_name), self.output[i][1])
                    
                    # Add to case_replaces
                    case_replaces[self.output[i][0]] = original_look
                else:
                    self.output[i] = self.output[i].replace(extract_case, case_name)
                    
                    # Add to case_replaces
                    case_replaces[self.output[i]] = original_look
                
                # Increment case_tracker
                case_tracker = chr(ord(case_tracker) + 1)
                
        # Out of self.output determine which of these are complex aggregations
        # I.e. ones like: 'sum(l_extendedprice * (1 - l_discount))'
        # This is complex because it uses more than one column
        # We want to perform the inner part of this, the column multiplication
        # Prior to grouping
        
        # Note: We decide to let the "before" aggregations happen to the previous_df
        before_group, during_group, after_group = handle_complex_aggregations(self, self.output, codeCompHelper, treeHelper, prev_df, this_df, case_replaces)
        
        # Combine after_filter and before_filter
        if hasattr(self, "filter"):
            # Only append if not already present
            for b_filt in before_filter:
                if b_filt not in before_group:
                    before_group.append(b_filt)
                    
            # Only append if not already present
            for d_filt in during_filter:
                if d_filt not in during_group:
                    during_group.append(d_filt)
            
            # Only append if not already present
            for a_filt in after_filter:
                if a_filt not in after_group:
                    # TODO: Determine answer: raise ValueError("Should we be doing this?")
                    # At the moment this is adding information we need, but we could do without if we were smarter
                    # We could edit filter_steps
                    
                    # Unspool a_filt
                    column_name, process = a_filt
                    # We search after_group[1] for the process,
                    if process in [i[1] for i in after_group]:
                        # The process is already happening
                        # If we find it that means the process is already happening
                        # Therefore we can get the column_name from after_group
                        after_group_column_name = [i[0] for i in after_group if i[1] == process]
                        # Validate after_group_column_name
                        if len(after_group_column_name) > 1:
                            raise ValueError("Incorrect searching")
                        else:
                            after_group_column_name = after_group_column_name[0]
                            
                        # And put that in filter_steps
                        # Iterate through filter_steps
                        for i in range(len(filter_steps)):
                            # filter_steps[i]
                                # 0 - name
                                # 1 - type
                                # 2 - amount                                
                            
                            # Check if used by bracket replaces
                            if hasattr(codeCompHelper, "bracket_replace"):
                                if codeCompHelper.bracket_replace.get(filter_steps[i][0], None) != None:
                                    filter_steps[i][0] = codeCompHelper.bracket_replace.get(filter_steps[i][0])
                            
                            # If we find the column name, that's equal to the one from the filter originally
                            # Then replace it with the new one
                            if filter_steps[i][0] == column_name:
                                filter_steps[i][0] = after_group_column_name
                                # And stop iterating
                                break
                    else:
                        # The process isn't already happening, so we have to add it
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
                
            
            instructions.append(prev_df + "['" + before_name + "'] = " + before_command + "")
        
        # Handle group
        instructions.append(this_df + " = " + prev_df + " \\")
        # Use the "sort=False", so that groupby doesn't change the sorting of groups themselves
        instructions.append("    .groupby(" + str(self.group_key) + ", sort=" + str(treeHelper.groupby_fusion) + ") \\")
        
        # Incase we aren't being given any aggregation operations
        if during_group == []:
            instructions.append("    .last()")
        else:
            instructions.append("    .agg(")
        
        # Handle During
        #if aggr_type == "count" and inner == "*":
        #   after_aggrs.append([col[1], "len(s.index)", aggr_type])
        for during_name, during_col, during_operation in during_group:   
            # Remove prev_df "." from after_col
            if str(prev_df+".") in during_col:
                during_col = during_col.replace(str(prev_df+"."), "").strip()
                
            # Trim brackets from start and end of after_col
            if (during_col[0] == "(") and (during_col[-1] == ")"):
                during_col = during_col[1:-1]
                      
            # Handle brackets in output
            is_complex, new_name = complex_name_solve(during_name)
            if is_complex == True:
                # Replace these
                codeCompHelper.add_bracket_replace(during_name, new_name)
                # Set to after_name for use now
                during_name = new_name
            
            if during_operation == "sum":
                instructions.append('        ' + during_name + '=("' + during_col + '", "' + during_operation + '"),')
            elif during_operation == "avg":
                instructions.append('        ' + during_name + '=("' + during_col + '", "mean"),')
            elif during_operation == "mean":
                instructions.append('        ' + during_name + '=("' + during_col + '", "mean"),')
            elif during_operation == "count":
                instructions.append('        ' + during_name + '=("' + during_col + '", "count"),')
            elif during_operation == "min":
                instructions.append('        ' + during_name + '=("' + during_col + '", "min"),')
            elif during_operation == "max":
                instructions.append('        ' + during_name + '=("' + during_col + '", "max"),')
            elif during_operation == "distinct":
                instructions.append('        ' + during_name + '=("' + during_col + '", lambda x: x.unique()),')
            elif during_operation == "count_distinct":
                instructions.append('        ' + during_name + '=("' + during_col + '", lambda x: x.nunique()),')
            elif during_operation == "last":
                instructions.append('        ' + during_name + '=("' + during_col + '", "last"),')
            else:
                raise ValueError("Operation: " + str(during_operation) + " not recognised!")
            
        # Add closing bracket, only if we have added during operations
        if during_group != []:
            instructions.append("    )")
        
        # Use the after_group
        for after_name, after_command in after_group:
            # TODO: We have to insert this_df in the after_command
            # Maybe we can do this in the replacement function of expr_tree
            if (after_name == "") and ("index" in after_command):
                # Do without a column, for index rename
                # df_group_1.index = df_group_1.index.rename('customer')
                instructions.append(this_df + ".index = " + after_command + "")
            else:
                instructions.append(this_df + "['" + after_name + "'] = " + after_command + "")
            
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
        
        # Choose aliases
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Check output_cols for bracket replaces
        for i in range(len(output_cols)):
            if codeCompHelper.bracket_replace.get(output_cols[i], None) != None:
                # We make our replacement
                output_cols[i] = codeCompHelper.bracket_replace.get(output_cols[i], None)
        
        # Limit to output columns
        if output_cols != [] and codeCompHelper.column_limiting:
            statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
            instructions.append(statement2_string)
            
        # This is a group_aggregate node
        # Therefore, all self.output with aliases must be in use
        # Therefore we can add these to the codeCompHelper use Alias
        for i in range(len(self.output)):
            # Only add if it has an alias
            if isinstance(self.output[i], tuple):
                # This column might have been involved in a distinct_replace
                if distinct_replaces.get(self.output[i][0], None) != None:
                    # This is in distinct replaces
                    # Add to useAlias
                    codeCompHelper.useAlias[distinct_replaces.get(self.output[i][0], None)] = self.output[i][1]
                
                # Check it's not already in there, raise an Error if it is
                if codeCompHelper.useAlias.get(self.output[i][0], None) == None:
                    # Add to useAlias
                    codeCompHelper.useAlias[self.output[i][0]] = self.output[i][1]
                else:
                    # Already in useAlias
                    raise ValueError("We are trying to add an Alias to useAlias, that is already in there!")
        
        # Add case_replaces into codeCompHelper bracket replace
        codeCompHelper.bracket_replace.update(case_locations)
        
        return instructions   
    
def complex_name_solve(in_name):
    complex_items = ["*", "-", "(", ")", " ", "/", "+", "-", "*", "."]
    
    is_complex = False
    new_name = None
    
    # If in_name is part of complex_item
    if in_name in complex_items:
        return is_complex, in_name
    
    
    if any(item in in_name for item in complex_items):
        # We have one of these items in our string
        is_complex = True
        new_name = in_name
        for item in complex_items:
            new_name = new_name.replace(item, "")
            
        # To lower, after all replacements made
        new_name = new_name.lower()
    
    # Can't start with numbers
    if new_name != None:
        new_name = new_name.lstrip(digits)
    
        # Catch bad changes made to new_name
        if (new_name == "") or (new_name == None):
            raise Exception("Name created by complex_name_solve is empty or None. This could be because it contained only digits and we removed these.")
        
        if "strslice" in new_name:
            new_name = str("slice_" + str(new_name.split("strslice")[0]).strip()).strip()
    
    # returns
    #   is_complex  -  True or False as to whether complex
    #   new_name  -  The name that we have after this function has done it's magic
    return is_complex, new_name
    
class merge_node():
    def __init__(self, condition, output, join, sort=False, filters=None):
        self.condition = condition
        self.output = output
        self.join_type = join
        self.sort = sort
        self.filter = filters

    def set_nodes(self, nodes):
        self.nodes = nodes
        
    def process_equating(self, cond):
        # Split into left and right
        if (" = " in cond):
            split_cond = str(cond).split(" = ")
        elif (" <> " in cond):
            split_cond = str(cond).split(" <> ")
        else:
            raise Exception("Unknown equating operator in process_equating of the merge node")
        
        if "." in split_cond[0]:
            left_side = str(split_cond[0]).split(".")
            left_table = str(left_side[0])
            left_cond = str(left_side[1])
        else:
            left_cond = str(split_cond[0])
            
        if "." in split_cond[1]:
            right_side = str(split_cond[1]).split(".")
            right_table = str(right_side[0])
            right_cond = str(right_side[1])
        else:
            right_cond = str(split_cond[1])
        
        # Return left_cond and right_cond
        return left_cond, right_cond
    
    def is_valid_before_join_filter(self):
        # If filter has AND or OR in it, skip it for now
        if (" AND " in self.filter) or (" OR " in self.filter):
            return False
        else:
            # If the filter refers to two different relations
            connector = None
            if (" = " in self.filter):
                connector = " = "
            elif (" <> " in self.filter):
                connector = " <> "
            else:
                raise Exception("Couldn't detect connector between merge join filter correctly")
            
            # Strip outer brackets
            local_filter = self.filter
            if (local_filter[0] == "(") and (local_filter[-1] == ")"):
                local_filter = local_filter[1:-1]
            
            detected_relations = []    
            split_l_f = local_filter.split(connector)
            for i in range(len(split_l_f)):
                detected_relations.append(str(split_l_f[i].split(".")[0]).strip())
                
            # Validate length and check aren't equal
            if len(detected_relations) != 2:
                raise Exception("Didn't detect enough relations!")
            
            if detected_relations[0] != detected_relations[1]:
                return True
            else:
                return False

    def process_condition_into_merge(self, left_prev_df, right_prev_df, this_df, codeCompHelper):
        # Strip brackets
        self.condition = clean_extra_brackets(self.condition)
        
        # Split down into a list
        if " AND " in self.condition:
            self.condition = str(self.condition).split(" AND ")
            # Strip down to clean it
            for i in range(len(self.condition)):
                self.condition[i] = clean_extra_brackets(self.condition[i].strip())
        else:
            self.condition = [self.condition]
            
        # Iterate through the conditions, try to match on CodeCompHelper Bracket Replace
        # We try and squeeze in any bracket replaces we might have
        
        # Check if we have any bracket replaces
        if (codeCompHelper.bracket_replace != {}) and (self.condition != ['']):
            for i in range(len(self.condition)):
            # Create a relation removed version
            # If bracket replace is not empty
                # Strip outer brackets if exist 
                bracket_replace_params = self.condition[i]
                if (bracket_replace_params[0] == "(") and (bracket_replace_params[-1] == ")"):
                    bracket_replace_params = bracket_replace_params[1:-1]
                
                # Split into keys, on Top level brackets
                keys = [match.group() for match in regex.finditer(r"(?:(\((?>[^()]+|(?1))*\))|\S)+", bracket_replace_params)]
                
                # Populate replace_dict
                replace_dict = {}
                
                for key in keys:
                    process_key = key
                    # For each key, remove relations
                    capture_relation = None
                    for relation in codeCompHelper.relations:
                        if relation in process_key:
                            process_key = process_key.replace(relation + ".", "")
                            capture_relation = relation + "."
                            
                    # Remove extraneous brackets if needed
                    while (process_key[0] == "(") and (process_key[-1] == ")"):
                        process_key = process_key[1:-1]
                    
                    replaced = False
                    # See is is in bracket replace
                    for br_key in codeCompHelper.bracket_replace:
                        if br_key == process_key:
                            process_key = codeCompHelper.bracket_replace[br_key]
                            replaced = True
                        
                    # If is add to replace dict    
                    if replaced == True:
                        # Add in relation
                        if capture_relation == None:
                            raise Exception("No relation has been captured at this point, unexpected!")
                        replace_dict[key] = str(capture_relation) + str(process_key)
                        
                
                # At end, carry out replace dict on params
                for key in replace_dict.keys():
                    self.condition[i] = self.condition[i].replace(key, replace_dict[key])
            
        # Iterate through the list of conditions, adding them to this list
        if self.join_type.lower() != "cross":
            left_labels, right_labels = [], []
            for individual_cond in self.condition:
                left, right = self.process_equating(individual_cond)
                left_labels.append(left)
                right_labels.append(right)
            
        # Create statement
        statements = []
            
        using_join_filter = False
        supported_before_join_filter_types = ["semi", "anti"]
        # Get the filter here and prepare it
        if hasattr(self, "filter") and (self.filter != None) and (self.join_type.lower() in supported_before_join_filter_types):
            if self.is_valid_before_join_filter() == True:
                # Set using join_filter so we know we need to use the intermediate dataframe in the join
                using_join_filter = True
                
                # Time to Join Filter
                if (self.filter[0] == "(") and (self.filter[-1] == ")"):
                    self.filter = self.filter[1:-1]  
                    
                join_left, join_right = self.process_equating(individual_cond)
                
                # inner_cond = df_merge_2.merge(df_filter_5, left_on='l_orderkey', right_on='l_orderkey', how='inner')
                local_statement = "inner_cond = " + str(left_prev_df) + ".merge(" + str(right_prev_df) + ", left_on='" + str(join_left) + "', right_on='" + str(join_right) + "', how='inner', sort=" + str(self.sort) + ")"
                statements.append(local_statement)
                
                # Get filter left and right
                before_filter_left, before_filter_right = self.process_equating(self.filter)
                # Add prefixes to these if they're equal
                if before_filter_left == before_filter_right:
                    before_filter_left = before_filter_left + "_x"
                    before_filter_right = before_filter_right + "_y"
                    
                # Determine equating operator
                equating_operator = None
                if " <> " in self.filter:
                    equating_operator = "!="
                elif " = " in self.filter:
                    equating_operator = "=="
                else:
                    raise Exception("Unable to determine correct equating operator")
                
                # inner_cond = inner_cond[inner_cond.l_suppkey_x != inner_cond.l_suppkey_y]['l_orderkey']
                local_statement = "inner_cond = inner_cond[inner_cond." + str(before_filter_left) + " " + str(equating_operator) + " inner_cond." + str(before_filter_right) + "]['" + join_left + "']" 
                statements.append(local_statement)
                
                # Set self.filter to None
                self.filter = None
                
        # Check if we need to undo an index
        # If we have a group_aggr_node below, then we need to put a statement in saying
        # To reset the index to a standard column
        if not hasattr(self, "nodes"):
            raise Exception("Merge node with no children")
        elif len(self.nodes) < 1:
            raise Exception("Merge node with not enough children")
        elif len(self.nodes) > 2:
            raise Exception("Merge node with too many children")
        
        # Iterate through to discover
        for i in range(len(self.nodes)):
            if self.nodes[i].__class__.__name__ == "group_aggr_node":
                # df_group_1 = df_group_1.reset_index(level=0)
                using_df = None
                if i == 0:
                    # Create for left_node
                    using_df = left_prev_df
                elif i == 1:
                    using_df = right_prev_df
                else:
                    raise Exception("Unexpected number of iterations")
                    
                statements.append(using_df + " = " + using_df + ".reset_index(level=0)")
            
        if self.join_type.lower() == "semi":
            if (len(left_labels) > 1) or (len(right_labels) > 1):
                raise ValueError("Unexpected size of labels in Semi Join merge")
            # Add support for a semi join
            
            if using_join_filter == True:
                # df_merge_5 = df_merge_4[df_merge_4.o_orderkey.isin(inner_cond)]
                
                # Overwrite right_prev_df
                right_prev_df = "inner_cond"
                
                local_statement = this_df + " = " + left_prev_df + '[' + left_prev_df + '.' + str(left_labels[0]) + '.isin(' + right_prev_df + ')]'
                statements.append(local_statement)
            else:
                # df_merge_1 =  df_filter_1[df_filter_1.o_orderkey.isin(df_filter_2["l_orderkey"])]
                local_statement = this_df + " = " + left_prev_df + '[' + left_prev_df + '.' + str(left_labels[0]) + '.isin(' + right_prev_df + '["' + str(right_labels[0]) + '"])]'
                statements.append(local_statement)
        elif self.join_type.lower() == "left":
            # Handle join_filter
            if using_join_filter == True:
                raise Exception("We shouldn't have done an join_filter for an left join")
            
            local_statement = this_df + " = " + left_prev_df+'.merge(' + right_prev_df+', left_on='+str(left_labels)+', right_on='+str(right_labels)+', how="' + str('left') + '", sort=' + str(self.sort) + ')'
            statements.append(local_statement)
        elif self.join_type.lower() == "right":
            # Handle join_filter
            if using_join_filter == True:
                raise Exception("We shouldn't have done an join_filter for an right join")
            
            local_statement = this_df + " = " + left_prev_df+'.merge(' + right_prev_df+', left_on='+str(left_labels)+', right_on='+str(right_labels)+', how="' + str('right') + '", sort=' + str(self.sort) + ')'
            statements.append(local_statement)
        elif self.join_type.lower() == "inner":
            # Handle join_filter
            if using_join_filter == True:
                raise Exception("We shouldn't have done an join_filter for an inner join")
            
            local_statement = this_df + " = " + left_prev_df+'.merge(' + right_prev_df+', left_on='+str(left_labels)+', right_on='+str(right_labels)+', how="' + str('inner') + '", sort=' + str(self.sort) + ')'
            statements.append(local_statement)
        elif self.join_type.lower() == "anti":
            # Do an anti_join
            
            if using_join_filter == True:
                # Overwrite right_prev_df
                right_prev_df = "inner_cond"
                
            #df_merge_1 = df_sort_1.merge(df_filter_3, left_on=['c_custkey'], right_on=['o_custkey'], how='outer', indicator=True)
            local_statement_1 = this_df + " = " + left_prev_df+'.merge(' + right_prev_df+', left_on='+str(left_labels)+', right_on='+str(right_labels)+', how="' + str('outer') + '", indicator=True, sort=' + str(self.sort) + ')'
            statements.append(local_statement_1)
            #df_merge_1 = df_merge_1[df_merge_1._merge == "left_only"]
            local_statement_2 = this_df + " = " + this_df + '[' + this_df + '._merge == "left_only"]' 
            statements.append(local_statement_2)
        elif self.join_type.lower() == "cross":
            # Do a cross_join
            local_statement = this_df + " = " + left_prev_df+'.merge(' + right_prev_df+', how="' + str('cross') + '", sort=' + str(self.sort) + ')'
            statements.append(local_statement)
        
        else:
            raise Exception("Unexpected type of Join given: " + str(self.join_type))
        
        return statements

    def to_pandas(self, prev_dfs, this_df, codeCompHelper, treeHelper): 
        # Overwrite self.sort to be the value of treeHelper.merge_fusion
        self.sort = treeHelper.merge_fusion
        
        # Pandas Merge resets the index
        # See More: https://datacomy.com/data_analysis/pandas/merge/#how-to-keep-index-when-using-pandas-merge
        codeCompHelper.indexes = []
               
        # Check if there's an extract in any of these of the self.outputs
        extract_present = "No Extract"
        for i in range(len(self.output)):
            if "EXTRACT" in self.output[i]:
                extract_present = i
        # Check if there's a substring in any of the self.outputs
        substring_present = "No Substring"
        for i in range(len(self.output)):
            if "SUBSTRING" in self.output[i]:
                substring_present = i
        
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        
        # Set prefixes
        if not isinstance(prev_dfs, list):
            raise ValueError("Inputted prev_df is not a list!")
        elif len(prev_dfs) != 2:
            raise ValueError("Too few previous dataframes specified")
        instructions = []
        
        merge_statements = self.process_condition_into_merge(prev_dfs[0], prev_dfs[1], this_df, codeCompHelper)
        for individual_statement in merge_statements:
            instructions.append(individual_statement)
        
        # Handle extract present
        if extract_present != "No Extract":
            # Output the extract at the given index, stored in extract_present
            if isinstance(self.output[extract_present], tuple):
                statement = str(this_df) + "['" + str(self.output[extract_present][1]) + "'] = " + this_df + "." + str(self.output[extract_present][0])
                instructions.append(statement)
                # Overwrite self.output, so that this element in not a tuple
                # Set in codeComp useAlias, to track when we should use the alias
                codeCompHelper.useAlias[self.output[extract_present][0]] =  self.output[extract_present][1]
                # But instead set to it's column output
                self.output[extract_present] = self.output[extract_present][1]
            else:
                raise ValueError("Extract does not have something we'd like to set it to")
        # Handle substring present
        if substring_present != "No Substring":
            # Output the substring at the given index, stored in substring_present
            if isinstance(self.output[substring_present], tuple):
                statement = str(this_df) + "['" + str(self.output[substring_present][1]) + "'] = " + this_df + "." + str(self.output[substring_present][0])
                instructions.append(statement)
                # Overwrite self.output, so that this element in not a tuple
                # Set in codeComp useAlias, to track when we should use the alias
                codeCompHelper.useAlias[self.output[substring_present][0]] =  self.output[substring_present][1]
                # But instead set to it's column output
                self.output[substring_present] = self.output[substring_present][1]
            else:
                # Output has no column name
                # Use solve_complex_name
                
                is_complex, new_name = complex_name_solve(self.output[substring_present]) 
                if is_complex == True:
                    statement = str(this_df) + "['" + str(new_name) + "'] = " + this_df + "." + str(self.output[substring_present])
                    instructions.append(statement)
                    # Overwrite self.output, so that this element in not a tuple
                    # Set in codeComp useAlias, to track when we should use the alias
                    codeCompHelper.useAlias[self.output[substring_present]] =  new_name
                    # But instead set to it's column output
                    self.output[substring_present] = new_name
                else:
                    raise Exception("String that was meant to be complex apparently isn't   ")
                
                # raise ValueError("Substring does not have something we'd like to set it to")
        
        # Process aliases here
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Clean params
        if hasattr(self, "filter") and self.filter != None:
            self.filter = clean_filter_params(self, self.filter, codeCompHelper, this_df)
        
        # After merge, we filter if we have some
        if self.filter != None:
            # We need to replace any relation name with this_df, use codeComp to know these
            for relation in codeCompHelper.relations:
                self.filter = self.filter.replace(relation+".", this_df+".")
            statement = this_df + " = " + this_df + "[" + str(self.filter) + "]"
            instructions.append(statement)
        
        
        # Limit to output columns
        if codeCompHelper.column_limiting and (output_cols != []):
            statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
            instructions.append(statement2_string)
            
        # Do renames
        if hasattr(self, "renames"):
            if (self.renames != []) and (self.renames != None):
                for original, new in self.renames:
                    rename_string = this_df + "['" + str(new) + "'] = " + this_df + "['" + str(original) + "']"
                    instructions.append(rename_string)
            
        return instructions

class aggr_node():
    def __init__(self, output):    
        self.output = output

    def set_nodes(self, nodes):
        self.nodes = nodes

    def to_pandas(self, prev_df, this_df, codeCompHelper, treeHelper):
        
        instructions = []
        
        for i in range(len(self.nodes)):
            if self.nodes[i].__class__.__name__ == "group_aggr_node":
                # df_group_1 = df_group_1.reset_index(level=0)
                using_df = None
                if i == 0:
                    # Create for left_node
                    using_df = prev_df
                else:
                    raise Exception("Unexpected number of iterations")
                    
                instructions.append(using_df + " = " + using_df + ".reset_index()")
                
                # Reset indexes
                codeCompHelper.indexes = []
        
        # Process output:
        self.output = process_output(self, self.output, codeCompHelper)
        
        # Set prefixes
        if not isinstance(prev_df, str):
            raise ValueError("Inputted prev_df is not a string!")
        
        # TODO: We need to add in CASE handling
        # Track the replaces of cases
        # DICT: [[original_look, new_look], ...]
        case_replaces = {}
        case_tracker = "a"
        case_locations = {}
        # Check for cases
        # TODO: Change to output_cols
        for i in range(len(self.output)):
            # Don't choose the alias
            is_tuple = False
            if isinstance(self.output[i], tuple):
                current_output = self.output[i][0]
                is_tuple = True
            else:
                current_output = self.output[i]
            
            if "CASE WHEN" and "THEN" and "ELSE" in current_output:
                original_look = current_output
                # Find position of CASE
                case_position = current_output.find("CASE")
                else_position = current_output.find("ELSE", case_position)
                end_position = current_output.find(" END", else_position) + len(str(" END"))
                
                # Extract the entire case statement
                extract_case = current_output[case_position : end_position]
                # Do case aggregation
                if treeHelper.use_numpy == True:
                    # Numpy case
                    case_string = aggregate_case(extract_case, prev_df, this_df, treeHelper)
                else:
                    # Pandas only
                    case_string = pandas_aggregate_case(extract_case, prev_df)
                
                # Create case_name
                case_name = "case_" + str(case_tracker)
                # Append to instructions
                statement = str(prev_df) + "['" + case_name + "'] = " + case_string
                instructions.append(statement)
                
                case_locations[extract_case] = case_name
                # Replace in self.output the extract_case with the new case_string
                if is_tuple == True:
                    self.output[i] = (self.output[i][0].replace(extract_case, case_name), self.output[i][1])
                    
                    # Add to case_replaces
                    case_replaces[self.output[i][0]] = original_look
                else:
                    self.output[i] = self.output[i].replace(extract_case, case_name)
                    
                    # Add to case_replaces
                    case_replaces[self.output[i]] = original_look
                
                # Increment case_tracker
                case_tracker = chr(ord(case_tracker) + 1)
        
        # Append the creation of the dataframe that we're going to add data into
        instructions.append(this_df + " = pd.DataFrame()")
        
        # Check for intermediate results
        # Iterate multiple times
        iter_count = 0
        while iter_count < 3:
            for i in range(len(self.output)):
                if isinstance(self.output[i], tuple):
                    # Iterate through keys in bracket_replace
                    for j in range(len(list(codeCompHelper.bracket_replace.keys()))):
                        if (list(codeCompHelper.bracket_replace.keys())[j].lower() in self.output[i][0]) and (list(codeCompHelper.bracket_replace.keys())[j] != self.output[i][0]):
                            # We have a bracket replace that's inside a columnar output and not identical to it,
                            # So we do a replace on the self.output - form a new tuple
                            self.output[i] = (self.output[i][0].replace(list(codeCompHelper.bracket_replace.keys())[j].lower(), list(codeCompHelper.bracket_replace.values())[j]), self.output[i][1])
                    # Iterate through keys in useAlias
                    for j in range(len(list(codeCompHelper.useAlias.keys()))):
                        if (list(codeCompHelper.useAlias.keys())[j].lower() in self.output[i][0]) and (list(codeCompHelper.useAlias.keys())[j] != self.output[i][0]):
                            # We have a bracket replace that's inside a columnar output and not identical to it,
                            # So we do a replace on the self.output - form a new tuple
                            self.output[i] = (self.output[i][0].replace(list(codeCompHelper.useAlias.keys())[j].lower(), list(codeCompHelper.useAlias.values())[j]), self.output[i][1])
                    
                else:
                    # Iterate through keys in bracket_replace
                    for j in range(len(list(codeCompHelper.bracket_replace.keys()))):
                        if (list(codeCompHelper.bracket_replace.keys())[j] in self.output[i]) and (list(codeCompHelper.bracket_replace.keys())[j] != self.output[i]):
                            # We have a bracket replace that's inside a columnar output and not identical to it,
                            # So we do a replace on the self.output - form a new tuple
                            self.output[i] = self.output[i].replace(list(codeCompHelper.bracket_replace.keys())[j], list(codeCompHelper.bracket_replace.values())[j])   
                    # Iterate through keys in useAlias
                    for j in range(len(list(codeCompHelper.useAlias.keys()))):
                        if (list(codeCompHelper.useAlias.keys())[j] in self.output[i]) and (list(codeCompHelper.useAlias.keys())[j] != self.output[i]):
                            # We have a bracket replace that's inside a columnar output and not identical to it,
                            # So we do a replace on the self.output - form a new tuple
                            self.output[i] = self.output[i].replace(list(codeCompHelper.useAlias.keys())[j], list(codeCompHelper.useAlias.values())[j])   
                    
                    
            iter_count += 1
        
        instructions += do_aggregation(self, prev_df, this_df, codeCompHelper, treeHelper)
        
        output_cols = choose_aliases(self, codeCompHelper)
        
        # Limit to output columns
        if codeCompHelper.column_limiting and (output_cols != []):
            statement2_string = this_df + " = " + this_df + "[" + str(output_cols) + "]"
            instructions.append(statement2_string)
                
        # Add case_replaces into codeCompHelper bracket replace
        codeCompHelper.bracket_replace.update(case_locations)
            
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
        if codeCompHelper.indexes != []:
            instructions.append(prev_df + " = " + prev_df + ".rename_axis(" + str(codeCompHelper.indexes) + ").reset_index()")
        
        # Create the current dataframe
        instructions.append(this_df + " = pd.DataFrame()")
        
        # Clear codeCompHelper indexes
        codeCompHelper.indexes = []
        
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
        
        # If prev_output are tuples, take the 1st index
        for i in range(len(prev_output)):
            if isinstance(prev_output[i], tuple):
                prev_output[i] = prev_output[i][1]
        
        # Check that we have the correct number of columns for a 1-to-1 mapping between output_cols and prev_output
        if len(output_cols) != len(prev_output):
            raise ValueError("Incorrect number of columns collected")
        
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
        
        # Process column_references, discover nested ones, and insert new ones
        self.fix_column_references()
        
        # Value of limit
        # TODO: Hardcoded to a single limit statement, what about multiple?
        self.limit = self.get_limit()
        
    def fix_column_references(self):
        # Iterate through values and keys
        orig_dict_values = list(self.column_references.values())
        orig_dict_keys = list(self.column_references.keys())
        for i in range(len(orig_dict_values)):
            # New dictionary values
            new_dict_values = []
            for j in range(len(list(self.column_references.keys()))):
                # If the value is inside this key
                if orig_dict_values[i] in list(self.column_references.keys())[j]:
                    get_key = list(self.column_references.keys())[j]
                    # Replace the value with the key for that value
                    replace_key = get_key.replace(orig_dict_values[i], orig_dict_keys[i])
                    # Get the value for this new replace_key
                    replace_key_value = list(self.column_references.values())[j]
                    
                    # Add to new_dict_values
                    new_dict_values.append((replace_key, replace_key_value))
            
            # Add in new dictionary values
            for key, value in new_dict_values:
                self.column_references[key] = value
        
    def get_limit(self):
        local_file = self.file_content.lower()
        if "limit" in local_file.lower():
            limit_amount = local_file.split("limit")[1].split(";")[0].strip()
            # for limit in parse_one(self.file_content).find_all(exp.Limit):
            #     limit_amount = int(limit.expression.alias_or_name)
        else:
            limit_amount = None
        return limit_amount
        
    def get_col_refs(self):
        column_references = dict()
        # Get column references for whole file
        self.read_sql_for_col_refs(self.file_content, column_references)

        # Manual mode for create views
        # Assume at the top
        if self.file_content[:11] == "create view":
            view_string = self.file_content.split(";")[0]
            references = view_string.split("(")[1].split(")")[0].split(",")
            for j in range(len(references)):
                references[j] = references[j].strip()
            
            self.read_sql_for_col_refs(view_string, column_references, references)
        
        return column_references
        
    def read_sql_file_for_information(self, sql_file):
        f = open(sql_file, "r")
        file = f.read()
        file = ' '.join(file.split())
        return file

    def read_sql_for_col_refs(self, sql_content, col_dict, references=None):
        current_from_references = 0
        for select in parse_one(sql_content).find_all(exp.Select):
            for projection in select.expressions:
                # If alias == "", substitute in for references
                projection_alias = str(projection.alias_or_name)
                
                if ((str(projection) == projection_alias) or (projection_alias == "")) and (references != None) and (current_from_references < len(references)):
                    projection_alias = references[current_from_references]
                    current_from_references += 1
                
                if (str(projection) != projection_alias) and (projection_alias != ""):
                    # Do we have an alias, remove if we do
                    if "as" in str(projection).lower():
                        # Remove as
                        projection_original = str(" ".join(str(projection).split()[:-2]).lower()).strip()
                    else:
                        # Keep as the same
                        projection_original = str(projection).strip()
                    
                    # Remove all brackets for comparison purposes
                    projection_original = projection_original.replace("(", "").replace(")", "")
                    if projection_original in col_dict:
                        # Check the existing key is the same as 
                        raise ValueError("We are trying to process a SQL but finding multiple identical projections")
                    else:
                        # Like replacement
                        if "like" in projection_original:
                            projection_original = projection_original.replace("like", "~~")
                        
                        # Get rid of end
                        if ("case" in projection_original) and (" then " in projection_original) and ("end" in projection_original):
                            projection_original = rreplace(projection_original, " end", " ", 1).strip()
                         
                            if "else" in projection_original:
                                projection_original = rreplace(projection_original, " else ", " ", 1).strip()
                            else:
                                # If theres no ELSE in the string, then we should add in "null"
                                projection_original = projection_original + " null"
                            
                        # Get rid of apostrophes
                        if "'" in projection_original:
                            projection_original = projection_original.replace("'", "")
                        
                        # Get rid of "." and relation if present
                        if "." in projection_original:
                            split_proj = projection_original.split(".")
                            # If the first part is all characters that are digits
                            if split_proj[0].isdigit():
                                pass
                            else:
                                # If we have any operators, then we need to preserve all of the projection, but cut out the .
                                if any(x in split_proj[0] for x in ["*", "/", "-", "+"]):
                                    projection_original = projection_original.replace(".", "")
                                else:
                                    # This is meant to catch a scenario like:
                                        # n2.n_name
                                        # And turn it into: n_name
                                    # projection_original = str(split_proj[1]).strip()
                                    col_dict[projection_original.split(".")[1]] = projection_alias
                                    
                                    # Add also the split after the dot of it
                                    #if projection.alias_or_name != "":
                                        # Don't do this, make the functions above start enough to recognise with the alias
                                        # column_references[projection_original.split(".")[1].strip()] = str(projection.alias_or_name)
                                    #    pass
                                    
                                    # This doesn't work, instead we need to keep the "n1" part to differentiate it
                                    projection_original = projection_original.replace(".", "")
                         
                        # If comma in, get rid of comma
                        if ("," in projection_original):
                            projection_original = projection_original.replace(",", "")
                        
                        if "SUM" in projection_original:
                            projection_original = projection_original.replace("SUM", "sum")
                            
                        if "." in projection_alias:
                            local_proj_al = str(projection_alias).replace(".", "")
                            if all([True for char in local_proj_al if char.isdigit()]):
                                continue
                           
                        col_dict[projection_original] = projection_alias
        

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
        node_class = aggr_node(current_node.output)
    elif node_type == "Seq Scan":
        node_class = filter_node(current_node.relation_name, current_node.output)
    
        if hasattr(current_node, "filters"):
            # Check if is a filter type of Seq Scan
            node_class.set_params(current_node.filters)
        
        # Add in subplan information
        if hasattr(current_node, "subplan_name"):
            node_class.add_subplan_name(current_node.subplan_name)
            
        # Add in alias
        if hasattr(current_node, "alias"):
            node_class.set_alias(current_node.alias)
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
        # It's a Hash Join, we have Sort = False
        if hasattr(current_node, "filter"):
            node_class = merge_node(current_node.hash_cond, current_node.output, join=current_node.join_type, filters=current_node.filter, sort=False)
        else:
            node_class = merge_node(current_node.hash_cond, current_node.output, join=current_node.join_type, sort=False)
        
        # Handle renames
        if hasattr(current_node, "renames"):
            if (current_node.renames != []) and (current_node.renames != None):
                node_class.renames = current_node.renames
    elif node_type == "Merge Join":
        # It's a Merge Join, we have Sort = True
        if hasattr(current_node, "filter"):
            node_class = merge_node(current_node.merge_cond, current_node.output, join=current_node.join_type, filters=current_node.filter, sort=True)
        else:
            node_class = merge_node(current_node.merge_cond, current_node.output, join=current_node.join_type, sort=True)
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
        
        # Add in alias
        if hasattr(current_node, "alias"):
            node_class.set_alias(current_node.alias)
    elif node_type == "Subquery Scan":
        # Make a Subquery Scan into a "rename node"
        node_class = rename_node(current_node.output, current_node.alias)
    elif node_type == "Index Only Scan":
        # Make an index scan into a filter node
        node_class = filter_node(current_node.relation_name, current_node.output)
            
        if hasattr(current_node, "filters"):
            # Check if is a filter type of Seq Scan
            node_class.set_params(current_node.filters)
        elif hasattr(current_node, "filter"):
            # Check if is a filter type of Seq Scan
            node_class.set_params(current_node.filter)
        
        # Add in alias
        if hasattr(current_node, "alias"):
            node_class.set_alias(current_node.alias)
    elif node_type == "Bitmap Heap Scan":
        node_class = filter_node(current_node.relation_name, current_node.recheck_cond, current_node.output)
         
        # Add in alias
        if hasattr(current_node, "alias"):
            node_class.set_alias(current_node.alias)
    elif node_type == "Unique":
        node_class = unique_node(current_node.output)
    elif node_type == "Group":
        node_class = group_aggr_node(current_node.output, current_node.group_key)
        
    else:
        raise ValueError("The node: " + str(current_node.node_type) + " is not recognised. Not all node have been implemented")
    
    if current_node.plans != None:
        current_node_plans = []
        for individual_plan in current_node.plans:
            current_node_plans.append(create_tree(individual_plan, sql_class))
        node_class.set_nodes(current_node_plans)
        
    return node_class
