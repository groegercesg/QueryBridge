import math
import datetime
import pandas as pd

def get_columns(query):
    # Alternate method
    cut_query = str(str(query.lower()).split("select")[1]).split("from")[0]
    clean_query = str(cut_query.replace("\n", ""))
    split_query = clean_query.split(",")
    for i in range(len(split_query)):
        if " as " in split_query[i]:
            split_query[i] = str(split_query[i]).split(" as ")[1]
        
        split_query[i] = str(split_query[i]).replace(" ", "")
    return split_query

def compare(query_file, pandas_result, sql_result, decimal_places):
    # Compare Result
    compare_result = True
    
    # Read SQL file
    with open(query_file, 'r') as file:
        sql_query = file.read()
    
    # Preprocessing
    # get columns of SQL
    columns = get_columns(sql_query)
    
    # If our SQL Column names have brackets in them
    # These obviously won't appear in Pandas Columns
    # So we just replace these out for our check comparison
    for i in range(len(columns)):
        columns[i] = str(str(columns[i]).replace("(", "")).replace(")", "")
    
    # Convert Pandas indexes to not be indexes, and instead be normal columns
    pandas_result = pandas_result.reset_index()
        
    # Check if same number of columns    
    if len(columns) != len(pandas_result.columns):
        compare_result = False
        return compare_result
        
    # Ensure SQL and Columns have same number
    if len(columns) != len(sql_result[0]):
        compare_result = False
        return compare_result
         
    # Iterate through Columnns of SQL_Result
    # Compare the column to the columns[idx] of pandas_result
    for i in range(len(columns)):
        column_compare = compare_column([row[i] for row in sql_result], pandas_result[columns[i]].tolist(), decimal_places)
        # Only propagate decision forward, if is "False"
        # I.e. Don't give a decision of True, as this will overwrite previous Falses
        if column_compare == False:
            compare_result = False
            return compare_result
        
    
    return compare_result

def truncate(number, digits):
    # Improve accuracy with floating point operations, to avoid truncate(16.4, 2) = 16.39 or truncate(-1.13, 2) = -1.12
    nbDecimals = len(str(number).split('.')[1]) 
    if nbDecimals <= digits:
        return number
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper


def compare_column(sql_column, pandas_column, decimal_places):
    """Function to compare two lists of data for accuracy

    Args:
        sql_column (list): Column of data returned from SQL
        pandas_column (list): Column of data returned from Pandas
    """
    column_equivalent = True
    
    if len(sql_column) != len(pandas_column):
        column_equivalent = False
        return column_equivalent
    
    # Iterate down SQL Column
    for i in range(len(sql_column)):
        if isinstance(sql_column[i], datetime.date) and isinstance(pandas_column[i], pd.Timestamp):
            sql_value = pd.Timestamp(sql_column[i])
            pd_value = pandas_column[i]
        else:
            sql_value = sql_column[i]
            pd_value = pandas_column[i]
            
        
        if sql_value == pd_value:
            # They are Equal, next item
            pass
        elif sql_value != pd_value:
            # Convert to float
            sql_float = float(sql_value)
            if sql_float == pd_value:
                # They are equal
                pass
            elif truncate(sql_float, decimal_places) == truncate(pd_value, decimal_places):
                # Equal to decimal places
                pass
            else:
                column_equivalent = False
                return column_equivalent
    
    return column_equivalent