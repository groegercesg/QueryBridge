import math
import datetime
import pandas as pd
import decimal

def get_columns(query):
    # Alternate method
    if query.count(";") > 1:
        # We have multiple statements in a single file
        remove_queries = []
        many_queries = query.split(";")
        for i in range(len(many_queries)):
            many_queries[i] = many_queries[i].strip()
            many_queries[i] = many_queries[i] + ";"
            if many_queries[i] == ";":
                remove_queries.append(i)
        # Do remove
        for idx in remove_queries.__reversed__():
            del many_queries[idx]
        remove_queries = []
        # Choose which to use
        for i in range(len(many_queries)):
            if ("select" not in many_queries[i]) or ("from" not in many_queries[i]):
                remove_queries.append(i)
        # Do remove
        for idx in remove_queries.__reversed__():
            del many_queries[idx]
        # Choose the last one
        query = many_queries[-1]
    
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
    if len(columns) == len(pandas_result.columns):
        pass
    else:
        if "index" in list(pandas_result.columns) and len(columns) == len(pandas_result.columns) - 1:
            pass
        else:
            print(columns)
            print(len(columns))
            print(pandas_result.columns)
            len(pandas_result.columns)
            compare_result = False
            return compare_result, columns
        
    # Ensure SQL and Columns have same number
    if len(columns) != len(sql_result[0]):
        compare_result = False
        return compare_result, columns
         
    # Iterate through Columnns of SQL_Result
    # Compare the column to the columns[idx] of pandas_result
    for i in range(len(columns)):
        column_compare = compare_column([row[i] for row in sql_result], pandas_result[columns[i]].tolist(), decimal_places)
        # Only propagate decision forward, if is "False"
        # I.e. Don't give a decision of True, as this will overwrite previous Falses
        if column_compare == False:
            compare_result = False
            return compare_result, columns
        
    
    return compare_result, columns

def truncate(number, digits):
    decimal.getcontext().rounding = decimal.ROUND_HALF_UP  # define rounding method
    return decimal.Decimal(str(float(number))).quantize(decimal.Decimal('1e-{}'.format(digits)))
    
    if "." in number:
        split_number = number.split(".")
        if len(split_number[1]) < digits:
            # calculate extra 0s
            extra_zeros = digits - len(split_number[1])
            return float(split_number[0] + "." + split_number[1] + "0" * extra_zeros)
        else:
            return float(split_number[0] + "." + split_number[1][:digits])
    else:
        return float(number)
    
    


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
            sql_value = str(sql_column[i]).strip()
            pd_value = str(pandas_column[i]).strip()
            
        
        if sql_value == pd_value:
            # They are Equal, next item
            pass
        elif sql_value != pd_value:
            # Convert to truncated floats
            sql_trunc = truncate(sql_value, decimal_places) 
            pd_trunc = truncate(pd_value, decimal_places)
            if float(sql_value) == pd_value:
                # They are equal, left float
                pass
            elif sql_value == float(pd_value):
                # They are equal, right float
                pass
            elif float(sql_value) == float(pd_value):
                # They are equal, as floats
                pass
            elif sql_trunc == pd_trunc:
                # Equal to decimal places
                pass
            else:
                column_equivalent = False
                return column_equivalent
    
    return column_equivalent