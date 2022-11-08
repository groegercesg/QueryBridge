from sqlglot import parse_one, exp
import math

def get_columns(query):
    column_references = []
    for select in parse_one(query).find_all(exp.Select):
        for projection in select.expressions:
            column_references.append(projection.alias_or_name)

    return column_references

def compare(query_file, pandas_result, sql_result, decimal_places):
    # Compare Result
    compare_result = True
    
    # Read SQL file
    with open(query_file, 'r') as file:
        sql_query = file.read()
    
    # Preprocessing
    # get columns of SQL
    columns = get_columns(sql_query)
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
        if sql_column[i] == pandas_column[i]:
            # They are Equal, next item
            pass
        elif sql_column[i] != pandas_column[i]:
            # Convert to float
            sql_float = float(sql_column[i])
            if sql_float == pandas_column[i]:
                # They are equal
                pass
            elif truncate(sql_float, decimal_places) == truncate(pandas_column[i], decimal_places):
                # Equal to decimal places
                pass
            else:
                column_equivalent = False
                return column_equivalent
    
    return column_equivalent