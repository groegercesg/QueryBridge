from collections import Counter
import datetime
import pandas as pd
import decimal
from tableauhyperapi import Date
from sqlglot import parse_one, exp

def get_columns_v2(query):
    # Use sqlglot, remove views from the query
    if "create view" in query:
        query = query.split(";")[1].strip()
    parsed_query = parse_one(query)
    gatherColumns = []
    top_select = parsed_query.find(exp.Select)
    for projection in top_select.expressions:
        projectionValue = projection.alias_or_name
        if projectionValue == '':
            projectionValue = projection.key
        assert projectionValue != ''
        gatherColumns.append(projectionValue)
    
    return gatherColumns

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
    clean_query = str(cut_query.replace("\n", "").replace("\t", ""))
    split_query = clean_query.split(",")
    for i in range(len(split_query)):
        if " as " in split_query[i]:
            split_query[i] = str(str(split_query[i]).split(" as ")[1]).strip()
            
            if (split_query[i][0] == '"') and (split_query[i][-1] == '"'):
                # If the start and end are quotation marks, strip these
                split_query[i] = split_query[i][1:-1]
        
        split_query[i] = str(split_query[i]).replace(" ", "")
    return split_query

def test_for_all_elements(inList: list, condition):
    pass

def compare(query_file, exec_result, sql_result, decimal_places, order_checking):
    # Compare Result
    compare_result = True
    
    # Read SQL file
    with open(query_file, 'r') as file:
        sql_query = file.read()
    
    # Preprocessing
    # get columns of SQL
    columns = get_columns_v2(sql_query)
    
    # If our SQL Column names have brackets in them
    # These obviously won't appear in Pandas Columns
    # So we just replace these out for our check comparison
    for i in range(len(columns)):
        columns[i] = str(str(columns[i]).replace("(", "")).replace(")", "")
    
    # Convert Pandas indexes to not be indexes, and instead be normal columns
    if isinstance(exec_result, pd.DataFrame):
        exec_result = exec_result.reset_index()
    elif isinstance(exec_result, dict):
        pass
    else:
        raise Exception("Unexpected exec_result format")
        
    # Check if same number of columns
    if isinstance(exec_result, pd.DataFrame):
        if len(columns) == len(exec_result.columns):
            pass
        else:
            if "index" in list(exec_result.columns) and len(columns) == len(exec_result.columns) - 1:
                pass
            else:
                print(columns)
                print(len(columns))
                print(exec_result.columns)
                print(len(exec_result.columns))
                compare_result = False
                return compare_result, columns
    elif isinstance(exec_result, dict):
        if len(columns) == len(exec_result):
            pass
        else:
            print(columns)
            print(len(columns))
            print(exec_result.keys())
            print(len(exec_result))
            compare_result = False
            return compare_result, columns
    else:
        raise Exception("Unexpected exec_result format")
        
    # Ensure SQL and Columns have same number
    if len(columns) != len(sql_result[0]):
        compare_result = False
        return compare_result, columns
         
    # Iterate through Columnns of SQL_Result
    # Compare the column to the columns[idx] of exec_result
    for i in range(len(columns)):
        exec_col = None
        is_sdql = False
        if isinstance(exec_result, pd.DataFrame):
            exec_col = exec_result[columns[i]].tolist()
        elif isinstance(exec_result, dict):
            exec_col = list(exec_result[columns[i]])
            is_sdql = True
            
            # Change the types
            if all([(x.isdigit() and len(x) == 8) for x in exec_col]) and "date" in columns[i]:
                # Cast to datetime.date
                for idx, val in enumerate(exec_col):
                    exec_col[idx] = datetime.datetime.strptime(val, "%Y%m%d").date()
            elif all([(x.count(".") == 1 and exec_col[0].replace(".", "").isdigit()) for x in exec_col]):
                # Cast to float
                for idx, val in enumerate(exec_col):
                    exec_col[idx] = float(val)
            elif all([(x.isdigit()) for x in exec_col]):
                # Cast to float
                for idx, val in enumerate(exec_col):
                    exec_col[idx] = int(val)
            else:
                assert all([isinstance(x, str) for x in exec_col]) == True
        else:
            raise Exception("Unexpected exec_result format")
        
        assert exec_col != None and isinstance(exec_col, list)
        
        if order_checking == True:
            column_compare = compare_column([row[i] for row in sql_result], exec_col, decimal_places)
        else:
            column_compare = compare_column_no_order([row[i] for row in sql_result], exec_col, decimal_places, is_sdql)
        # Only propagate decision forward, if is "False"
        # I.e. Don't give a decision of True, as this will overwrite previous Falses
        if column_compare == False:
            compare_result = False
            return compare_result, columns
    
    return compare_result, columns

def truncate(number, digits):
    decimal.getcontext().rounding = decimal.ROUND_HALF_UP  # define rounding method
    return decimal.Decimal(str(float(number))).quantize(decimal.Decimal('1e-{}'.format(digits)))

import difflib

def get_overlap(s1, s2):
    s = difflib.SequenceMatcher(None, s1, s2)
    pos_a, pos_b, size = s.find_longest_match(0, len(s1), 0, len(s2)) 
    return s1[pos_a:pos_a+size]

def are_there_nones(inList: list) -> bool:
    # Check no nones in a list
    nones = False
    for x in inList:
        if x == None:
            nones = True
            break
    return nones

def sdql_float_counter_compare(sql_counter: Counter, pd_counter: Counter) -> bool:
    things_in_both = sql_counter & pd_counter
    SQL_missing_from_pd = sql_counter - things_in_both
    PD_missing_from_sql = pd_counter - things_in_both
    moving = 0.01
    
    testing_for_close = True
    if testing_for_close == True:
        for key in SQL_missing_from_pd:
            smaller = round(key - moving, 2)
            larger = round(key + moving, 2)
            if ((PD_missing_from_sql[key] > 0) or
                (PD_missing_from_sql[smaller] > 0) or
                (PD_missing_from_sql[larger] > 0)
            ):
                pass
            else:
                testing_for_close = False
                break
    if testing_for_close == True:
        for key in PD_missing_from_sql:
            smaller = round(key - moving, 2)
            larger = round(key + moving, 2)
            if ((SQL_missing_from_pd[key] > 0) or
                (SQL_missing_from_pd[smaller] > 0) or
                (SQL_missing_from_pd[larger] > 0)
            ):
                pass
            else:
                testing_for_close = False
                break
    
    return testing_for_close
        

def compare_column_no_order(sql_column, pandas_column, decimal_places, is_sdql):
    """Function to compare two lists of data for accuracy
    """
    column_equivalent = False
    
    # Check neither list has any nones in it
    assert isinstance(sql_column, list) and isinstance(pandas_column, list)
    assert (are_there_nones(sql_column) == False) and (are_there_nones(pandas_column) == False)
    
    if len(sql_column) != len(pandas_column):
        return column_equivalent
    
    sql_first_type = type(sql_column[0])
    assert(all(isinstance(x, sql_first_type) for x in sql_column))
    pandas_first_type = type(pandas_column[0])
    assert(all(isinstance(x, pandas_first_type) for x in pandas_column))
    
    column_types = set([sql_first_type, pandas_first_type])
    
    # Change decimal places for SDQL
    if is_sdql == True:
        # SDQL can only return 2 decimal points
        decimal_places = 2
    
    if column_types == set([decimal.Decimal, int]):
        # Decimal and Int
        sql_column = from_decimal_to_int(sql_column, decimal_places)
        pandas_column = from_decimal_to_int(pandas_column, decimal_places)
        
        column_equivalent = Counter(sql_column) == Counter(pandas_column)
    elif column_types == set([int, float]):
        # Float and Int
        sql_column = from_decimal_to_float(sql_column, decimal_places)
        pandas_column = from_decimal_to_float(pandas_column, decimal_places)
        
        column_equivalent = Counter(sql_column) == Counter(pandas_column)
        if column_equivalent == False and is_sdql == True:
            column_equivalent = sdql_float_counter_compare(Counter(sql_column), Counter(pandas_column))
    elif column_types == set([decimal.Decimal, float]):
        # Decimal and Float
        sql_column = from_decimal_to_float(sql_column, decimal_places)
        pandas_column = from_decimal_to_float(pandas_column, decimal_places)
        
        column_equivalent = Counter(sql_column) == Counter(pandas_column)
        if column_equivalent == False and is_sdql == True:
            column_equivalent = sdql_float_counter_compare(Counter(sql_column), Counter(pandas_column))
    elif column_types == set([type(1.012)]):
        # Float        
        sql_column = from_decimal_to_float(sql_column, decimal_places)
        pandas_column = from_decimal_to_float(pandas_column, decimal_places)
        
        sql_column_sorted = sorted(sql_column)
        pandas_column_sorted = sorted(pandas_column)
        
        overlaps = []
        for i in range(len(sql_column)):
            overlaps.append(len(get_overlap(str(sql_column_sorted[i]), str(pandas_column_sorted[i]))))
        
        column_equivalent = (Counter(sql_column) == Counter(pandas_column)) or (len(overlaps) == len(list(filter(lambda x: x >= 10, overlaps))))
        if column_equivalent == False and is_sdql == True:
            column_equivalent = sdql_float_counter_compare(Counter(sql_column), Counter(pandas_column))
    elif column_types == set([type(1)]):
        # Int
        
        column_equivalent = Counter(sql_column) == Counter(pandas_column)
    elif column_types == set([type(1), type("STR")]):
        # String and Int
        if sql_first_type == type("STR"):
            # SQL is str
            assert all([x.isdigit() for x in sql_column])
            sql_column = from_str_to_int(sql_column, decimal_places)
        elif pandas_first_type == type("STR"):
            # Pandas is str
            assert all([x.isdigit() for x in pandas_column])
            pandas_column = from_str_to_int(pandas_column, decimal_places)
        else:
            raise Exception("Not possible to reach here")
        
        # Now both strings
        assert len(sql_column) == len(pandas_column)
        for i in range(len(sql_column)):
            sql_column[i] = str(sql_column[i]).strip()
            pandas_column[i] = str(pandas_column[i]).strip()
            
        column_equivalent = Counter(sql_column) == Counter(pandas_column)    
    elif column_types == set([type("STR")]):
        # String
        assert len(sql_column) == len(pandas_column)
        for i in range(len(sql_column)):
            sql_column[i] = str(sql_column[i]).strip()
            pandas_column[i] = str(pandas_column[i]).strip()
        
        column_equivalent = Counter(sql_column) == Counter(pandas_column)
    elif column_types == set([ datetime.date, Date]):
        # Convert hyper date to pd.Timestamp
        # Convert datetime.date to pd.Timestamp
        
        if isinstance(sql_column[0], Date):
            sql_column = from_hyper_date_to_pd_timestamp(sql_column)
        else: 
            raise Exception(f"sql_column was not of type Hyper Date, it was: {type(sql_column[0])}")
        
        if isinstance(pandas_column[0], datetime.date):
            pandas_column = from_datetime_date_to_pd_timestamp(pandas_column)
        else: 
            raise Exception(f"pandas_column was not of type datetime Date, it was: {type(pandas_column[0])}")
        
        column_equivalent = Counter(sql_column) == Counter(pandas_column)
    elif column_types == set([pd.Timestamp, Date]):
        # Timestamp and (Hyper) Date, convert hyper date to pd.Timestamp
        
        if isinstance(sql_column[0], Date):
            sql_column = from_hyper_date_to_pd_timestamp(sql_column)
        else: 
            raise Exception(f"sql_column was not of type Hyper Date, it was: {type(sql_column[0])}")
        
        column_equivalent = Counter(sql_column) == Counter(pandas_column)
    elif column_types == set([pd.Timestamp, datetime.date]):
        # Timestamp and datetime, convert datetime to pd.Timestamp
        
        if isinstance(sql_column[0], datetime.date):
            sql_column = from_datetime_date_to_pd_timestamp(sql_column)
        else: 
            raise Exception(f"sql_column was not of type datetime Date, it was: {type(sql_column[0])}")
        
        column_equivalent = Counter(sql_column) == Counter(pandas_column)
    else:
        raise Exception(f"Unknown type of column_types: {column_types}")
    
    return column_equivalent

def from_hyper_date_to_pd_timestamp(column_values):
    newColumn = []
    for date in column_values:
        assert isinstance(date, Date)
        sql_year, sql_month, sql_day = date.year, date.month, date.day
        newColumn.append(pd.Timestamp(sql_year, sql_month, sql_day))
    
    return newColumn

def from_datetime_date_to_pd_timestamp(column_values):
    newColumn = []
    for date in column_values:
        assert isinstance(date, datetime.date)
        newColumn.append(pd.Timestamp(date))
            
    return newColumn

def from_decimal_to_float(columns_values, decimal_places):
    for i in range(len(columns_values)):
        columns_values[i] = float(truncate(columns_values[i], decimal_places))
    return columns_values

def from_decimal_to_int(columns_values, decimal_places):
    for i in range(len(columns_values)):
        columns_values[i] = int(truncate(columns_values[i], decimal_places))
    return columns_values


def from_str_to_int(columns_values, decimal_places):
    for i in range(len(columns_values)):
        assert "." not in columns_values[1]
        columns_values[i] = int(columns_values[i])
    return columns_values

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
        elif isinstance(sql_column[i], Date) and isinstance(pandas_column[i], pd.Timestamp):
            sql_year, sql_month, sql_day = sql_column[i].year, sql_column[i].month, sql_column[i].day
            sql_value = pd.Timestamp(sql_year, sql_month, sql_day)
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
            
            longest_overlap = len(get_overlap(str(sql_value), str(pd_value)))
            
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
            elif longest_overlap >= 10:
                # Long overlap, so fine
                pass
            else:
                column_equivalent = False
                return column_equivalent
    
    return column_equivalent