from sqlglot import parse_one, exp
import json

def get_columns(query):
    column_references = dict()
    for select in parse_one(query).find_all(exp.Select):
        for projection in select.expressions:
            if str(projection) != str(projection.alias_or_name):
                projection_original = " ".join(str(projection).split()[:-2]).lower()
                if projection_original in column_references:
                    raise ValueError("We are trying to process a SQL but finding multiple identical projections")
                else:
                    column_references[projection_original] = str(projection.alias_or_name)

    return list(column_references.values())

def compare(query_file, pandas_result, sql_result):
    # Read SQL file
    with open(query_file, 'r') as file:
        sql_query = file.read()
    
    columns = get_columns(sql_query)
    
    print(columns)
    print("A")