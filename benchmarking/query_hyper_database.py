import time
from prepare_databases.prepare_hyperdb import PrepareHyperDB

def run_hyper_query(db_details, query_file, verbose):
    # Read SQL file
    with open(query_file, 'r') as file:
        sql_query = file.read()
        
    exec_time = 0.0
    results = []
    queries = None
    
    # Process our SQL queries into an array of queries
    if sql_query.count(";") > 1:
        # Many queries in one file
        queries = sql_query.split(";")
        for i in range(len(queries)):
            queries[i] = queries[i].strip()
            queries[i] = queries[i] + ";"
            if queries[i] == ";":
                del queries[i]
    else:
        queries = [sql_query]

    # Try query, catch error
    try:
        db = PrepareHyperDB(db_details)
        
        for i, single_query in enumerate(queries):
            if verbose:
                print("Executing SQL Query, part", i+1, "of", len(queries), ".")
            
            start = time.time()
            
            retrieved_records = db.execute_query(single_query)
            results.append(retrieved_records)
            
            end = time.time()
            
            # Increment running counter
            exec_time += (end - start)
    except Exception as error:
        print("Error while fetching data from Duck DB: ", error)
    
    # Choose results
    if len(results) == 1:
        results = results[0]
    else:
        for individual_res in results:
            if individual_res != []:
                results = individual_res
                break
        #raise ValueError("We have multiple statements that return values, we haven't coded how to handle this.")
            
    return results, exec_time