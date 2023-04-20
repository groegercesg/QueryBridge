import time
import duckdb

def run_duck_query(db_details, query_file, verbose):
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
        con = duckdb.connect(database=db_details, read_only=False)
        
        # Prepare connection by set to single threaded
        con.execute("SET threads TO 1;")
        
        for i, single_query in enumerate(queries):
            if verbose:
                print("Executing SQL Query, part", i+1, "of", len(queries), ".")
            
            start = time.time()
            exec = con.execute(single_query)
            
            retrieved_records = exec.fetchall()
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
        raise ValueError("We have multiple statements that return values, we haven't coded how to handle this.")
            
    return results, exec_time
