import duckdb

# connect to an in-memory database
con = duckdb.connect()

print("DuckDB Testing")

# CREATE TABLE weather (
#    city           VARCHAR,
#    temp_lo        INTEGER, -- minimum temperature on a day
#    temp_hi        INTEGER, -- maximum temperature on a day
#    prcp           REAL,
#    date           DATE
#);

# COPY weather FROM '/home/user/weather.csv';

setup_commands = ["CREATE TABLE students(sid INTEGER PRIMARY KEY, name VARCHAR);",
                  "CREATE TABLE exams(cid INTEGER, sid INTEGER, grade INTEGER, PRIMARY KEY(cid, sid));",
                  "INSERT INTO students VALUES (1, 'Mark'), (2, 'Hannes'), (3, 'Pedro');",
                  "INSERT INTO exams VALUES (1, 1, 8), (1, 2, 8), (1, 3, 7), (2, 1, 9), (2, 2, 10);"]

# enable profiling in json format
# write the profiling output to a specific file on disk
# configure the system to use 1 thread
explain_commands = ["PRAGMA enable_profiling='json';",
                    "PRAGMA profile_output='duck_db_explain.json';",
                    "PRAGMA explain_output='all';",
                    "SET threads TO 1;"]

for command in setup_commands:
    con.execute(command)
    
for command in explain_commands:
    con.execute(command)

explain_command = "EXPLAIN SELECT name FROM students JOIN exams USING (sid) WHERE name LIKE 'Ma%';"
con.execute(explain_command)