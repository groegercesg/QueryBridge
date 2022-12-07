"""

Tests of parsing aggregations:
    - Nested tests, direct to the function (do_aggregation)
    - A sum of an avg
    - Count of a min of an avg of a max of a sum
    - A sum divided by an average
    - A sum multiplied by a count
    - A min minuses a max
    - A count plus an average all divided a two columns added and multiplied by 25

Tests of the SQL to pandas flow
    - Featuring these complex aggregations
    - Two or Three of them
    
Tests of Group aggregation:
    - Testing the same sort of stuff but after a group
    - See TPC-H Query 1
    
Reformed Aggregation Implementation Notes:
    - One function for both Aggregation and Group Aggregation, write first for Aggregation
    - Use (Binary) Expression Trees
        - With all the standard operators (+, -, *, /) and special ones like Avg, Sum, Min, Max, Count
    - Build a tree for a Expression, visualise it, then evaluate it by converting it to Python
        - We can have a built-in knowledge for what each operator is represented like in Python.

"""