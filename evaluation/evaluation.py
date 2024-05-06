import sqlite3
import multiprocessing as mp
from func_timeout import func_timeout, FunctionTimedOut
import sys
import concurrent.futures

def execute_model(predicted_sql,ground_truth, db_place, idx, meta_time_out):
    try:
        res = func_timeout(meta_time_out, execute_sql,
                                  args=(predicted_sql, ground_truth, db_place))
    except KeyboardInterrupt:
        sys.exit(0)
    except FunctionTimedOut:
        result = [(f'timeout',)]
        res = 0
    except Exception as e:
        result = [(f'error',)]  # possibly len(query) > 512 or not executable
        res = 0
    # print(result)
    # result = str(set([ret[0] for ret in result]))
    result = {'sql_idx': idx, 'res': res}
    # print(result)
    return result

def read_sql_file(file_path):
    """
    Reads a SQL file and returns a list of SQL statements. Handles both line-separated formats.
    Returns:
        list of str: List of SQL statements.
    """
    content = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            # every line should have a semicolon
            semicolon_index = line.find(';')
            # get content before semicolon
            if semicolon_index != -1:
                #sql_info = (sql query, database_name)
                sql_info = (line[:semicolon_index],line[semicolon_index+1:].strip())
            else:
                sql_info = (line[:semicolon_index],'')
            content.append(sql_info)
        return content

def exact_match(query_pairs):
    """
    Compare generated SQL queries to reference queries for exact match accuracy.
    """

    # Count the number of exact matches
    match_count = 0
    for actual, rgenerated in query_pairs:
        # Normalize the SQL queries by stripping and converting to lower case for a case-insensitive comparison
        if actual[0].replace(' ', '').lower() == rgenerated[0].replace(' ', '').lower():
            match_count += 1
            
    # Calculate the exact match accuracy
    accuracy = (match_count / len(generated_sqls)) * 100
    return accuracy

def execute_sql(sql, database):
    """
    Executes a SQL query on a SQLite database and returns the results.
    """
    database_path = 'evaluation/database/' + database + '/' + database +'.sqlite'
    with sqlite3.connect(database_path) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()  # Fetch all results of the query.
    return results

def parallel_execute_and_compare(query_pairs):
    """
    Computes the execution accuracy by comparing the results of the generated SQL query to the reference SQL query.
    """
    #TODO parallel
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     #execute all sqls asynchronously
    #     future_to_actual_sql = {executor.submit(execute_sql, actual_sql[0], actual_sql[1]): i for i, (actual_sql, _) in enumerate(query_pairs)}
    #     future_to_generated_sql = {executor.submit(execute_sql, generated_sql[0], generated_sql[1]): i for i, (_, generated_sql) in enumerate(query_pairs)}
        
    #     #restore all results
    #     actual_results = []
    #     generated_results = []

    #     for future in concurrent.futures.as_completed(future_to_actual_sql):
    #         actual_results.append(future_to_actual_sql[future])
    #     for future in concurrent.futures.as_completed(future_to_generated_sql):
    #         generated_results.append(future)
    match_count = 0
    for query in query_pairs:
        actual = query[0]
        generated = query[1]
        actual_result = execute_sql(actual[0] , actual[1])
        generated_result = execute_sql(generated[0], generated[1])
        if actual_result == generated_result:
            match_count += 1
            
    # Calculate the exact match accuracy
    accuracy = (match_count / len(query_pairs)) * 100
    return accuracy


if __name__ == "__main__":
    exec_result = []
    # # Example usage
    # sql_query = "SELECT name, age FROM users WHERE id = 1;"
    # table_name = extract_single_table_name(sql_query)
    # print("Extracted table name:", table_name)

    # File paths
    actual_sql_file = 'evaluation/actual_query_duplicate.txt'
    # TODO: predict file
    generated_sql_file = 'evaluation/actual_query.txt'

    # Read SQL statements from files
    actual_sqls = read_sql_file(actual_sql_file)
    generated_sqls = read_sql_file(generated_sql_file)

    query_pairs = list(zip(actual_sqls, generated_sqls))
    # Calculate accuracy
    EM_rate = exact_match(query_pairs)
    print(f"Exact Match Rate: {EM_rate}%")

    # Calculate execution accuracy
    EX_accuracy = parallel_execute_and_compare(query_pairs)
    print(f"Execution Accuracy: {EX_accuracy}%")