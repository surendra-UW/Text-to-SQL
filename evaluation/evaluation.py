import sqlite3
import concurrent.futures
import argparse

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
                sql_info = (line[:semicolon_index].strip(),line[semicolon_index+1:].strip())
            else:
                sql_info = (line[:semicolon_index].strip(),'')
            content.append(sql_info)
        return content

# def compute_acc_by_diff(exec_results,diff_json_path):
#     num_queries = len(exec_results)
#     results = [res['res'] for res in exec_results]
#     contents = load_json(diff_json_path)
#     simple_results, moderate_results, challenging_results = [], [], []

#     for i,content in enumerate(contents):
#         if content['difficulty'] == 'simple':
#             simple_results.append(exec_results[i])

#         if content['difficulty'] == 'moderate':
#             moderate_results.append(exec_results[i])

#         if content['difficulty'] == 'challenging':
#             challenging_results.append(exec_results[i])

#     simple_acc = sum([res['res'] for res in simple_results])/len(simple_results)
#     moderate_acc = sum([res['res'] for res in moderate_results])/len(moderate_results)
#     challenging_acc = sum([res['res'] for res in challenging_results])/len(challenging_results)
#     all_acc = sum(results)/num_queries
#     count_lists = [len(simple_results), len(moderate_results), len(challenging_results), num_queries]
#     return simple_acc * 100, moderate_acc * 100, challenging_acc * 100, all_acc * 100, count_lists


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
    with concurrent.futures.ThreadPoolExecutor() as executor:
        #execute all sqls asynchronously
        future_to_actual_sql = {executor.submit(execute_sql, actual_sql[0], actual_sql[1]): i for i, (actual_sql, _) in enumerate(query_pairs)}
        future_to_generated_sql = {executor.submit(execute_sql, generated_sql[0], generated_sql[1]): i for i, (_, generated_sql) in enumerate(query_pairs)}
        
        #restore all results
        actual_results = [None] * len(query_pairs)
        generated_results = [None] * len(query_pairs)

        for future in concurrent.futures.as_completed(future_to_actual_sql):
            index = future_to_actual_sql[future]
            actual_results[index] =future._result
        for future in concurrent.futures.as_completed(future_to_generated_sql):
            index = future_to_generated_sql[future]
            generated_results[index] =future._result
    match_count = 0
    for result in zip(actual_results,generated_results):
        if result[0] == result[1]:
            match_count += 1

    # Calculate the exact match accuracy
    accuracy = (match_count / len(query_pairs)) * 100
    return accuracy


if __name__ == "__main__":
    # args_parser = argparse.ArgumentParser()
    # args_parser.add_argument('--actual_sql_path', type=str, required=True, default='')
    # args_parser.add_argument('--generated_sql_path', type=str, required=True, default='')
    # args = args_parser.parse_args()
    # # Example usage
    # sql_query = "SELECT name, age FROM users WHERE id = 1;"
    # table_name = extract_single_table_name(sql_query)
    # print("Extracted table name:", table_name)

    # File paths
    actual_sql_file = 'evaluation/actual_query_1.txt'
    #actual_sql_file = args.actual_sql_path

    # TODO: predict file
    generated_sql_file = 'evaluation/generated_query_1.txt'
    #generated_sql_file = args.generated_sql_path

    # Read SQL statements from files
    actual_sqls = read_sql_file(actual_sql_file)
    generated_sqls = read_sql_file(generated_sql_file)

    #use actual query's database name
    for idx, val in enumerate(generated_sqls):
        generated_sqls[idx] = (val[0],actual_sqls[idx][1])
        temp = actual_sqls[idx]
    
    for val in generated_sqls:
        val = val

    query_pairs = list(zip(actual_sqls, generated_sqls))
    # Calculate accuracy
    EM_rate = exact_match(query_pairs)
    print(f"Exact Match Rate: {EM_rate}%")

    # Calculate execution accuracy
    EX_accuracy = parallel_execute_and_compare(query_pairs)
    print(f"Execution Accuracy: {EX_accuracy}%")