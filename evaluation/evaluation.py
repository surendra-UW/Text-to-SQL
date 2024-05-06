import sqlite3
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword
import multiprocessing as mp
from func_timeout import func_timeout, FunctionTimedOut
import sys

def result_callback(result):
    exec_result.append(result)

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

def extract_single_table_name(sql):
    """
    Extracts the table name from a SQL query using sqlparse
    """
    # Parse the SQL
    parsed = sqlparse.parse(sql)[0]

    # Initialize a flag to detect when 'FROM' has been found
    from_seen = False

    # Iterate over the tokens in the parsed SQL
    for token in parsed.tokens:
        # If 'FROM' has been found, the next identifier should be the table name
        if from_seen:
            if isinstance(token, Identifier) or isinstance(token, IdentifierList):
                # Return the first Identifier name, ignoring potential commas or other characters
                return token.get_real_name() if isinstance(token, Identifier) else token.get_identifiers().__next__().get_real_name()
        
        # Set the flag when 'FROM' keyword is encountered
        if token.ttype is Keyword and token.value.upper() == 'FROM':
            from_seen = True

    return None

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
                content_before_semicolon = line[:semicolon_index]
            else:
                content_before_semicolon = line
            content.append(content_before_semicolon)
        return content

def exact_match(generated_sqls, reference_sqls):
    """
    Compare generated SQL queries to reference queries for exact match accuracy.
    """
    if len(generated_sqls) != len(reference_sqls):
        raise ValueError("The number of generated SQL queries and reference SQL queries must be the same.")
    
    # Count the number of exact matches
    match_count = 0
    for generated, reference in zip(generated_sqls, reference_sqls):
        # Normalize the SQL queries by stripping and converting to lower case for a case-insensitive comparison
        if generated.replace(' ', '').lower() == reference.replace(' ', '').lower():
            match_count += 1
            
    # Calculate the exact match accuracy
    accuracy = (match_count / len(generated_sqls)) * 100
    return accuracy

def execute_sql(database_path, sql):
    """
    Executes a SQL query on a SQLite database and returns the results.
    """
    with sqlite3.connect(database_path) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()  # Fetch all results of the query.
    return results

def execution_accuracy(generated_sqls, reference_sqls):
    """
    Computes the execution accuracy by comparing the results of the generated SQL query to the reference SQL query.
    """
    match_count = 0
    for generated, reference in zip(generated_sqls, reference_sqls):
        ref_table = extract_single_table_name(reference) + '.sqlite'
        reference_results = execute_sql(ref_table, reference)
        gen_table = extract_single_table_name(generated) + '.sqlite'
        generated_results = execute_sql(gen_table, generated)

    # Check if the results match
    if generated_results == reference_results:
        match_count += 1

    # Calculate the exact match accuracy
    accuracy = (match_count / len(generated_sqls)) * 100
    return accuracy

def run_sqls_parallel(sqls, db_places, num_cpus=1, meta_time_out=30.0):
    pool = mp.Pool(processes=num_cpus)
    for i,sql_pair in enumerate(sqls):

        predicted_sql, ground_truth = sql_pair
        pool.apply_async(execution_accuracy, args=(predicted_sql, ground_truth, db_places[i], i, meta_time_out), callback=result_callback)
    pool.close()
    pool.join()

if __name__ == "__main__":
    exec_result = []
    # # Example usage
    # sql_query = "SELECT name, age FROM users WHERE id = 1;"
    # table_name = extract_single_table_name(sql_query)
    # print("Extracted table name:", table_name)

    # File paths
    generated_sql_file = 'actual_query.txt'
    reference_sql_file = 'actual_query_duplicate.txt'

    # Read SQL statements from files
    generated_sqls = read_sql_file(generated_sql_file)
    reference_sqls = read_sql_file(reference_sql_file)

    # Calculate accuracy
    EM_accuracy = exact_match(generated_sqls, reference_sqls)
    print(f"Exact Match Accuracy: {EM_accuracy}%")

    # Calculate execution accuracy
    EX_accuracy = execution_accuracy(generated_sqls, reference_sqls)
    print(f"Execution Accuracy: {EX_accuracy}%")