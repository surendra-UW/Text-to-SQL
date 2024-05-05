from database import build_databases_list
from embedding_retriever import EmbeddingRetriever
from models import Prompt
from query_generator import generate_query
import json

def main():

    databases, database_index_name_map = build_databases_list()
    retriever = EmbeddingRetriever()
    retriever.build_index(databases)
    basicPrompt = Prompt()
    
    with open('./Documents/spider/train_others.json') as f:
        train_data = json.load(f)
        actual_query_file = open("actual_query.txt", "w")
        generated_query_file = open("generated_query.txt", "w")

        for query in train_data:
            actual_sql_query = query['query']
            db_id = query['db_id']
            actual_query_file.write(actual_sql_query+ " "+db_id + "\n" )
            table_schema = retriever.build_schema(databases,database_index_name_map, query['question'])

            prompt = basicPrompt.generate_prompt(table_schema, query['question'])

            generated_sql = generate_query(prompt)
            generated_query_file.write(generated_sql)
    
    

    
    
if __name__ == '__main__':
    main()