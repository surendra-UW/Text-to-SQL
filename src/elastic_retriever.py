from elasticsearch import Elasticsearch
from models import Table
from database import build_databases_list
import json
from retriever import Retriever
from database import build_table_schema

class ElasticRetriever(Retriever):
    es = Elasticsearch('https://localhost:9200', http_auth=('elastic', 'ZXJxoo-rx_YWk1kbMs*b'), verify_certs=False)
    index_name = "database_catalogue_index"
    es.indices.create(index=index_name)
    SEPARATOR_SPACE_STR = " "

    def build_document_content(self, table):
        content = table.database_name + self.SEPARATOR_SPACE_STR + table.description
        for column in table.columns_description:
            content += self.SEPARATOR_SPACE_STR + column
        return content

    def build_index(self, databases):
        databases = build_databases_list()

        for database in databases:
            # Index the document
            for index, table in enumerate(database.tables):
                document_body = {
                    "content": self.build_document_content(table)
                }
                # if index == 0:
                # file.write(json.dumps(table, cls=DatabaseCatalogueEncoder))
                # file.write("\n")
                        # print(json.dumps(table, cls=DatabaseCatalogueEncoder)) json.dumps(table, cls=DatabaseCatalogueEncoder)
                self.es.index(index=self.index_name, id=database.name +'_'+ str(index), body=document_body)

    def build_schema(self, databases,database_index_map, query):
        search_results = self.es.search(index="database", body={"query": {"match": {"content": query}}})

        hits = search_results['hits']['hits']
        if len(hits) == 0:
            return ""
        
        id = hits[0]['_id']

        words = id.split('_')
        db_id, table_id = "_".join(words[:-1]), int(words[-1])
        table_schema  = ""
        
        database = databases[database_index_map[db_id]]
        table = database.tables[table_id]
        table_schema = build_table_schema(database, table, True)
        
        return table_schema

class DatabaseCatalogueEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Table):
      # Override default behavior for Book objects
      return {
        "name" : obj.name,
        "description" : obj.description,
        # "columns" : obj.columns,
        "columns_description" : obj.columns_description,
        "db_name": obj.database_name,
        # "column_types" : obj.column_types,
        # "primary_key" : obj.primary_key
      }
    return super().default(obj)
  
#   Prompt().generate_default_prompt(eng_query)
# basicPrompt = Prompt()
# prompt = basicPrompt.generate_prompt(table_schema, eng_query)