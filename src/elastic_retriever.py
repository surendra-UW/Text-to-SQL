from elasticsearch import Elasticsearch
from models import Table
from database import build_databases_list
import json

es = Elasticsearch('https://localhost:9200', http_auth=('elastic', 'ZXJxoo-rx_YWk1kbMs*b'), verify_certs=False)
index_name = "database_catalogue_index"
es.indices.create(index=index_name)
SEPARATOR_SPACE_STR = " "

def build_document_content(table):
    content = table.database_name + SEPARATOR_SPACE_STR + table.description
    for column in table.columns_description:
        content += SEPARATOR_SPACE_STR + column
    return content

def build_index(databases):
    databases = build_databases_list()

    for database in databases:
        # Index the document
        for index, table in enumerate(database.tables):
            document_body = {
                "content": build_document_content(table)
            }
            # if index == 0:
            # file.write(json.dumps(table, cls=DatabaseCatalogueEncoder))
            # file.write("\n")
                    # print(json.dumps(table, cls=DatabaseCatalogueEncoder)) json.dumps(table, cls=DatabaseCatalogueEncoder)
            es.index(index=index_name, id=database.name +'_'+ str(index), body=document_body)

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