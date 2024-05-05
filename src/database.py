from models import Table, Database
import json
tables_file_path = 'Documents/spider/tables.json'

def create_table(db, start, end, id):
  table_name = db['table_names_original'][id]
  table_description = db['table_names'][id]
  all_columns = db['column_names_original']
  all_columns_description = db['column_names']
  database_name = db['db_id']
  table_columns = list(map(lambda column: column[1], all_columns[start:end+1]))
  table_column_descriptions = list(map(lambda column: column[1], all_columns_description[start:end+1]))

  table_column_types = db['column_types'][start:end+1]
  # print(db['primary_keys'][id], db['db_id'], id)
  primary_key = all_columns[int(db['primary_keys'][id])][1] if len(db['primary_keys']) > id else None
  return Table(id, database_name, table_name, table_description, table_columns, \
               table_column_descriptions, table_column_types, primary_key)

def build_database(db):
  columns = db['column_names_original']
  tables = []
  id,start = 0,1
  for index, column in enumerate(columns[1:]):
    #accumlate all the columns related to a particular table
    if column[0] != id:
      table = create_table(db, start, index, id)
      tables.append(table)
      start = index+1
      id += 1

  tables.append(create_table(db, start, len(columns)-1, id))
  return Database(db['db_id'], tables, columns, db['foreign_keys'])

def build_databases_list():
    with open(tables_file_path) as tables_file:
        db_list = json.load(tables_file)
        databases = []
        # with open("catalogue.txt", "w") as file:
        for db in db_list:
            database = build_database(db)
            databases.append(database)
    return databases