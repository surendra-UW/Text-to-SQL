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
    database_index_name_map = {}
    with open(tables_file_path) as tables_file:
        db_list = json.load(tables_file)
        databases = []
        # with open("catalogue.txt", "w") as file:
        for index, db in enumerate(db_list):
            database = build_database(db)
            databases.append(database)
            database_index_name_map[database.name] = index
    return databases, database_index_name_map

def build_table_schema(database, table, include_reference_tables):
    column_types = table.column_types
    query = f"CREATE TABLE {table.name} (\n"
    for index, column in enumerate(table.columns):
        primary_key = " PRIMARY KEY" if column == table.primary_key else ""
        query = query + f"{column} {column_types[index]}{primary_key},\n"
    all_columns = database.columns
    reference_tables = []
    for fk in database.foreign_keys:
        if all_columns[fk[0]][0] == table.id:
            query += f"FOREIGN KEY({all_columns[fk[0]][1]}) REFERENCES "
            reference_table_ind = all_columns[fk[1]][0]
            reference_tables.append(reference_table_ind)
            reference_table = database.tables[reference_table_ind]
            query += f"{reference_table.name}({all_columns[fk[1]][1]})\n"

    #end of the table
    query += ");\n"

    #add the reference tables to the query string 
    if include_reference_tables and reference_tables:
        for table_ind in reference_tables:
            query += "\n\n"
            query += build_table_schema(database,  database.tables[table_ind], False)
    return query