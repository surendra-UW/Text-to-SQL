

class Database:
    """
    Model class for Database 
    represents the database schema with list of Tables
    and foreign Key relationships
    """
    def __init__(self, name, tables, columns, foreign_keys):
        self.name = name
        self.tables = tables
        self.columns = columns
        self.foreign_keys = foreign_keys

    def __str__(self):
        """Returns a string representation of the Database object."""
        return f"Database(name='{self.name}', tables={self.tables}), columns={self.columns}), foreign_keys={self.foreign_keys})"

class Table:
    """
    Model class for Database Table
    represents the table with list of Columns
    and corresponding description
    """
    def __init__(self, id, database_name ,name, description, columns, columns_description,column_types, primary_key):
        self.id = id
        self.database_name = database_name
        self.name = name
        self.description = description
        self.columns = columns
        self.columns_description = columns_description
        self.column_types = column_types
        self.primary_key = primary_key

    def __str__(self):
        """Returns a string representation of the Table object."""
        return f"Table(id={self.id}, name='{self.name}', description='{self.description}', columns={self.columns}, columns_description={self.columns_description}, column_types={self.column_types}, primary_key='{self.primary_key}')"


class Prompt:
    """
    Model generating the LLM Prompt
    """
    def __init__(self):
        self.prompt_template = \
        f"""Given the following table schema:
{{}}

Write a SQL query for the english query:
Dont provide any explaination just give the response query as a string:
{{}}
"""

    def generate_prompt(self, table_schema, question):
        return self.prompt_template.format(table_schema, question)

    def generate_default_prompt(self, question):
        return f"""Write a SQL query for the english query:
Dont provide any explaination just give the response query as a string in a single line:
{question}"""

    
class Embeddings:
    """
    Model Class for embeddings for a particular Table
    Represents embedding Vector, Table id and Database id
    """  
    def __init__(self, table_embeddings, db_id, table_id):
        self.table_embeddings = table_embeddings
        self.db_id = db_id
        self.table_id=table_id