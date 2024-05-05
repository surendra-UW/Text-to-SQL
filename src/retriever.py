from abc import ABC, abstractmethod

class Retriever(ABC):
    @abstractmethod
    def build_schema(self, databases,database_index_map, query):
        """
        Builds the table schema for the natural language query by retrieving the relavant tables.

        Args:
            databases: List of all the Databases as object
            database_index_map: Database name to database index in databases list map 
            query: Natural Language query to build the related tables Schema  

        Returns:
            Schema: table schema as a string 
        """
        pass

    @abstractmethod
    def build_index(self, databases):
        """
        Builds the index for the retriever.

        Args:
            databases: Database Object list with all the tables information

        Returns:
            str: Message indicating successful index creation.
        """
        pass
