from sentence_transformers import SentenceTransformer
from models import Embeddings
import numpy as np
import heapq
import math
from retriever import Retriever
from database import build_table_schema

class EmbeddingRetriever(Retriever):
    SEPARATOR_STR = " "
    SEPERATOR_COMMA = ","
    model = SentenceTransformer("all-MiniLM-L6-v2")

    def _build_sentenses(self, database):

        sentenses = []
        for table in database.tables: 
            content = table.database_name + self.SEPARATOR_STR + table.description
            for column in table.columns_description:
                content += self.SEPERATOR_COMMA + self.SEPARATOR_STR + column
            sentenses.append(content)

        return sentenses

    def build_index(self, databases):

        embedding_list = []
        for database in databases: 
            # Sentences are encoded by calling model.encode()
            tables_encoded = self._build_sentenses(database)
            embeddings = self.model.encode(tables_encoded)

            for index, embedding in enumerate(embeddings):
                em = Embeddings(embedding, database.name, index)
                embedding_list.append(em)

        self.embedding_list = embedding_list
        return embedding_list

    def _cosine_similarity(self, vectora, vectorb):

        # Calculate dot product and vector magnitudes
        dot_product = np.dot(vectora, vectorb)
        norm_a = np.linalg.norm(vectora)
        norm_b = np.linalg.norm(vectorb)

        if norm_a == 0 or norm_b == 0:
            return 0
        
        return dot_product / (norm_a * norm_b)

    def _topk_table_match(self, query_embedding, k=5):
        max_heap = []
        embedding_list = self.embedding_list
        for index, embedding in enumerate(embedding_list):
            sim = self._cosine_similarity(query_embedding, embedding.table_embeddings)
            prob = math.exp(sim)
            heapq.heappush(max_heap, (-prob, index))

        top_k_embeddings = []
        for i in range(5):
            prob, index = heapq.heappop(max_heap)
            em = embedding_list[index]
            print(f"emibedding is {em.db_id} {em.table_id} with prob {-prob}")
            top_k_embeddings.apend(em)

        return top_k_embeddings
    
    def build_schema(self, databases,database_index_map, query):
        query_embedding = self.model.encode(query)
        top_match_table_embeddings = self._topk_table_match(query_embedding)

        table_schema = ""
        for embedding in top_match_table_embeddings:
            database = databases[database_index_map[embedding.db_id]]
            table = database.tables[embedding.table_id]
            table_schema += build_table_schema(database, table, False)

        return table_schema

            


