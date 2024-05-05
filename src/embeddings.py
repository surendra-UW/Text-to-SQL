from sentence_transformers import SentenceTransformer
from models import Embeddings
import numpy as np
import heapq
import math

SEPARATOR_STR = " "
SEPERATOR_COMMA = ","

def build_sentenses(database):

    sentenses = []
    for table in database.tables: 
        content = table.database_name + SEPARATOR_STR + table.description
        for column in table.columns_description:
            content += SEPERATOR_COMMA + SEPARATOR_STR + column
        sentenses.append(content)

    return sentenses

def build_table_embeddings(databases):
    model = SentenceTransformer("all-MiniLM-L6-v2")

    embedding_list = []
    for database in databases: 
        # Sentences are encoded by calling model.encode()
        tables_encoded = build_sentenses(database)
        embeddings = model.encode(tables_encoded)

        for index, embedding in enumerate(embeddings):
            em = Embeddings(embedding, database.name, index)
            embedding_list.append(em)

    return embedding_list

def cosine_similarity(vectora, vectorb):

    # Calculate dot product and vector magnitudes
    dot_product = np.dot(vectora, vectorb)
    norm_a = np.linalg.norm(vectora)
    norm_b = np.linalg.norm(vectorb)

    if norm_a == 0 or norm_b == 0:
      return 0
    
    return dot_product / (norm_a * norm_b)

def topk_table_match(embedding_list, query_embedding, k=5):
    max_heap = []

    for index, embedding in enumerate(embedding_list):
        sim = cosine_similarity(query_embedding, embedding.table_embeddings)
        prob = math.exp(sim)
        heapq.heappush(max_heap, (-prob, index))

    top_k_embeddings = []
    for i in range(5):
        prob, index = heapq.heappop(max_heap)
        em = embedding_list[index]
        print(f"emibedding is {em.db_id} {em.table_id} with prob {-prob}")
        top_k_embeddings.apend(em)

    return top_k_embeddings