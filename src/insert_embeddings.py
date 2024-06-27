import pandas as pd
from snowflake.connector import connect
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct
import ray

# Initialize Ray
ray.init()

# Snowflake connection details
SNOWFLAKE_HOST='localhost'
SNOWFLAKE_PORT=10017
SNOWFLAKE_USER = 'sdm'
SNOWFLAKE_PASSWORD = 'sdm'
SNOWFLAKE_ACCOUNT = 'zefr'
SNOWFLAKE_PROTOCOL= 'http'
SNOWFLAKE_WAREHOUSE= 'ENGINEER_WH',
SNOWFLAKE_DATABASE = 'prod'
SNOWFLAKE_SCHEMA = 'measurement'
SNOWFLAKE_TABLE = 'meta_embeddings'

# Qdrant connection details
QDRANT_HOST = 'localhost'
QDRANT_PORT = 6333
QDRANT_COLLECTION_NAME = 'embeddings_index'

# Fetch embeddings from Snowflake
@ray.remote
def fetch_embeddings_from_snowflake():
    conn = connect(
        host="localhost",
        port=10017,
        user="sdm",
        password="sdm",
        account="zefr",
        protocol="http",
        warehouse="ENGINEER_WH",
        database="prod",
        schema="measurement",
    )
    print("snowflake connection:",conn)
    query = f"""
                SELECT *
                FROM prod.measurement.meta_embeddings
                LIMIT 100;
            """
    df = pd.read_sql(query, conn)
    print(df.head(1))
    conn.close()
    return df

# Ingest data into Qdrant
@ray.remote
def ingest_into_qdrant(df):
    client = QdrantClient(QDRANT_HOST, port=QDRANT_PORT)

    points = []
    for index, row in df.iterrows():
        point = PointStruct(
            platform_content_id=row['platform_content_id'],  # platform_content_id is the primary key
            vector=row['embedding'],  # 'embedding' is the vector column
            payload=row.to_dict()
        )
        points.append(point)

    client.upsert(collection_name=QDRANT_COLLECTION_NAME, points=points)

if __name__ == "__main__":

    print("############START POINT")
    # Fetch data from Snowflake
    fetch_result = fetch_embeddings_from_snowflake.remote()
    df = ray.get(fetch_result)

    # Ingest data into Qdrant
    ingestion_result = ingest_into_qdrant.remote(df)
    ray.get(ingestion_result)

    # Shutdown Ray
    ray.shutdown()
