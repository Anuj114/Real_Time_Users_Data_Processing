import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {''}

    """)

def create_table(session):
    pass

def insert_data(session, **kwargs):
    pass

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder.appName('SparkDataStreaming') \
        .config('spark.jars.packages',"com.datastax.spark:spark-cassandra-connector_2.13:3.41",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1")\
                .config("spark.cassandra.connection.host",'localhost').\
        getOrCreate()

        s_conn.sparkContext.setLogLevel("Error")
        logging.info("Spark connection created successfully")
        
    except Exception as e:
        logging.error(f"Couldn't establish the spark session due to {e}")

    return spark_conn

def create_cassandra_connection():
    cas_session = None
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
    except Exception as e:
        logging.error(f"Couldn't establish cassandra connect due to error {e}")

    return cas_session

if __name__ == '__main__':
    
    spark_conn = create_spark_connection()
    
    if spark_conn is not None:
        session = create_cassandra_connection()
        if session is not None:
            create_keyspace(session)
            create_table(session)

