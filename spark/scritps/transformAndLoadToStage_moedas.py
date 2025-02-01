import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, current_timestamp
from airflow.providers.jdbc.hooks.jdbc import JdbcHook

if __name__ == '__main__':
    # Receiving csv file_path
    file_path = sys.argv[1]

    if file_path:
        # Creating Spark Session
        spark = SparkSession.builder \
        .appName("Spark-Airflow TransformAndLoad") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.5.jar") \
        .getOrCreate()

        # Reading data
        df = spark.read.csv(file_path, header=False, sep=";")

        # Transforming data
        columns = ["data_fechamento", "cod", "tipo", "desc_moeda", "taxa_compra", "taxa_venda", "paridade_compra", "paridade_venda"]
        df = df.toDF(*columns)
        df = df.withColumn("taxa_compra", regexp_replace(col("taxa_compra"), ",", ".").cast("float"))
        df = df.withColumn("taxa_venda", regexp_replace(col("taxa_venda"), ",", ".").cast("float"))
        df = df.withColumn("paridade_compra", regexp_replace(col("paridade_compra"), ",", ".").cast("float"))
        df = df.withColumn("paridade_venda", regexp_replace(col("paridade_venda"), ",", ".").cast("float"))
        df = df.withColumn("data_fechamento", to_date(col("data_fechamento"), "dd/MM/yyyy"))
        df = df.withColumn("processed_at", current_timestamp())
        df.createOrReplaceTempView("temp_table_moedas")
        df.show()

        # Collecting database credentials
        jdbc_hook = JdbcHook(jdbc_conn_id='postgres_spark_jdbc')
        conn = jdbc_hook.get_connection('postgres_spark_jdbc')

        # Loading data to stage
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/bacen") \
            .option("dbtable", "stg_moedas") \
            .option("user", conn.login) \
            .option("password", conn.password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

    spark.stop()
