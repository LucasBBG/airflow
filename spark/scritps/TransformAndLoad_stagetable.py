import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, to_date, current_timestamp
from airflow.providers.jdbc.hooks.jdbc import JdbcHook

if __name__ == '__main__':
    file_path = sys.argv[1]

    columns = ["data_fechamento", "cod", "tipo", "desc_moeda", "taxa_compra", "taxa_venda", "paridade_compra", "paridade_venda"]
    
    spark = SparkSession.builder \
        .appName('Airflow Spark Transform and Load') \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.5.jar") \
        .getOrCreate()

    if file_path:
        df = spark.read.csv(file_path, header=False, sep=";")
        df = df.toDF(*columns)

        # Convertendo os tipos de dados das colunas
        from pyspark.sql.functions import col
        df = df.withColumn("taxa_compra", regexp_replace(col("taxa_compra"), ",", ".").cast("float"))
        df = df.withColumn("taxa_venda", regexp_replace(col("taxa_venda"), ",", ".").cast("float"))
        df = df.withColumn("paridade_compra", regexp_replace(col("paridade_compra"), ",", ".").cast("float"))
        df = df.withColumn("paridade_venda", regexp_replace(col("paridade_venda"), ",", ".").cast("float"))
        df = df.withColumn("data_fechamento", to_date(col("data_fechamento"), "dd/MM/yyyy"))
        df = df.withColumn("processed_at", current_timestamp())

        df.show()
        df.createOrReplaceTempView("temp_table_moedas")

        spark.sql("SHOW TABLES").show()

        # Consultando o conteúdo da tabela temporária
        df_temp_table = spark.sql("SELECT * FROM temp_table_moedas")

        # Exibindo o conteúdo da tabela temporária
        df_temp_table.show()

        # Pegando as credenciais do banco de dados
        jdbc_hook = JdbcHook(jdbc_conn_id='postgres_spark_jdbc')
        conn = jdbc_hook.get_connection('postgres_spark_jdbc')

        # Inserindo os dados na tabela "moedas" do PostgreSQL
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/bacen") \
            .option("dbtable", "stg_moedas") \
            .option("user", conn.login) \
            .option("password", conn.password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    spark.stop()
