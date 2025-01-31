from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator

def main():
    spark = SparkSession.builder \
        .appName("Airflow PySpark Insert PSQL") \
        .getOrCreate()

    df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/bacen") \
    .option("dbtable", "moedas") \
    .option("user", "postgres") \
    .option("password", "123") \
    .option("driver", "org.postgresql.Driver") \
    .load()

    df.printSchema()
    df.show()

    spark.stop()

if __name__ == "__main__":
    main()

