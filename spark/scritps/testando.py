from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('Test JDBC Connection') \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/bacen") \
    .option("dbtable", "moedas") \
    .option("user", "postgres") \
    .option("password", "1598746320") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()
