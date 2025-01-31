from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Airflow PySpark Read") \
        .getOrCreate()
    

    df = spark.read.csv("/home/dev-linux/airflow/spark/files/data.csv", header=True, sep=",")
    df.show()

    spark.stop()

if __name__ == "__main__":
    main()