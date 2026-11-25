from pyspark.sql import SparkSession


def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("HealthcareInformationRetrieval")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


if __name__ == "__main__":

    spark = create_spark_session()

    print("================================")
    print("Spark Session Started Successfully!")
    print("Spark Version:", spark.version)
    print("Master:", spark.sparkContext.master)
    print("================================")

    data = [
        (1, "Healthcare"),
        (2, "Medical Research"),
        (3, "Patient Care")
    ]

    df = spark.createDataFrame(data, ["id", "topic"])

    df.show(truncate=False)

    spark.stop()
