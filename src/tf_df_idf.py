from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    size,
    lit,
    when,
    log10,
    countDistinct
)


def calculate_raw_tf(positional_index: DataFrame) -> DataFrame:
    """
    Raw TF = number of occurrences of a term in a document
           = number of positions stored in the positional index.
    """

    return positional_index.select(
        col("term"),
        col("document_id"),
        size(col("positions")).alias("tf")
    )


def calculate_weighted_tf(raw_tf: DataFrame) -> DataFrame:
    """
    Weighted TF = 1 + log10(TF) when TF > 0, otherwise 0.
    """

    return raw_tf.withColumn(
        "weighted_tf",
        when(
            col("tf") > 0,
            lit(1.0) + log10(col("tf").cast("double"))
        ).otherwise(lit(0.0))
    )


def calculate_df_idf(
    raw_tf: DataFrame,
    num_documents: int
) -> DataFrame:
    """
    DF  = number of documents containing the term.
    IDF = log10(N / DF), N = number of documents in the dataset.
    """

    return (
        raw_tf
        .groupBy("term")
        .agg(
            countDistinct("document_id").alias("df")
        )
        .withColumn(
            "idf",
            log10(lit(float(num_documents)) / col("df"))
        )
        .orderBy("term")
    )