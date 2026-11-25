from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    size,
    lit,
    when,
    log10,
    countDistinct
)

from src.config import NUM_DOCUMENTS


def calculate_raw_tf(positional_index: DataFrame) -> DataFrame:
    """
    Calculate Raw Term Frequency for every term in every document.

    TF = Number of occurrences of a term in a document.

    We calculate TF directly from the positions stored
    in the positional index.
    """

    raw_tf = positional_index.select(
        col("term"),
        col("document_id"),
        size(col("positions")).alias("tf")
    )

    return raw_tf


def calculate_weighted_tf(raw_tf: DataFrame) -> DataFrame:
    """
    Calculate Weighted TF.

    Weighted TF = 1 + log10(TF), when TF > 0.
    Otherwise, Weighted TF = 0.
    """

    weighted_tf = raw_tf.withColumn(
        "weighted_tf",
        when(
            col("tf") > 0,
            lit(1.0) + log10(col("tf").cast("double"))
        ).otherwise(lit(0.0))
    )

    return weighted_tf


def calculate_df_idf(raw_tf: DataFrame) -> DataFrame:
    """
    Calculate Document Frequency and Inverse Document Frequency.

    DF = Number of documents containing a term.

    IDF = log10(N / DF)

    N = Total number of documents.
    """

    df_idf = (
        raw_tf
        .groupBy("term")
        .agg(
            countDistinct("document_id").alias("df")
        )
        .withColumn(
            "idf",
            log10(
                lit(float(NUM_DOCUMENTS)) / col("df")
            )
        )
        .orderBy("term")
    )

    return df_idf
