from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sqrt,
    sum as spark_sum,
    pow,
    first,
    regexp_extract
)

from src.config import DOCUMENT_IDS


def calculate_tfidf(
    weighted_tf: DataFrame,
    df_idf: DataFrame
) -> DataFrame:
    """
    Calculate TF-IDF for every term in every document.

    TF-IDF = Weighted TF * IDF
    """

    tfidf = (
        weighted_tf
        .join(df_idf, on="term", how="inner")
        .withColumn(
            "tfidf",
            col("weighted_tf") * col("idf")
        )
        .select(
            "term",
            "document_id",
            "tf",
            "weighted_tf",
            "df",
            "idf",
            "tfidf"
        )
    )

    return tfidf


def calculate_document_lengths(
    tfidf: DataFrame
) -> DataFrame:
    """
    Calculate the L2 vector length of every document.

    Length(d) = sqrt(sum(TF-IDF(t,d)^2))
    """

    document_lengths = (
        tfidf
        .groupBy("document_id")
        .agg(
            sqrt(
                spark_sum(
                    pow(col("tfidf"), 2)
                )
            ).alias("vector_length")
        )
        .withColumn(
            "document_number",
            regexp_extract(
                col("document_id"),
                r"(\d+)",
                1
            ).cast("int")
        )
        .orderBy("document_number")
        .drop("document_number")
    )

    return document_lengths


def normalize_tfidf(
    tfidf: DataFrame,
    document_lengths: DataFrame
) -> DataFrame:
    """
    Normalize every TF-IDF weight.

    Normalized TF-IDF = TF-IDF / Document Vector Length
    """

    normalized = (
        tfidf
        .join(
            document_lengths,
            on="document_id",
            how="inner"
        )
        .withColumn(
            "normalized_tfidf",
            col("tfidf") / col("vector_length")
        )
        .select(
            "term",
            "document_id",
            "tfidf",
            "vector_length",
            "normalized_tfidf"
        )
    )

    return normalized


def create_matrix(
    data: DataFrame,
    value_column: str
) -> DataFrame:
    """
    Convert long-format records into a term-document matrix.

    Rows = Terms
    Columns = Documents
    Missing values = 0
    """

    matrix = (
        data
        .groupBy("term")
        .pivot("document_id", DOCUMENT_IDS)
        .agg(
            first(col(value_column))
        )
        .na.fill(0)
        .orderBy("term")
    )

    return matrix
