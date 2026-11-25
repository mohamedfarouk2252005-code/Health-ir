from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sqrt,
    sum as spark_sum,
    pow,
    first,
    when,
    lit
)


def calculate_tfidf(
    weighted_tf: DataFrame,
    df_idf: DataFrame
) -> DataFrame:
    """
    TF-IDF = Weighted TF * IDF
    """

    return (
        weighted_tf
        .join(df_idf, on="term", how="inner")
        .withColumn("tfidf", col("weighted_tf") * col("idf"))
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


def calculate_document_lengths(tfidf: DataFrame) -> DataFrame:
    """
    Length(d) = sqrt(sum(TF-IDF(t, d)^2))
    """

    return (
        tfidf
        .groupBy("document_id")
        .agg(
            sqrt(
                spark_sum(pow(col("tfidf"), 2))
            ).alias("vector_length")
        )
    )


def normalize_tfidf(
    tfidf: DataFrame,
    document_lengths: DataFrame
) -> DataFrame:
    """
    Normalized TF-IDF = TF-IDF / Document Vector Length

    A document whose terms all appear in every document has
    length 0 (every IDF is 0); its normalized weights are 0.
    """

    return (
        tfidf
        .join(document_lengths, on="document_id", how="inner")
        .withColumn(
            "normalized_tfidf",
            when(
                col("vector_length") > 0,
                col("tfidf") / col("vector_length")
            ).otherwise(lit(0.0))
        )
        .select(
            "term",
            "document_id",
            "tfidf",
            "vector_length",
            "normalized_tfidf"
        )
    )


def create_matrix(
    data: DataFrame,
    value_column: str,
    document_ids: list
) -> DataFrame:
    """
    Long format -> term-document matrix.

    Rows = terms, columns = documents (in the given order),
    missing values = 0.
    """

    return (
        data
        .groupBy("term")
        .pivot("document_id", document_ids)
        .agg(first(col(value_column)))
        .na.fill(0)
        .orderBy("term")
    )