from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    split,
    posexplode,
    collect_list,
    sort_array,
    regexp_extract
)


def build_positional_index(documents_df: DataFrame) -> DataFrame:
    """
    Build a positional index using Spark.

    Returns a DataFrame containing:
        term
        document_id
        positions
    """

    # Step 1: Convert document text into lowercase tokens
    # Step 2: Assign zero-based positions using posexplode

    tokens_df = documents_df.select(
        col("document_id"),
        posexplode(
            split(
                lower(trim(col("text"))),
                r"\s+"
            )
        ).alias("zero_based_position", "term")
    )

    # Step 3: Convert positions to 1-based indexing

    token_positions_df = tokens_df.select(
        col("term"),
        col("document_id"),
        (col("zero_based_position") + 1).alias("position")
    )

    # Step 4: Group occurrences by term and document
    # Step 5: Collect and sort all positions

    positional_index = (
        token_positions_df
        .groupBy("term", "document_id")
        .agg(
            sort_array(
                collect_list("position")
            ).alias("positions")
        )
    )

    # Step 6: Sort terms alphabetically
    # Sort document IDs numerically (d1, d2, ..., d10)

    positional_index = positional_index.orderBy(
        col("term").asc(),
        regexp_extract(
            col("document_id"),
            r"(\d+)",
            1
        ).cast("int").asc()
    )

    return positional_index


def display_positional_index(positional_index: DataFrame):
    """
    Display the positional index in a readable format.
    """

    print("\n========== POSITIONAL INDEX ==========\n")

    current_term = None

    rows = positional_index.collect()

    for row in rows:

        if row["term"] != current_term:

            current_term = row["term"]

            print(f"\n{current_term}")

        print(
            f"    {row['document_id']}: {row['positions']}"
        )

    print("\n======================================")
