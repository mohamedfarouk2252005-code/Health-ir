from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lower,
    split,
    posexplode,
    collect_list,
    sort_array
)

from src.utils import natural_key


def build_positional_index(documents_df: DataFrame) -> DataFrame:
    """
    Build a positional index using Spark.

    Returns a DataFrame containing:
        term
        document_id
        positions   (sorted, 1-based)
    """

    # Steps 1-2: lowercase, split on whitespace, keep zero-based position
    tokens_df = documents_df.select(
        col("document_id"),
        posexplode(
            split(lower(col("text")), r"\s+")
        ).alias("zero_based_position", "term")
    ).filter(col("term") != "")

    # Step 3: convert to 1-based positions
    token_positions_df = tokens_df.select(
        col("term"),
        col("document_id"),
        (col("zero_based_position") + 1).alias("position")
    )

    # Steps 4-5: group by (term, document) and collect sorted positions
    positional_index = (
        token_positions_df
        .groupBy("term", "document_id")
        .agg(
            sort_array(
                collect_list("position")
            ).alias("positions")
        )
    )

    # Step 6: terms alphabetically (document order is handled on display)
    return positional_index.orderBy("term", "document_id")


def display_positional_index(positional_index: DataFrame):
    """
    Display the positional index: term -> document IDs and positions.
    """

    rows = sorted(
        positional_index.collect(),
        key=lambda row: (row["term"], natural_key(row["document_id"]))
    )

    print("\n========== POSITIONAL INDEX ==========\n")

    postings = {}

    for row in rows:
        postings.setdefault(row["term"], []).append(
            f"{row['document_id']}: "
            + ", ".join(str(p) for p in row["positions"])
        )

    width = max((len(term) for term in postings), default=4) + 2

    print(f"{'Term':<{width}}Document IDs and 1-based positions")
    print("-" * (width + 40))

    for term, entries in postings.items():
        print(f"{term:<{width}}" + "; ".join(entries))

    print(f"\nTotal terms: {len(postings)}")
    print("\n======================================")