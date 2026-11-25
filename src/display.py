import csv
import json

from pyspark.sql import DataFrame

from src.config import OUTPUT_DIR
from src.utils import natural_key


# =====================================================
# HELPERS
# =====================================================

def ensure_output_directory():
    """Create output directory if it doesn't exist."""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def collect_sorted_rows(dataframe: DataFrame) -> list:
    """
    Collect a small result DataFrame and sort it by
    term (alphabetical) then document_id (natural order).
    """

    sort_columns = [
        c for c in ("term", "document_id") if c in dataframe.columns
    ]

    return sorted(
        dataframe.collect(),
        key=lambda row: tuple(natural_key(row[c]) for c in sort_columns)
    )


def format_value(value) -> str:
    """Floats use 4 decimals, lists/dicts are JSON."""

    if isinstance(value, float):
        return f"{value:.4f}"

    if isinstance(value, (list, dict)):
        return json.dumps(value)

    return str(value)


# =====================================================
# CONSOLE TABLES
# =====================================================

def print_dataframe(dataframe: DataFrame, title: str):
    """
    Print any small Spark DataFrame as an aligned table.
    """

    columns = dataframe.columns

    table = [
        [format_value(row[c]) for c in columns]
        for row in collect_sorted_rows(dataframe)
    ]

    widths = [
        max(len(columns[i]), *(len(r[i]) for r in table))
        if table else len(columns[i])
        for i in range(len(columns))
    ]

    print(f"\n========== {title} ==========\n")

    print("  ".join(f"{c:<{w}}" for c, w in zip(columns, widths)))
    print("  ".join("-" * w for w in widths))

    for row in table:
        print("  ".join(f"{v:<{w}}" for v, w in zip(row, widths)))

    print(f"\n({len(table)} rows)")


# =====================================================
# FILE OUTPUT
# =====================================================

def save_dataframe_csv(dataframe: DataFrame, filename: str):
    """
    Save a Spark DataFrame as CSV (floats shown to 4 decimals).
    """

    ensure_output_directory()

    output_path = OUTPUT_DIR / filename

    with open(output_path, "w", newline="", encoding="utf-8") as file:

        writer = csv.writer(file)

        writer.writerow(dataframe.columns)

        for row in collect_sorted_rows(dataframe):
            writer.writerow([
                f"{v:.4f}" if isinstance(v, float)
                else json.dumps(v) if isinstance(v, (list, dict))
                else v
                for v in row
            ])

    print(f"Saved: {output_path.name}")


def save_positional_index_json(positional_index: DataFrame):
    """
    Save the positional index as JSON: term -> document -> positions.
    """

    ensure_output_directory()

    index = {}

    for row in collect_sorted_rows(positional_index):
        index.setdefault(row["term"], {})[row["document_id"]] = (
            row["positions"]
        )

    output_path = OUTPUT_DIR / "positional_index.json"

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(index, file, indent=4)

    print(f"Saved: {output_path.name}")


def save_phrase_results_csv(phrase_results: dict):
    """Save phrase / term query results."""

    ensure_output_directory()

    output_path = OUTPUT_DIR / "phrase_query_results.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as file:

        writer = csv.writer(file)

        writer.writerow([
            "query", "document_id", "starting_positions", "phrase_ranges"
        ])

        for query, results in phrase_results.items():
            for result in results:
                writer.writerow([
                    query,
                    result["document_id"],
                    json.dumps(result["starting_positions"]),
                    json.dumps(result["phrase_ranges"])
                ])

    print(f"Saved: {output_path.name}")


def save_ranking_results_csv(ranking_results: dict):
    """Save similarity scores and document rankings."""

    ensure_output_directory()

    output_path = OUTPUT_DIR / "ranking_results.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as file:

        writer = csv.writer(file)

        writer.writerow([
            "query", "query_vector_length", "document_id",
            "similarity", "rank"
        ])

        for query, results in ranking_results.items():
            for rank, score in enumerate(results["scores"], start=1):
                writer.writerow([
                    query,
                    f"{results['query_vector']['vector_length']:.6f}",
                    score["document_id"],
                    f"{score['similarity']:.4f}",
                    rank
                ])

    print(f"Saved: {output_path.name}")


def save_query_vectors_csv(ranking_results: dict):
    """Save query TF-IDF weights and normalized query weights."""

    ensure_output_directory()

    output_path = OUTPUT_DIR / "query_vectors.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as file:

        writer = csv.writer(file)

        writer.writerow([
            "query", "term", "query_tf", "weighted_tf",
            "query_tfidf", "normalized_weight"
        ])

        for query, results in ranking_results.items():

            query_vector = results["query_vector"]

            for term, values in query_vector["weights"].items():
                writer.writerow([
                    query,
                    term,
                    values["query_tf"],
                    f"{values['weighted_tf']:.4f}",
                    f"{values['query_tfidf']:.4f}",
                    f"{query_vector['normalized_weights'][term]:.4f}"
                ])

    print(f"Saved: {output_path.name}")


def save_boolean_results_csv(boolean_results: dict):
    """Save Boolean query results."""

    ensure_output_directory()

    output_path = OUTPUT_DIR / "boolean_query_results.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as file:

        writer = csv.writer(file)

        writer.writerow([
            "query", "operator", "first_phrase",
            "second_phrase", "matched_document"
        ])

        for query, results in boolean_results.items():

            for document_id in (results["matched_documents"] or [""]):
                writer.writerow([
                    query,
                    results["operator"],
                    results["first_phrase"],
                    results["second_phrase"],
                    document_id
                ])

    print(f"Saved: {output_path.name}")