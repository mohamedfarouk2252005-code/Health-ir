import csv
import json

from pathlib import Path
from pyspark.sql import DataFrame

from src.config import OUTPUT_DIR


def ensure_output_directory():
    """Create output directory if it doesn't exist."""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def save_dataframe_csv(
    dataframe: DataFrame,
    filename: str
):
    """
    Save a Spark DataFrame as a CSV file.

    Floating-point values are displayed to 4 decimal places.
    """

    ensure_output_directory()

    output_path = OUTPUT_DIR / filename

    rows = dataframe.collect()

    with open(
        output_path,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerow(dataframe.columns)

        for row in rows:

            values = []

            for value in row:

                if isinstance(value, float):
                    values.append(f"{value:.4f}")

                elif isinstance(value, (list, dict)):
                    values.append(json.dumps(value))

                else:
                    values.append(value)

            writer.writerow(values)

    print(f"Saved: {output_path.name}")


def save_positional_index_json(
    positional_index: DataFrame
):
    """
    Save the positional index as a JSON file.
    """

    ensure_output_directory()

    index = {}

    rows = positional_index.collect()

    for row in rows:

        term = row["term"]
        document_id = row["document_id"]
        positions = row["positions"]

        if term not in index:
            index[term] = {}

        index[term][document_id] = positions

    output_path = OUTPUT_DIR / "positional_index.json"

    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            index,
            file,
            indent=4
        )

    print(f"Saved: {output_path.name}")


def save_phrase_results_csv(
    phrase_results: dict
):
    """
    Save phrase query results.
    """

    ensure_output_directory()

    output_path = OUTPUT_DIR / "phrase_query_results.csv"

    with open(
        output_path,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerow([
            "query",
            "document_id",
            "starting_positions",
            "phrase_ranges"
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


def save_ranking_results_csv(
    ranking_results: dict
):
    """
    Save similarity scores and document rankings.
    """

    ensure_output_directory()

    output_path = OUTPUT_DIR / "ranking_results.csv"

    with open(
        output_path,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerow([
            "query",
            "query_vector_length",
            "document_id",
            "similarity",
            "rank"
        ])

        for query, results in ranking_results.items():

            for rank, score in enumerate(
                results["scores"],
                start=1
            ):

                writer.writerow([
                    query,
                    f"{results['query_vector']['vector_length']:.6f}",
                    score["document_id"],
                    f"{score['similarity']:.4f}",
                    rank
                ])

    print(f"Saved: {output_path.name}")


def save_query_vectors_csv(
    ranking_results: dict
):
    """
    Save query TF-IDF weights and normalized query weights.
    """

    ensure_output_directory()

    output_path = OUTPUT_DIR / "query_vectors.csv"

    with open(
        output_path,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerow([
            "query",
            "term",
            "query_tf",
            "weighted_tf",
            "query_tfidf",
            "normalized_weight"
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
    """
    Save Boolean query results.
    """

    ensure_output_directory()

    output_path = OUTPUT_DIR / "boolean_query_results.csv"

    with open(
        output_path,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerow([
            "query",
            "operator",
            "first_phrase",
            "second_phrase",
            "matched_document"
        ])

        for query, results in boolean_results.items():

            documents = results["matched_documents"]

            if documents:
                for document_id in documents:
                    writer.writerow([
                        query,
                        results["operator"],
                        results["first_phrase"],
                        results["second_phrase"],
                        document_id
                    ])

            else:
                writer.writerow([
                    query,
                    results["operator"],
                    results["first_phrase"],
                    results["second_phrase"],
                    ""
                ])

    print(f"Saved: {output_path.name}")
