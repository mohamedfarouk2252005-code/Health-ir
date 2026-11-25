from pyspark.sql import SparkSession

from src.ingest import load_documents
from src.positional_index import (
    build_positional_index,
    display_positional_index
)
from src.tf_df_idf import (
    calculate_raw_tf,
    calculate_weighted_tf,
    calculate_df_idf
)
from src.tfidf_norm import (
    calculate_tfidf,
    calculate_document_lengths,
    normalize_tfidf,
    create_matrix
)
from src.query_engine import (
    phrase_search,
    display_phrase_results,
    boolean_search,
    display_boolean_results,
    rank_documents,
    display_ranking
)
from src.display import (
    print_dataframe,
    save_dataframe_csv,
    save_positional_index_json,
    save_phrase_results_csv,
    save_boolean_results_csv,
    save_ranking_results_csv,
    save_query_vectors_csv
)
from src.config import PHRASE_QUERIES, BOOLEAN_QUERIES
from src.utils import natural_key


# =====================================================
# INDEX CONSTRUCTION
# =====================================================

def build_pipeline(spark: SparkSession, folder) -> dict:
    """
    Read the dataset folder with Spark and build every structure:
    positional index, TF, DF/IDF, TF-IDF, lengths, normalized vectors.

    Returns a context dictionary used by the query functions.
    """

    spark.catalog.clearCache()

    documents = load_documents(spark, folder).cache()

    document_ids = sorted(
        (row["document_id"] for row in documents.select("document_id").collect()),
        key=natural_key
    )

    num_documents = len(document_ids)

    positional_index = build_positional_index(documents).cache()

    raw_tf = calculate_raw_tf(positional_index).cache()
    weighted_tf = calculate_weighted_tf(raw_tf).cache()
    df_idf = calculate_df_idf(raw_tf, num_documents).cache()

    tfidf = calculate_tfidf(weighted_tf, df_idf).cache()
    document_lengths = calculate_document_lengths(tfidf).cache()
    normalized = normalize_tfidf(tfidf, document_lengths).cache()

    return {
        "folder": folder,
        "document_ids": document_ids,
        "num_documents": num_documents,
        "num_terms": df_idf.count(),
        "positional_index": positional_index,
        "raw_tf": raw_tf,
        "weighted_tf": weighted_tf,
        "df_idf": df_idf,
        "tfidf": tfidf,
        "document_lengths": document_lengths,
        "normalized": normalized,
        "raw_tf_matrix": create_matrix(raw_tf, "tf", document_ids),
        "weighted_tf_matrix": create_matrix(
            weighted_tf, "weighted_tf", document_ids
        ),
        "tfidf_matrix": create_matrix(tfidf, "tfidf", document_ids),
        "normalized_matrix": create_matrix(
            normalized, "normalized_tfidf", document_ids
        )
    }


def new_session() -> dict:
    """Holds every query executed so far (used for the CSV outputs)."""

    return {"phrase": {}, "boolean": {}, "ranking": {}}


# =====================================================
# TABLE DISPLAY / SAVING
# =====================================================

TABLE_MENU = [
    ("Positional index", None),
    ("Raw TF matrix", "raw_tf_matrix"),
    ("Weighted TF matrix", "weighted_tf_matrix"),
    ("DF and IDF", "df_idf"),
    ("TF-IDF matrix", "tfidf_matrix"),
    ("Document vector lengths", "document_lengths"),
    ("Normalized TF-IDF matrix", "normalized_matrix"),
]


def show_table(ctx: dict, number: int):
    """Print one table (1-7) on screen."""

    title, key = TABLE_MENU[number - 1]

    if key is None:
        display_positional_index(ctx["positional_index"])
    else:
        print_dataframe(ctx[key], title.upper())


def show_all_tables(ctx: dict):
    for number in range(1, len(TABLE_MENU) + 1):
        show_table(ctx, number)


def save_calculation_outputs(ctx: dict):
    """Save index and calculation tables to the output folder."""

    save_positional_index_json(ctx["positional_index"])
    save_dataframe_csv(ctx["raw_tf_matrix"], "raw_tf.csv")
    save_dataframe_csv(ctx["weighted_tf_matrix"], "weighted_tf.csv")
    save_dataframe_csv(ctx["df_idf"], "df_idf.csv")
    save_dataframe_csv(ctx["tfidf_matrix"], "tfidf.csv")
    save_dataframe_csv(ctx["document_lengths"], "document_lengths.csv")
    save_dataframe_csv(ctx["normalized_matrix"], "normalized_tfidf.csv")


def save_query_outputs(session: dict):
    """Save all queries executed so far in this session."""

    save_phrase_results_csv(session["phrase"])
    save_boolean_results_csv(session["boolean"])
    save_ranking_results_csv(session["ranking"])
    save_query_vectors_csv(session["ranking"])


# =====================================================
# QUERIES
# =====================================================

def run_phrase_query(ctx, session, phrase, label="PHRASE QUERY"):
    """Phrase (or single-term) search, then rank the matches."""

    results = phrase_search(ctx["positional_index"], phrase)

    display_phrase_results(phrase, results, label)

    ranking = rank_documents(
        phrase,
        ctx["df_idf"],
        ctx["normalized"],
        [r["document_id"] for r in results]
    )

    display_ranking(ranking)

    session["phrase"][phrase] = results
    session["ranking"][phrase] = ranking


def run_term_query(ctx, session, term):
    """Single-term search."""

    if len(term.split()) != 1:
        raise ValueError(
            "Term search takes ONE word. Use phrase search for several words."
        )

    run_phrase_query(ctx, session, term, label="TERM SEARCH")


def run_boolean_query(ctx, session, query):
    """AND / AND NOT query, then rank the returned documents."""

    results = boolean_search(ctx["positional_index"], query)

    display_boolean_results(results)

    if results["operator"] == "AND":
        scoring_query = results["first_phrase"] + " " + results["second_phrase"]
    else:
        # AND NOT is scored with the positive phrase only
        scoring_query = results["first_phrase"]

    ranking = rank_documents(
        scoring_query,
        ctx["df_idf"],
        ctx["normalized"],
        results["matched_documents"]
    )

    ranking["query"] = query

    display_ranking(ranking)

    session["boolean"][query] = results
    session["ranking"][query] = ranking


def run_assigned_queries(ctx, session):
    """Run the four assigned queries from config.py."""

    for phrase in PHRASE_QUERIES:
        run_phrase_query(ctx, session, phrase)

    for query in BOOLEAN_QUERIES:
        run_boolean_query(ctx, session, query)