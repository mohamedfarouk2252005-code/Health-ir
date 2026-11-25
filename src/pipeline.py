from src.spark_session import create_spark_session
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
    save_dataframe_csv,
    save_positional_index_json,
    save_phrase_results_csv,
    save_boolean_results_csv,
    save_ranking_results_csv,
    save_query_vectors_csv
)

from src.config import (
    PHRASE_QUERIES,
    BOOLEAN_QUERIES
)


def run_pipeline():

    spark = create_spark_session()

    try:

        print("\n========================================")
        print(" HEALTHCARE INFORMATION RETRIEVAL SYSTEM")
        print("========================================")

        # ====================================
        # 1. LOAD DATASET
        # ====================================

        print("\n[1] Loading Healthcare Dataset...")

        documents = load_documents(spark).cache()

        print("Documents loaded:", documents.count())

        # ====================================
        # 2. BUILD POSITIONAL INDEX
        # ====================================

        print("\n[2] Building Positional Index...")

        positional_index = build_positional_index(
            documents
        ).cache()

        print(
            "Unique terms:",
            positional_index.select("term").distinct().count()
        )

        save_positional_index_json(positional_index)

        # ====================================
        # 3. CALCULATE TERM STATISTICS
        # ====================================

        print("\n[3] Calculating TF, Weighted TF, DF and IDF...")

        raw_tf = calculate_raw_tf(
            positional_index
        ).cache()

        weighted_tf = calculate_weighted_tf(
            raw_tf
        ).cache()

        df_idf = calculate_df_idf(
            raw_tf
        ).cache()

        # ====================================
        # 4. CALCULATE TF-IDF
        # ====================================

        print("\n[4] Calculating TF-IDF...")

        tfidf = calculate_tfidf(
            weighted_tf,
            df_idf
        ).cache()

        # ====================================
        # 5. DOCUMENT VECTOR LENGTHS
        # ====================================

        print("\n[5] Calculating Document Vector Lengths...")

        document_lengths = calculate_document_lengths(
            tfidf
        ).cache()

        # ====================================
        # 6. NORMALIZATION
        # ====================================

        print("\n[6] Normalizing Document Vectors...")

        normalized = normalize_tfidf(
            tfidf,
            document_lengths
        ).cache()

        # ====================================
        # 7. GENERATE MATRICES
        # ====================================

        print("\n[7] Generating Matrices...")

        raw_tf_matrix = create_matrix(
            raw_tf,
            "tf"
        )

        weighted_tf_matrix = create_matrix(
            weighted_tf,
            "weighted_tf"
        )

        tfidf_matrix = create_matrix(
            tfidf,
            "tfidf"
        )

        normalized_matrix = create_matrix(
            normalized,
            "normalized_tfidf"
        )

        # ====================================
        # 8. SAVE CALCULATION OUTPUTS
        # ====================================

        print("\n[8] Saving Calculation Outputs...")

        save_dataframe_csv(
            raw_tf_matrix,
            "raw_tf.csv"
        )

        save_dataframe_csv(
            weighted_tf_matrix,
            "weighted_tf.csv"
        )

        save_dataframe_csv(
            df_idf,
            "df_idf.csv"
        )

        save_dataframe_csv(
            tfidf_matrix,
            "tfidf.csv"
        )

        save_dataframe_csv(
            document_lengths,
            "document_lengths.csv"
        )

        save_dataframe_csv(
            normalized_matrix,
            "normalized_tfidf.csv"
        )

        # ====================================
        # 9. EXECUTE PHRASE QUERIES
        # ====================================

        print("\n[9] Executing Phrase Queries...")

        phrase_results = {}

        for phrase in PHRASE_QUERIES:

            results = phrase_search(
                positional_index,
                phrase
            )

            phrase_results[phrase] = results

            display_phrase_results(
                phrase,
                results
            )

        save_phrase_results_csv(
            phrase_results
        )

        # ====================================
        # 10. EXECUTE BOOLEAN QUERIES
        # ====================================

        print("\n[10] Executing Boolean Queries...")

        boolean_results = {}

        for query in BOOLEAN_QUERIES:

            results = boolean_search(
                positional_index,
                query
            )

            boolean_results[query] = results

            display_boolean_results(
                results
            )

        save_boolean_results_csv(
            boolean_results
        )

        # ====================================
        # 11. SIMILARITY AND RANKING
        # ====================================

        print("\n[11] Calculating Similarity and Ranking...")

        ranking_results = {}

        # Rank phrase query results
        for phrase, results in phrase_results.items():

            matching_documents = [
                result["document_id"]
                for result in results
            ]

            ranking = rank_documents(
                phrase,
                df_idf,
                normalized,
                matching_documents
            )

            ranking_results[phrase] = ranking

            display_ranking(ranking)

        # Rank Boolean query results
        for query, results in boolean_results.items():

            if results["operator"] == "AND":

                scoring_query = (
                    results["first_phrase"]
                    + " "
                    + results["second_phrase"]
                )

            else:

                # AND NOT uses only the positive phrase
                scoring_query = results["first_phrase"]

            ranking = rank_documents(
                scoring_query,
                df_idf,
                normalized,
                results["matched_documents"]
            )

            # Preserve the original Boolean query for reporting
            ranking["query"] = query

            ranking_results[query] = ranking

            display_ranking(ranking)

        # ====================================
        # 12. SAVE QUERY RESULTS
        # ====================================

        print("\n[12] Saving Query and Ranking Outputs...")

        save_ranking_results_csv(
            ranking_results
        )

        save_query_vectors_csv(
            ranking_results
        )

        # ====================================
        # FINISHED
        # ====================================

        print("\n========================================")
        print(" PROJECT EXECUTED SUCCESSFULLY!")
        print("========================================")

        print("\nAll outputs are available in the output folder.")

    finally:

        spark.stop()
