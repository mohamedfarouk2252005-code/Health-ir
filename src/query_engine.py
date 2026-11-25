import re

from collections import Counter
from math import log10, sqrt, fsum
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def phrase_search(
    positional_index: DataFrame,
    phrase: str
) -> list:
    """
    Find documents containing an exact contiguous phrase.

    Uses the positional index to check whether query terms
    appear consecutively and in the correct order.

    Returns:
        A list containing:
        - document_id
        - starting_positions
        - phrase_ranges
    """

    # Step 1: Normalize the query
    terms = phrase.lower().split()

    if not terms:
        raise ValueError("Phrase query cannot be empty.")

    # Step 2: Retrieve only the postings for query terms
    required_terms = list(set(terms))

    rows = (
        positional_index
        .filter(col("term").isin(required_terms))
        .select("term", "document_id", "positions")
        .collect()
    )

    # Step 3: Organize the positional information
    postings = {}

    for row in rows:

        if row["term"] not in postings:
            postings[row["term"]] = {}

        postings[row["term"]][row["document_id"]] = set(
            row["positions"]
        )

    # Step 4: Check whether all query terms exist
    for term in terms:

        if term not in postings:
            return []

    # Step 5: Find documents containing every query term
    candidate_documents = set(
        postings[terms[0]].keys()
    )

    for term in terms[1:]:

        candidate_documents.intersection_update(
            postings[term].keys()
        )

    # Step 6: Check consecutive positions
    results = []

    for document_id in candidate_documents:

        first_positions = postings[terms[0]][document_id]

        starting_positions = []

        for start in first_positions:

            match = True

            for offset, term in enumerate(terms):

                expected_position = start + offset

                if expected_position not in postings[term][document_id]:

                    match = False
                    break

            if match:
                starting_positions.append(start)

        if starting_positions:

            starting_positions.sort()

            phrase_ranges = [
                f"{start}-{start + len(terms) - 1}"
                for start in starting_positions
            ]

            results.append({
                "document_id": document_id,
                "starting_positions": starting_positions,
                "phrase_ranges": phrase_ranges
            })

    # Step 7: Sort documents numerically
    results.sort(
        key=lambda result: int(result["document_id"][1:])
    )

    return results


def display_phrase_results(
    phrase: str,
    results: list
):
    """
    Display phrase-search results.
    """

    print(f"\n========== PHRASE QUERY: {phrase} ==========\n")

    if not results:

        print("No matching documents found.")

        return

    for result in results:

        print(f"Document: {result['document_id']}")

        print(
            f"Starting positions: {result['starting_positions']}"
        )

        print(
            f"Phrase ranges: {result['phrase_ranges']}"
        )

        print()


def parse_boolean_query(query: str) -> tuple:
    """
    Parse a Boolean query into its two phrases and operator.

    Supported:
        phrase1 AND phrase2
        phrase1 AND NOT phrase2
    """

    pattern = r"^\s*(.*?)\s+AND\s+(NOT\s+)?(.*?)\s*$"

    match = re.fullmatch(pattern, query, re.IGNORECASE)

    if not match:
        raise ValueError(
            "Invalid Boolean query. Use AND or AND NOT."
        )

    first_phrase = match.group(1).strip().lower()
    is_not = match.group(2) is not None
    second_phrase = match.group(3).strip().lower()

    if not first_phrase or not second_phrase:
        raise ValueError("Both phrases must be provided.")

    operator = "AND NOT" if is_not else "AND"

    return first_phrase, operator, second_phrase


def boolean_search(
    positional_index: DataFrame,
    query: str
) -> dict:
    """
    Execute Boolean phrase queries using the positional index.

    Supports AND and AND NOT.
    """

    # Parse the Boolean query
    first_phrase, operator, second_phrase = (
        parse_boolean_query(query)
    )

    # Execute both phrase searches
    first_results = phrase_search(
        positional_index,
        first_phrase
    )

    second_results = phrase_search(
        positional_index,
        second_phrase
    )

    # Extract matching document IDs
    first_documents = {
        result["document_id"]
        for result in first_results
    }

    second_documents = {
        result["document_id"]
        for result in second_results
    }

    # Apply Boolean operation
    if operator == "AND":

        matched_documents = (
            first_documents.intersection(second_documents)
        )

    else:

        matched_documents = (
            first_documents.difference(second_documents)
        )

    # Sort documents numerically
    matched_documents = sorted(
        matched_documents,
        key=lambda document_id: int(document_id[1:])
    )

    return {
        "query": query,
        "operator": operator,
        "first_phrase": first_phrase,
        "second_phrase": second_phrase,
        "first_results": first_results,
        "second_results": second_results,
        "matched_documents": matched_documents
    }


def display_boolean_results(results: dict):
    """
    Display Boolean query results.
    """

    print(f"\n========== BOOLEAN QUERY ==========")

    print(f"\nQuery: {results['query']}")

    print(f"Operator: {results['operator']}")

    print("\nMatching Documents:")

    if results["matched_documents"]:

        for document_id in results["matched_documents"]:

            print(f"    {document_id}")

    else:

        print("    No matching documents found.")

    print("\n===================================")


def build_query_vector(
    query: str,
    df_idf: DataFrame
) -> dict:
    """
    Build and normalize the query TF-IDF vector.

    Query TF-IDF = Weighted Query TF * Collection IDF

    Query vectors use the same IDF values as document vectors.
    """

    # Step 1: Tokenize query
    terms = query.lower().split()

    if not terms:
        raise ValueError("Query cannot be empty.")

    # Step 2: Calculate query term frequencies
    query_tf = Counter(terms)

    # Step 3: Retrieve IDF values
    idf_rows = df_idf.select("term", "idf").collect()

    idf_lookup = {
        row["term"]: float(row["idf"])
        for row in idf_rows
    }

    # Ensure all query terms exist in the collection
    missing_terms = set(query_tf) - set(idf_lookup)

    if missing_terms:
        raise ValueError(
            f"Query terms not found in collection: {missing_terms}"
        )

    # Step 4: Calculate weighted TF and query TF-IDF
    query_weights = {}

    for term, tf in query_tf.items():

        weighted_tf = 1 + log10(tf)

        idf = idf_lookup[term]

        query_tfidf = weighted_tf * idf

        query_weights[term] = {
            "query_tf": tf,
            "weighted_tf": weighted_tf,
            "idf": idf,
            "query_tfidf": query_tfidf
        }

    # Step 5: Calculate query vector length
    query_length = sqrt(
        fsum(
            values["query_tfidf"] ** 2
            for values in query_weights.values()
        )
    )

    if query_length == 0:
        raise ValueError("Query vector has zero length.")

    # Step 6: Normalize query vector
    normalized_weights = {}

    for term, values in query_weights.items():

        normalized_weights[term] = (
            values["query_tfidf"] / query_length
        )

    return {
        "query": query,
        "weights": query_weights,
        "vector_length": query_length,
        "normalized_weights": normalized_weights
    }


def calculate_similarity(
    query_vector: dict,
    normalized_vectors: DataFrame,
    matching_documents: list
) -> list:
    """
    Calculate cosine similarity between the normalized
    query vector and matching normalized document vectors.

    Similarity = Normalized Query . Normalized Document
    """

    if not matching_documents:
        return []

    # Step 1: Retrieve vectors for matching documents
    rows = (
        normalized_vectors
        .filter(
            col("document_id").isin(matching_documents)
        )
        .select(
            "document_id",
            "term",
            "normalized_tfidf"
        )
        .collect()
    )

    # Step 2: Organize document vectors
    document_vectors = {}

    for row in rows:

        document_id = row["document_id"]
        term = row["term"]

        if document_id not in document_vectors:
            document_vectors[document_id] = {}

        document_vectors[document_id][term] = float(
            row["normalized_tfidf"]
        )

    # Step 3: Calculate dot product
    scores = []

    for document_id in matching_documents:

        document_vector = document_vectors.get(
            document_id, {}
        )

        similarity = fsum(
            query_weight * document_vector.get(term, 0.0)
            for term, query_weight
            in query_vector["normalized_weights"].items()
        )

        scores.append({
            "document_id": document_id,
            "similarity": similarity
        })

    # Step 4: Sort by similarity (highest first)
    scores.sort(
        key=lambda result: (
            -result["similarity"],
            int(result["document_id"][1:])
        )
    )

    return scores


def rank_documents(
    query: str,
    df_idf: DataFrame,
    normalized_vectors: DataFrame,
    matching_documents: list
) -> dict:
    """
    Build the query vector, calculate similarity
    and rank matching documents.
    """

    # Build normalized query vector
    query_vector = build_query_vector(
        query,
        df_idf
    )

    # Calculate similarity
    scores = calculate_similarity(
        query_vector,
        normalized_vectors,
        matching_documents
    )

    # Return results
    return {
        "query": query,
        "query_vector": query_vector,
        "scores": scores
    }


def display_ranking(results: dict):
    """
    Display query weights, similarity scores and rankings.
    """

    query_vector = results["query_vector"]

    print(f"\n========== RANKING: {results['query']} ==========\n")

    print(
        f"Query Vector Length: "
        f"{query_vector['vector_length']:.6f}"
    )

    print("\nQuery Vector:")

    print(
        f"{'Term':<15}"
        f"{'TF':<10}"
        f"{'Weighted TF':<15}"
        f"{'Query TF-IDF':<15}"
        f"{'Normalized':<15}"
    )

    for term, values in sorted(
        query_vector["weights"].items()
    ):

        normalized = query_vector["normalized_weights"][term]

        print(
            f"{term:<15}"
            f"{values['query_tf']:<10}"
            f"{values['weighted_tf']:<15.4f}"
            f"{values['query_tfidf']:<15.4f}"
            f"{normalized:<15.4f}"
        )

    print("\nSimilarity Scores:")

    for result in results["scores"]:

        print(
            f"{result['document_id']}: "
            f"{result['similarity']:.4f}"
        )

    print("\nRanking:")

    ranking = [
        result["document_id"]
        for result in results["scores"]
    ]

    print(" > ".join(ranking) if ranking else "No matching documents.")

    print("\n==============================================")
