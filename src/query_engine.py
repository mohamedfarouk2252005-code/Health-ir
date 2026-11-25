import re

from collections import Counter
from math import log10, sqrt, fsum
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.utils import natural_key


# =====================================================
# PHRASE / TERM SEARCH
# =====================================================

def phrase_search(
    positional_index: DataFrame,
    phrase: str
) -> list:
    """
    Find documents containing an exact contiguous phrase.

    A single word is a phrase of length 1, so this function
    also performs plain term search.

    Returns a list of dicts:
        document_id, starting_positions, phrase_ranges
    """

    # Step 1: normalize the query
    terms = phrase.lower().split()

    if not terms:
        raise ValueError("Query cannot be empty.")

    # Step 2: retrieve only the postings of the query terms
    rows = (
        positional_index
        .filter(col("term").isin(list(set(terms))))
        .select("term", "document_id", "positions")
        .collect()
    )

    # Step 3: organize positional information
    postings = {}

    for row in rows:
        postings.setdefault(row["term"], {})[row["document_id"]] = set(
            row["positions"]
        )

    # Step 4: every query term must exist in the collection
    for term in terms:
        if term not in postings:
            return []

    # Step 5: documents containing every query term
    candidate_documents = set(postings[terms[0]].keys())

    for term in terms[1:]:
        candidate_documents.intersection_update(postings[term].keys())

    # Step 6: check consecutive positions
    results = []

    for document_id in candidate_documents:

        starting_positions = []

        for start in postings[terms[0]][document_id]:

            if all(
                (start + offset) in postings[term][document_id]
                for offset, term in enumerate(terms)
            ):
                starting_positions.append(start)

        if starting_positions:

            starting_positions.sort()

            results.append({
                "document_id": document_id,
                "starting_positions": starting_positions,
                "phrase_ranges": [
                    f"{start}-{start + len(terms) - 1}"
                    for start in starting_positions
                ]
            })

    # Step 7: documents in natural order
    results.sort(key=lambda r: natural_key(r["document_id"]))

    return results


def display_phrase_results(
    phrase: str,
    results: list,
    label: str = "PHRASE QUERY"
):
    """
    Display phrase / term search results.
    """

    print(f"\n========== {label}: {phrase} ==========\n")

    if not results:
        print("No matching documents found.")
        return

    for result in results:
        print(f"Document: {result['document_id']}")
        print(f"Starting positions: {result['starting_positions']}")
        print(f"Phrase ranges: {result['phrase_ranges']}")
        print()


# =====================================================
# BOOLEAN SEARCH
# =====================================================

def parse_boolean_query(query: str) -> tuple:
    """
    Parse:  phrase1 AND phrase2
            phrase1 AND NOT phrase2

    The operators must be written in UPPERCASE, so a lowercase
    "and" (a real word in the dataset) is treated as a normal term.
    """

    pattern = r"^\s*(.*?)\s+AND\s+(NOT\s+)?(.*?)\s*$"

    match = re.fullmatch(pattern, query)

    if not match:
        raise ValueError(
            "Invalid Boolean query. Use: phrase1 AND phrase2  "
            "or  phrase1 AND NOT phrase2  (operators in UPPERCASE)."
        )

    first_phrase = match.group(1).strip().lower()
    is_not = match.group(2) is not None
    second_phrase = match.group(3).strip().lower()

    if not first_phrase or not second_phrase:
        raise ValueError("Both phrases must be provided.")

    if re.search(r"\bAND\b", match.group(3)):
        raise ValueError(
            "Only one AND / AND NOT operator is supported per query."
        )

    return first_phrase, ("AND NOT" if is_not else "AND"), second_phrase


def boolean_search(
    positional_index: DataFrame,
    query: str
) -> dict:
    """
    Execute Boolean phrase queries (AND, AND NOT).
    """

    first_phrase, operator, second_phrase = parse_boolean_query(query)

    first_results = phrase_search(positional_index, first_phrase)
    second_results = phrase_search(positional_index, second_phrase)

    first_documents = {r["document_id"] for r in first_results}
    second_documents = {r["document_id"] for r in second_results}

    if operator == "AND":
        matched = first_documents & second_documents
    else:
        matched = first_documents - second_documents

    return {
        "query": query,
        "operator": operator,
        "first_phrase": first_phrase,
        "second_phrase": second_phrase,
        "first_results": first_results,
        "second_results": second_results,
        "matched_documents": sorted(matched, key=natural_key)
    }


def display_boolean_results(results: dict):
    """
    Display Boolean query results.
    """

    print("\n========== BOOLEAN QUERY ==========")
    print(f"\nQuery: {results['query']}")
    print(f"Operator: {results['operator']}")
    print(f"Phrase 1 '{results['first_phrase']}' matched: "
          f"{[r['document_id'] for r in results['first_results']]}")
    print(f"Phrase 2 '{results['second_phrase']}' matched: "
          f"{[r['document_id'] for r in results['second_results']]}")

    print("\nMatching Documents:")

    if results["matched_documents"]:
        for document_id in results["matched_documents"]:
            print(f"    {document_id}")
    else:
        print("    No matching documents found.")

    print("\n===================================")


# =====================================================
# QUERY VECTOR, SIMILARITY, RANKING
# =====================================================

def build_query_vector(
    query: str,
    df_idf: DataFrame
) -> dict:
    """
    Build and normalize the query TF-IDF vector.

    Query TF-IDF = (1 + log10(query TF)) * collection IDF

    Terms that do not exist in the collection are ignored
    (they cannot contribute to any similarity score).
    """

    terms = query.lower().split()

    if not terms:
        raise ValueError("Query cannot be empty.")

    query_tf = Counter(terms)

    idf_lookup = {
        row["term"]: float(row["idf"])
        for row in df_idf.select("term", "idf").collect()
    }

    ignored_terms = sorted(set(query_tf) - set(idf_lookup))

    query_weights = {}

    for term, tf in query_tf.items():

        if term not in idf_lookup:
            continue

        weighted_tf = 1 + log10(tf)
        idf = idf_lookup[term]

        query_weights[term] = {
            "query_tf": tf,
            "weighted_tf": weighted_tf,
            "idf": idf,
            "query_tfidf": weighted_tf * idf
        }

    query_length = sqrt(
        fsum(v["query_tfidf"] ** 2 for v in query_weights.values())
    )

    # A zero-length vector (no known terms, or only terms found in
    # every document) gives similarity 0 instead of crashing.
    normalized_weights = {
        term: (v["query_tfidf"] / query_length if query_length > 0 else 0.0)
        for term, v in query_weights.items()
    }

    return {
        "query": query,
        "weights": query_weights,
        "vector_length": query_length,
        "normalized_weights": normalized_weights,
        "ignored_terms": ignored_terms
    }


def calculate_similarity(
    query_vector: dict,
    normalized_vectors: DataFrame,
    matching_documents: list
) -> list:
    """
    Similarity = normalized query vector . normalized document vector
    """

    if not matching_documents:
        return []

    rows = (
        normalized_vectors
        .filter(col("document_id").isin(matching_documents))
        .select("document_id", "term", "normalized_tfidf")
        .collect()
    )

    document_vectors = {}

    for row in rows:
        document_vectors.setdefault(row["document_id"], {})[row["term"]] = (
            float(row["normalized_tfidf"])
        )

    scores = []

    for document_id in matching_documents:

        document_vector = document_vectors.get(document_id, {})

        similarity = fsum(
            weight * document_vector.get(term, 0.0)
            for term, weight in query_vector["normalized_weights"].items()
        )

        scores.append({
            "document_id": document_id,
            "similarity": similarity
        })

    # Highest similarity first; ties in natural document order
    scores.sort(
        key=lambda r: (-r["similarity"], natural_key(r["document_id"]))
    )

    return scores


def rank_documents(
    query: str,
    df_idf: DataFrame,
    normalized_vectors: DataFrame,
    matching_documents: list
) -> dict:
    """
    Build the query vector, calculate similarity, rank documents.
    """

    query_vector = build_query_vector(query, df_idf)

    scores = calculate_similarity(
        query_vector,
        normalized_vectors,
        matching_documents
    )

    return {
        "query": query,
        "query_vector": query_vector,
        "scores": scores
    }


def display_ranking(results: dict):
    """
    Display query weights, similarity scores and ranking.
    """

    query_vector = results["query_vector"]

    print(f"\n========== RANKING: {results['query']} ==========\n")

    print(f"Query Vector Length: {query_vector['vector_length']:.6f}")

    if query_vector["ignored_terms"]:
        print(
            "Ignored (not in collection): "
            f"{', '.join(query_vector['ignored_terms'])}"
        )

    print("\nQuery Vector:")

    print(
        f"{'Term':<15}"
        f"{'TF':<10}"
        f"{'Weighted TF':<15}"
        f"{'Query TF-IDF':<15}"
        f"{'Normalized':<15}"
    )

    for term, values in sorted(query_vector["weights"].items()):

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
        print(f"{result['document_id']}: {result['similarity']:.4f}")

    print("\nRanking:")

    ranking = [r["document_id"] for r in results["scores"]]

    print(" > ".join(ranking) if ranking else "No matching documents.")

    print("\n==============================================")