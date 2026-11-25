from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, regexp_extract

from src.config import DATA_DIR, DOCUMENT_IDS


def load_documents(spark: SparkSession) -> DataFrame:
    """
    Load the healthcare documents and return a Spark DataFrame.

    Columns:
        document_id: Document identifier (d1, d2, ..., d10)
        text: Complete document content
    """

    # Check dataset directory
    if not DATA_DIR.exists():
        raise FileNotFoundError(
            f"Dataset directory not found: {DATA_DIR}"
        )

    # Discover text files
    files = list(DATA_DIR.glob("*.txt"))

    # Extract document IDs
    actual_ids = [file.stem for file in files]

    # Validate document IDs
    missing_ids = set(DOCUMENT_IDS) - set(actual_ids)
    unexpected_ids = set(actual_ids) - set(DOCUMENT_IDS)

    if missing_ids or unexpected_ids:
        raise ValueError(
            f"Missing documents: {sorted(missing_ids)}\n"
            f"Unexpected documents: {sorted(unexpected_ids)}"
        )

    # Read documents using Python
    records = []

    for file in sorted(
        files,
        key=lambda path: int(path.stem[1:])
    ):

        document_id = file.stem
        text = file.read_text(encoding="utf-8").strip()

        if not text:
            raise ValueError(
                f"Empty document found: {document_id}"
            )

        records.append((document_id, text))

    # Define Spark DataFrame schema
    schema = StructType([
        StructField("document_id", StringType(), False),
        StructField("text", StringType(), False)
    ])

    # Create Spark DataFrame
    documents_df = spark.createDataFrame(
        records,
        schema=schema
    )

    # Maintain numerical document ordering
    documents_df = documents_df.orderBy(
        regexp_extract(
            col("document_id"),
            r"(\d+)",
            1
        ).cast("int")
    )

    return documents_df
