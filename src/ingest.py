from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from src.utils import natural_key


def load_documents(spark: SparkSession, folder) -> DataFrame:
    """
    Import every .txt file of the selected folder and return a
    Spark DataFrame.

    Columns:
        document_id: file name without extension (d1, article_1, ...)
        text: complete document content

    Files are read with Python (this avoids Hadoop's native Windows
    libraries, winutils.exe / hadoop.dll); everything after this point
    - tokenization, positional index, TF, IDF, TF-IDF, normalization -
    is processed by Spark.
    """

    folder = Path(folder).expanduser().resolve()

    if not folder.is_dir():
        raise FileNotFoundError(f"Folder not found: {folder}")

    files = sorted(
        folder.glob("*.txt"),
        key=lambda path: natural_key(path.stem)
    )

    if not files:
        raise ValueError(f"No .txt files found in: {folder}")

    records = []

    for file in files:

        text = file.read_text(encoding="utf-8-sig").strip()

        # Ignore empty files
        if text:
            records.append((file.stem, text))

    if not records:
        raise ValueError(f"All .txt files in {folder} are empty.")

    schema = StructType([
        StructField("document_id", StringType(), False),
        StructField("text", StringType(), False)
    ])

    return spark.createDataFrame(records, schema=schema)
