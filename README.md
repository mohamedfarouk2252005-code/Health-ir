# Healthcare Information Retrieval System

## 1. Project Overview

This project implements an interactive Information Retrieval (IR) system for a healthcare text collection. It uses Python and PySpark to build a positional index, calculate term statistics, generate TF-IDF document vectors, process term/phrase/Boolean queries, and rank matching documents by cosine similarity.

The assigned healthcare collection contains ten documents (`d1.txt` through `d10.txt`). The application can also load another folder containing `.txt` files; it discovers the files and document IDs at runtime rather than requiring the healthcare filenames to be hardcoded.

Assigned healthcare queries:

- `patient care`
- `medical research`
- `patient care AND medical research`
- `patient care AND NOT medical research`

## 2. Technologies

- Python 3.14.6 (development environment)
- PySpark 4.3.0
- Java Development Kit 25 (development environment)
- VS Code (recommended editor)

A separate Spark installation is not required for this local PySpark application. Install the pinned Python dependency from `requirements.txt`.

## 3. Project Structure

```text
Health-ir/
├── data/
│   ├── healthcare/              # Assigned 10-document collection
│   └── environment/             # Small alternate-domain demo collection
├── src/
│   ├── config.py                # Default data/output paths and assigned queries
│   ├── utils.py                 # Natural sorting of document IDs
│   ├── spark_session.py         # Local SparkSession configuration
│   ├── ingest.py                # Discover/read .txt files and create Spark DataFrame
│   ├── positional_index.py      # Token positions and positional postings
│   ├── tf_df_idf.py             # Raw TF, weighted TF, DF and IDF
│   ├── tfidf_norm.py            # TF-IDF, vector lengths, normalization and matrices
│   ├── query_engine.py          # Term/phrase/Boolean search, similarity and ranking
│   ├── display.py               # Console tables and CSV/JSON output
│   ├── pipeline.py              # Builds the data structures and runs queries
│   ├── cli.py                   # Interactive command-line menu
│   └── run_all.py               # Application entry point
├── output/                      # Generated CSV and JSON results
├── docs/                        # Requirements, technical notes and demo instructions
├── report/                      # Printable report
├── screenshots/                 # Evidence screenshots to capture during execution
├── requirements.txt
├── .gitignore
└── README.md
```

## 4. System Architecture and PySpark's Role

The application follows this processing flow:

```text
Selected folder of .txt documents
              |
              v
     Document discovery/read
              |
              v
     Spark DataFrame creation
              |
              v
  PySpark tokenization and positions
              |
              v
       Positional Index
              |
              v
  TF / Weighted TF / DF / IDF
              |
              v
      TF-IDF and vector lengths
              |
              v
      Normalized document vectors
              |
              v
   Term / Phrase / Boolean queries
              |
              v
     Cosine similarity and ranking
              |
              v
       Console + CSV/JSON outputs
```

**Input-file handling detail:** `src/ingest.py` discovers `.txt` files with `pathlib` and reads their UTF-8 text on the local machine. It then creates a Spark DataFrame from the document records. This approach avoids a Hadoop `NativeIO$Windows.access0` failure encountered when using Hadoop's local file listing on the development Windows machine. The indexing and collection calculations are performed on Spark DataFrames/RDD-backed Spark operations; the application does not display precomputed healthcare tables in place of processing the selected files.

PySpark is used for:

- Tokenization with `lower`, `trim`, and `split`.
- Assigning zero-based token offsets with `posexplode`, then converting them to 1-based positions.
- Grouping `(term, document_id)` postings and collecting sorted positions.
- Calculating TF from the number of stored positions.
- Calculating weighted TF, DF and IDF with Spark SQL functions and aggregations.
- Joining term weights to calculate TF-IDF.
- Aggregating squared weights to calculate document vector lengths.
- Normalizing document weights and pivoting long records into term-document matrices.

The query engine collects the relevant postings or normalized vector values for the query and performs phrase-position checks, Boolean set operations, and the final dot product in Python. This keeps query processing straightforward while the document indexing and statistical processing are performed in Spark.

## 5. Calculation Conventions

- Lowercase all terms.
- Split text on whitespace; do not remove stop words or apply stemming.
- Count token positions from 1 in each document.
- Let `N` be the number of non-empty `.txt` documents loaded from the selected folder.
- `TF(t,d)` = number of occurrences of term `t` in document `d`.
- `WeightedTF(t,d) = 1 + log10(TF(t,d))` when TF is nonzero; otherwise 0.
- `DF(t)` = number of distinct documents containing term `t`.
- `IDF(t) = log10(N / DF(t))`.
- `TF-IDF(t,d) = WeightedTF(t,d) × IDF(t)`.
- `Length(d) = sqrt(sum_t(TF-IDF(t,d)^2))`.
- `NormalizedTFIDF(t,d) = TF-IDF(t,d) / Length(d)`; a zero-length vector is handled as all zero weights.
- Normalize the query vector in the same way and calculate similarity as the dot product of the normalized query and document vectors.

## 6. Installation

### Prerequisites

Install Python and a JDK. The development environment used Python 3.14.6 and Temurin JDK 25.

### Windows PowerShell

Open the extracted project folder in VS Code, then open a terminal in the project root.

Create a virtual environment:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, run this in the current terminal session and activate again:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 7. Running the Application

Run the interactive CLI from the project root:

```powershell
python -m src.run_all
```

The application prompts for a dataset folder. Press Enter to use `data/healthcare`.

You can also provide a folder directly:

```powershell
python -m src.run_all --folder "C:\path\to\folder"
```

Run the four assigned healthcare queries without opening the interactive menu:

```powershell
python -m src.run_all --batch
```

The interactive menu supports:

```text
1) Term search
2) Phrase query
3) Boolean query (AND / AND NOT)
4) Show tables (index, TF, DF/IDF, TF-IDF, ...)
5) Run the assigned queries
6) Change dataset folder
0) Exit
```

Query examples:

- Term: `monitoring`
- Phrase: `medical research`
- Boolean AND: `patient care AND medical research`
- Boolean AND NOT: `patient care AND NOT medical research`

Boolean operators must be uppercase (`AND`, `AND NOT`). A lowercase `and` is treated as an ordinary document term.

## 8. Healthcare Reference Results

The implementation was run against the supplied healthcare documents and its numerical results were compared with the project reference outputs.

| Query | Matching documents | Ranking (highest similarity first) |
|---|---|---|
| `patient care` | d1, d5, d6 | d1 > d5 > d6 |
| `medical research` | d3, d5, d8 | d5 > d3 > d8 |
| `patient care AND medical research` | d5 | d5 |
| `patient care AND NOT medical research` | d1, d6 | d1 > d6 |

Reference similarity scores:

| Query | Document scores |
|---|---|
| `patient care` | d1: 1.0000; d5: 0.5473; d6: 0.2253 |
| `medical research` | d5: 0.6325; d3: 0.4715; d8: 0.2970 |
| `patient care AND medical research` | d5: 0.8296 |
| `patient care AND NOT medical research` | d1: 1.0000; d6: 0.2253 |

## 9. Generated Outputs

The `output/` folder contains:

- `positional_index.json`
- `raw_tf.csv`
- `weighted_tf.csv`
- `df_idf.csv`
- `tfidf.csv`
- `document_lengths.csv`
- `normalized_tfidf.csv`
- `phrase_query_results.csv`
- `boolean_query_results.csv`
- `ranking_results.csv`
- `query_vectors.csv`

The index and calculation tables are regenerated when a dataset is loaded. Query-result CSV files are updated based on queries executed in the current application session. To regenerate the complete set of four assigned healthcare query results before submission, run:

```powershell
python -m src.run_all --batch
```

This is important: if the last interactive session only ran a single term query, the query CSV files will contain that session's query results, not all four assigned examples.

## 10. Dataset Flexibility Demonstration

The application discovers all non-empty `.txt` files in the selected folder and derives document IDs from filenames. It calculates `N` from the loaded documents and creates matrix columns from the discovered IDs. It therefore does not require exactly ten documents or `d1.txt` through `d10.txt`.

A small alternate-domain collection is included in `data/environment/`. Follow `docs/bonus_dataset_flexibility_demo.md` to run it and capture evidence for the dataset-flexibility bonus.

## 11. Submission Notes

Before submitting:

1. Run `python -m src.run_all --batch` with the healthcare dataset to refresh all assigned-query outputs.
2. Run the environment demo and capture screenshots showing the different filenames and document count.
3. Capture screenshots of the positional index, calculation tables, phrase results, Boolean results, and rankings.
4. Print the technical report in `report/` and attach or print the full CSV/JSON output tables as appendices where required.
5. Do not include `.venv/` or `__pycache__/` in the submission ZIP.
