# Healthcare Information Retrieval System

## 1. Project Overview

This project implements an Information Retrieval System using Apache Spark and Python.

The system processes a healthcare dataset containing ten text documents (d1.txt to d10.txt) and implements positional indexing, term statistics, TF-IDF weighting, document vector normalization, phrase searching, Boolean retrieval, cosine similarity and document ranking.

The project uses the healthcare domain and supports the following assigned queries:

- Patient care
- Medical research
- Patient care AND medical research
- Patient care AND NOT medical research

## 2. Technologies Used

- Python 3.14.6
- Apache Spark 4.3.0
- PySpark 4.3.0
- Java 25
- Visual Studio Code

## 3. Project Structure

```text
healthcare-ir-spark/
│
├── data/
│   └── healthcare/
│       ├── d1.txt
│       ├── d2.txt
│       ├── ...
│       └── d10.txt
│
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── spark_session.py
│   ├── ingest.py
│   ├── positional_index.py
│   ├── tf_df_idf.py
│   ├── tfidf_norm.py
│   ├── query_engine.py
│   ├── display.py
│   ├── pipeline.py
│   └── run_all.py
│
├── output/
│   ├── positional_index.json
│   ├── raw_tf.csv
│   ├── weighted_tf.csv
│   ├── df_idf.csv
│   ├── tfidf.csv
│   ├── document_lengths.csv
│   ├── normalized_tfidf.csv
│   ├── phrase_query_results.csv
│   ├── boolean_query_results.csv
│   ├── ranking_results.csv
│   └── query_vectors.csv
│
├── docs/
├── screenshots/
├── report/
│
├── requirements.txt
├── .gitignore
└── README.md
```

## 4. Main Features

### A. Positional Index

Builds a positional index using Spark.

Each term is associated with:

- Document IDs containing the term.
- All occurrences of the term.
- 1-based token positions.

### B. Term Statistics

Calculates:

- Raw Term Frequency (TF).
- Weighted Term Frequency.
- Document Frequency (DF).
- Inverse Document Frequency (IDF).

### C. TF-IDF and Normalization

Calculates:

- TF-IDF weights.
- Document vector lengths.
- Normalized TF-IDF document vectors.

### D. Query Processing

Supports exact phrase queries:

- patient care
- medical research

Supports Boolean queries:

- patient care AND medical research
- patient care AND NOT medical research

### E. Similarity and Ranking

Builds normalized query vectors, calculates cosine similarity between queries and documents, and ranks matching documents according to their similarity scores.

## 5. Calculation Conventions

- All terms are converted to lowercase.
- Tokenization uses whitespace.
- Positions start from 1.
- Total number of documents: N = 10.
- IDF = log10(N / DF).
- Weighted TF = 1 + log10(TF) for nonzero TF.
- TF-IDF = Weighted TF × IDF.
- Document vectors are L2-normalized.
- Query-document similarity is calculated using the dot product of normalized vectors.

## 6. Installation and Setup

### Prerequisites

Install:

- Python 3.14 or compatible version.
- Java Development Kit 25.
- Visual Studio Code (recommended).

### Open the Project

1. Extract the `healthcare-ir-spark.zip` file.
2. Open the extracted `healthcare-ir-spark` folder in VS Code.
3. Open a new terminal inside the project folder.

### Create a Virtual Environment

Windows PowerShell:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks script activation:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
```

Then activate again.

### Install Dependencies

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Run the Project

From the project root:

```powershell
python -m src.run_all
```

The application will execute the complete information retrieval pipeline and generate the results inside the output directory.

## 7. Expected Results

The implementation produces:

| Query                                 | Expected Ranking |
| ------------------------------------- | ---------------- |
| patient care                          | d1 > d5 > d6     |
| medical research                      | d5 > d3 > d8     |
| patient care AND medical research     | d5               |
| patient care AND NOT medical research | d1 > d6          |

The numerical results are calculated using the supplied healthcare dataset and the specified calculation conventions.

## 8. Output Files

The output directory contains:

- Positional Index.
- Raw TF Matrix.
- Weighted TF Matrix.
- DF and IDF.
- TF-IDF Matrix.
- Document Vector Lengths.
- Normalized TF-IDF Matrix.
- Phrase Query Results.
- Boolean Query Results.
- Query Vectors.
- Similarity Scores and Rankings.

## 9. Team Project

Domain: Healthcare

The project implements the Information Retrieval requirements assigned for the healthcare dataset.# Healthcare Information Retrieval System

## 1. Project Overview

This project implements an Information Retrieval System using Apache Spark and Python.

The system processes a healthcare dataset containing ten text documents (d1.txt to d10.txt) and implements positional indexing, term statistics, TF-IDF weighting, document vector normalization, phrase searching, Boolean retrieval, cosine similarity and document ranking.

The project uses the healthcare domain and supports the following assigned queries:

- Patient care
- Medical research
- Patient care AND medical research
- Patient care AND NOT medical research

## 2. Technologies Used

- Python 3.14.6
- Apache Spark 4.3.0
- PySpark 4.3.0
- Java 25
- Visual Studio Code

## 3. Project Structure

```text
healthcare-ir-spark/
│
├── data/
│   └── healthcare/
│       ├── d1.txt
│       ├── d2.txt
│       ├── ...
│       └── d10.txt
│
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── spark_session.py
│   ├── ingest.py
│   ├── positional_index.py
│   ├── tf_df_idf.py
│   ├── tfidf_norm.py
│   ├── query_engine.py
│   ├── display.py
│   ├── pipeline.py
│   └── run_all.py
│
├── output/
│   ├── positional_index.json
│   ├── raw_tf.csv
│   ├── weighted_tf.csv
│   ├── df_idf.csv
│   ├── tfidf.csv
│   ├── document_lengths.csv
│   ├── normalized_tfidf.csv
│   ├── phrase_query_results.csv
│   ├── boolean_query_results.csv
│   ├── ranking_results.csv
│   └── query_vectors.csv
│
├── docs/
├── screenshots/
├── report/
│
├── requirements.txt
├── .gitignore
└── README.md
```

## 4. Main Features

### A. Positional Index

Builds a positional index using Spark.

Each term is associated with:

- Document IDs containing the term.
- All occurrences of the term.
- 1-based token positions.

### B. Term Statistics

Calculates:

- Raw Term Frequency (TF).
- Weighted Term Frequency.
- Document Frequency (DF).
- Inverse Document Frequency (IDF).

### C. TF-IDF and Normalization

Calculates:

- TF-IDF weights.
- Document vector lengths.
- Normalized TF-IDF document vectors.

### D. Query Processing

Supports exact phrase queries:

- patient care
- medical research

Supports Boolean queries:

- patient care AND medical research
- patient care AND NOT medical research

### E. Similarity and Ranking

Builds normalized query vectors, calculates cosine similarity between queries and documents, and ranks matching documents according to their similarity scores.

## 5. Calculation Conventions

- All terms are converted to lowercase.
- Tokenization uses whitespace.
- Positions start from 1.
- Total number of documents: N = 10.
- IDF = log10(N / DF).
- Weighted TF = 1 + log10(TF) for nonzero TF.
- TF-IDF = Weighted TF × IDF.
- Document vectors are L2-normalized.
- Query-document similarity is calculated using the dot product of normalized vectors.

## 6. Installation and Setup

### Prerequisites

Install:

- Python 3.14 or compatible version.
- Java Development Kit 25.
- Visual Studio Code (recommended).

### Open the Project

1. Extract the `healthcare-ir-spark.zip` file.
2. Open the extracted `healthcare-ir-spark` folder in VS Code.
3. Open a new terminal inside the project folder.

### Create a Virtual Environment

Windows PowerShell:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks script activation:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
```

Then activate again.

### Install Dependencies

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Run the Project

From the project root:

```powershell
python -m src.run_all
```

The application will execute the complete information retrieval pipeline and generate the results inside the output directory.

## 7. Expected Results

The implementation produces:

| Query                                 | Expected Ranking |
| ------------------------------------- | ---------------- |
| patient care                          | d1 > d5 > d6     |
| medical research                      | d5 > d3 > d8     |
| patient care AND medical research     | d5               |
| patient care AND NOT medical research | d1 > d6          |

The numerical results are calculated using the supplied healthcare dataset and the specified calculation conventions.

## 8. Output Files

The output directory contains:

- Positional Index.
- Raw TF Matrix.
- Weighted TF Matrix.
- DF and IDF.
- TF-IDF Matrix.
- Document Vector Lengths.
- Normalized TF-IDF Matrix.
- Phrase Query Results.
- Boolean Query Results.
- Query Vectors.
- Similarity Scores and Rankings.

## 9. Team Project

Domain: Healthcare

The project implements the Information Retrieval requirements assigned for the healthcare dataset.
