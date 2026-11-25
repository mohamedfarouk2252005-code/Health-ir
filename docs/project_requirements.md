## Document structure

Part 1 contains the project requirements for each of the four versions. Part 2 contains reference outputs for the corresponding datasets, including positional indexes, term statistics, weighted TF, TF-IDF, normalized vectors, phrase-query matches, Boolean results, similarity scores, and rankings.

The four versions are Healthcare, Technology, Education, and Business. Each version contains ten documents. The exact text below is the basis for the reference calculations; if a team changes the spelling, tokenization, or document text, its expected values will change.

### Shared calculation conventions

* All terms are lowercased and tokenized by whitespace; punctuation is not present in the supplied documents.

* Positions are 1-based and count every token from left to right.

* IDF uses log base 10: $\text{IDF}(t) = \log_{10}(N / \text{DF}(t))$, with $N = 10$.

* Weighted TF $= 1 + \log_{10}(\text{TF})$ for nonzero TF.

* TF-IDF $= \text{Weighted TF} \times \text{IDF}$.

* Document vectors and query vectors are $L_2$-normalized before the dot-product similarity calculation.

# PART 1 — PROJECT REQUIREMENTS

## 1. HEALTHCARE VERSION:

Assigned domain: Healthcare. The team must use its own assigned dataset containing ten text documents (d1.txt to d10.txt).

### Part A — Spark App: Positional Index

Build a Positional Index from the assigned dataset using the Spark application. Display each term with the document IDs in which it appears and all token positions in each document. Positions are counted from 1 for the first token in each document.

### Part B — Retrieval and Ranking

 1. Use the output of the Positional Index from Part A.

 2. Compute and display Term Frequency (TF) for every term in every document.

 3. Compute and display Document Frequency (DF) and Inverse Document Frequency (IDF) for every term.

 4. Compute and display the TF-IDF matrix.

 5. Compute document vector lengths and display the Normalized TF-IDF matrix.

 6. Allow phrase queries using the Positional Index. Assigned phrase queries: “patient care” and “medical research”.

 7. For each phrase query, identify matching documents, compute query-document similarity, display similarity scores, and rank the matching documents.

 8. Support the Boolean query: “patient care AND medical research”.

 9. Support the Boolean query: “patient care AND NOT medical research”.

10. For Boolean queries, display the matching documents and rank the returned documents using the similarity score.

### Part C — Documentation and Submission

* Submit source code for the Spark Positional Index and the retrieval/ranking part.

* Include the Positional Index, TF, DF/IDF, TF-IDF, document vector lengths, Normalized TF-IDF, query results, similarity scores, and rankings.

* Include screenshots or program outputs that demonstrate the required functions.

* Explain how positional information supports phrase queries.

* Explain the relationship between DF and IDF, and why repeated terms affect TF.

* Explain the purpose of TF-IDF and vector normalization.

* Explain how similarity scores are used to rank documents.

* Explain the difference between AND and AND NOT using examples from the assigned dataset.

All teams implement the same project functions. Only the assigned domain, dataset, and phrase queries differ.

# PART 2 — EXPECTED PROJECT OUTPUTS

## 1. HEALTHCARE VERSION

Reference calculations for checking the implementation. Document IDs and token positions follow the supplied dataset. Logarithms use base 10.

### Assigned Dataset

| File | Exact document text | Word count | 
| ----- | ----- | ----- | 
| **d1** | patient care | 2 | 
| **d2** | patient safety needs careful monitoring | 5 | 
| **d3** | medical research improves treatment | 4 | 
| **d4** | medical treatment supports patient recovery | 5 | 
| **d5** | patient care patient safety medical research patient care | 8 | 
| **d6** | hospital staff provide patient care | 5 | 
| **d7** | clinical teams support patient recovery through treatment | 7 | 
| **d8** | medical research uses patient records and clinical data | 8 | 
| **d9** | patient monitoring patient monitoring supports care | 6 | 
| **d10** | health information systems improve patient safety medical records clinical services | 10 | 

### Positional Index (expected)

| Term | Document IDs and 1-based positions | 
| ----- | ----- | 
| **and** | d8: 6 | 
| **care** | d1: 2; d5: 2, 8; d6: 5; d9: 6 | 
| **careful** | d2: 4 | 
| **clinical** | d7: 1; d8: 7; d10: 9 | 
| **data** | d8: 8 | 
| **health** | d10: 1 | 
| **hospital** | d6: 1 | 
| **improve** | d10: 4 | 
| **improves** | d3: 3 | 
| **information** | d10: 2 | 
| **medical** | d3: 1; d4: 1; d5: 5; d8: 1; d10: 7 | 
| **monitoring** | d2: 5; d9: 2, 4 | 
| **needs** | d2: 3 | 
| **patient** | d1: 1; d2: 1; d4: 4; d5: 1, 3, 7; d6: 4; d7: 4; d8: 4; d9: 1, 3; d10: 5 | 
| **provide** | d6: 3 | 
| **records** | d8: 5; d10: 8 | 
| **recovery** | d4: 5; d7: 5 | 
| **research** | d3: 2; d5: 6; d8: 2 | 
| **safety** | d2: 2; d5: 4; d10: 6 | 
| **services** | d10: 10 | 
| **staff** | d6: 2 | 
| **support** | d7: 3 | 
| **supports** | d4: 3; d9: 5 | 
| **systems** | d10: 3 | 
| **teams** | d7: 2 | 
| **through** | d7: 6 | 
| **treatment** | d3: 4; d4: 2; d7: 7 | 
| **uses** | d8: 3 | 

### Term Frequency (TF) Matrix

#### Raw TF

| Term | d1 | d2 | d3 | d4 | d5 | d6 | d7 | d8 | d9 | d10 | 
| ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | 
| **and** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 
| **care** | 1.0000 | 0 | 0 | 0 | 2.0000 | 1.0000 | 0 | 0 | 1.0000 | 0 | 
| **careful** | 0 | 1.0000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 
| **clinical** | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 1.0000 | 0 | 1.0000 | 
| **data** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 
| **health** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **hospital** | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 
| **improve** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **improves** | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 
| **information** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **medical** | 0 | 0 | 1.0000 | 1.0000 | 1.0000 | 0 | 0 | 1.0000 | 0 | 1.0000 | 
| **monitoring** | 0 | 1.0000 | 0 | 0 | 0 | 0 | 0 | 0 | 2.0000 | 0 | 
| **needs** | 0 | 1.0000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 
| **patient** | 1.0000 | 1.0000 | 0 | 1.0000 | 3.0000 | 1.0000 | 1.0000 | 1.0000 | 2.0000 | 1.0000 | 
| **provide** | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 
| **records** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 1.0000 | 
| **recovery** | 0 | 0 | 0 | 1.0000 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **research** | 0 | 0 | 1.0000 | 0 | 1.0000 | 0 | 0 | 1.0000 | 0 | 0 | 
| **safety** | 0 | 1.0000 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 1.0000 | 
| **services** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **staff** | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 
| **support** | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **supports** | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 
| **systems** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **teams** | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **through** | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **treatment** | 0 | 0 | 1.0000 | 1.0000 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **uses** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 

#### Weighted TF: $1 + \log_{10}(\text{TF})$

| Term | d1 | d2 | d3 | d4 | d5 | d6 | d7 | d8 | d9 | d10 | 
| ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | 
| **and** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 
| **care** | 1.0000 | 0 | 0 | 0 | 1.3010 | 1.0000 | 0 | 0 | 1.0000 | 0 | 
| **careful** | 0 | 1.0000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 
| **clinical** | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 1.0000 | 0 | 1.0000 | 
| **data** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 
| **health** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **hospital** | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 
| **improve** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **improves** | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 
| **information** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **medical** | 0 | 0 | 1.0000 | 1.0000 | 1.0000 | 0 | 0 | 1.0000 | 0 | 1.0000 | 
| **monitoring** | 0 | 1.0000 | 0 | 0 | 0 | 0 | 0 | 0 | 1.3010 | 0 | 
| **needs** | 0 | 1.0000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 
| **patient** | 1.0000 | 1.0000 | 0 | 1.0000 | 1.4771 | 1.0000 | 1.0000 | 1.0000 | 1.3010 | 1.0000 | 
| **provide** | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 
| **records** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 1.0000 | 
| **recovery** | 0 | 0 | 0 | 1.0000 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **research** | 0 | 0 | 1.0000 | 0 | 1.0000 | 0 | 0 | 1.0000 | 0 | 0 | 
| **safety** | 0 | 1.0000 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 1.0000 | 
| **services** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **staff** | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 
| **support** | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **supports** | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 
| **systems** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **teams** | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **through** | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **treatment** | 0 | 0 | 1.0000 | 1.0000 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **uses** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 

### DF and IDF

| Term | DF | $\text{IDF} = \log_{10}(10 / \text{DF})$ | 
| ----- | ----- | ----- | 
| **and** | 1 | 1.0000 | 
| **care** | 4 | 0.3979 | 
| **careful** | 1 | 1.0000 | 
| **clinical** | 3 | 0.5229 | 
| **data** | 1 | 1.0000 | 
| **health** | 1 | 1.0000 | 
| **hospital** | 1 | 1.0000 | 
| **improve** | 1 | 1.0000 | 
| **improves** | 1 | 1.0000 | 
| **information** | 1 | 1.0000 | 
| **medical** | 5 | 0.3010 | 
| **monitoring** | 2 | 0.6990 | 
| **needs** | 1 | 1.0000 | 
| **patient** | 9 | 0.0458 | 
| **provide** | 1 | 1.0000 | 
| **records** | 2 | 0.6990 | 
| **recovery** | 2 | 0.6990 | 
| **research** | 3 | 0.5229 | 
| **safety** | 3 | 0.5229 | 
| **services** | 1 | 1.0000 | 
| **staff** | 1 | 1.0000 | 
| **support** | 1 | 1.0000 | 
| **supports** | 2 | 0.6990 | 
| **systems** | 1 | 1.0000 | 
| **teams** | 1 | 1.0000 | 
| **through** | 1 | 1.0000 | 
| **treatment** | 3 | 0.5229 | 
| **uses** | 1 | 1.0000 | 

### TF-IDF Matrix

| Term | d1 | d2 | d3 | d4 | d5 | d6 | d7 | d8 | d9 | d10 | 
| ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | 
| **and** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 
| **care** | 0.3979 | 0 | 0 | 0 | 0.5177 | 0.3979 | 0 | 0 | 0.3979 | 0 | 
| **careful** | 0 | 1.0000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 
| **clinical** | 0 | 0 | 0 | 0 | 0 | 0 | 0.5229 | 0.5229 | 0 | 0.5229 | 
| **data** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 
| **health** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **hospital** | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 
| **improve** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **improves** | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 
| **information** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **medical** | 0 | 0 | 0.3010 | 0.3010 | 0.3010 | 0 | 0 | 0.3010 | 0 | 0.3010 | 
| **monitoring** | 0 | 0.6990 | 0 | 0 | 0 | 0 | 0 | 0 | 0.9094 | 0 | 
| **needs** | 0 | 1.0000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 
| **patient** | 0.0458 | 0.0458 | 0 | 0.0458 | 0.0676 | 0.0458 | 0.0458 | 0.0458 | 0.0595 | 0.0458 | 
| **provide** | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 
| **records** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.6990 | 0 | 0.6990 | 
| **recovery** | 0 | 0 | 0 | 0.6990 | 0 | 0 | 0.6990 | 0 | 0 | 0 | 
| **research** | 0 | 0 | 0.5229 | 0 | 0.5229 | 0 | 0 | 0.5229 | 0 | 0 | 
| **safety** | 0 | 0.5229 | 0 | 0 | 0.5229 | 0 | 0 | 0 | 0 | 0.5229 | 
| **services** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **staff** | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 0 | 
| **support** | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **supports** | 0 | 0 | 0 | 0.6990 | 0 | 0 | 0 | 0 | 0.6990 | 0 | 
| **systems** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 
| **teams** | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **through** | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 0 | 
| **treatment** | 0 | 0 | 0.5229 | 0.5229 | 0 | 0 | 0.5229 | 0 | 0 | 0 | 
| **uses** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1.0000 | 0 | 0 | 

### Document Vector Lengths

| Document | Vector length | 
| ----- | ----- | 
| **d1** | 0.4006 | 
| **d2** | 1.6625 | 
| **d3** | 1.2796 | 
| **d4** | 1.1590 | 
| **d5** | 0.9540 | 
| **d6** | 1.7778 | 
| **d7** | 2.0093 | 
| **d8** | 2.0318 | 
| **d9** | 1.2155 | 
| **d10** | 2.4755 | 

### Normalized TF-IDF Matrix

| Term | d1 | d2 | d3 | d4 | d5 | d6 | d7 | d8 | d9 | d10 | 
| ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | 
| **and** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.4922 | 0 | 0 | 
| **care** | 0.9935 | 0 | 0 | 0 | 0.5427 | 0.2238 | 0 | 0 | 0.3274 | 0 | 
| **careful** | 0 | 0.6015 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 
| **clinical** | 0 | 0 | 0 | 0 | 0 | 0 | 0.2602 | 0.2574 | 0 | 0.2112 | 
| **data** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.4922 | 0 | 0 | 
| **health** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.4040 | 
| **hospital** | 0 | 0 | 0 | 0 | 0 | 0.5625 | 0 | 0 | 0 | 0 | 
| **improve** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.4040 | 
| **improves** | 0 | 0 | 0.7815 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 
| **information** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.4040 | 
| **medical** | 0 | 0 | 0.2352 | 0.2597 | 0.3156 | 0 | 0 | 0.1482 | 0 | 0.1216 | 
| **monitoring** | 0 | 0.4204 | 0 | 0 | 0 | 0 | 0 | 0 | 0.7482 | 0 | 
| **needs** | 0 | 0.6015 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 
| **patient** | 0.1142 | 0.0275 | 0 | 0.0395 | 0.0709 | 0.0257 | 0.0228 | 0.0225 | 0.0490 | 0.0185 | 
| **provide** | 0 | 0 | 0 | 0 | 0 | 0.5625 | 0 | 0 | 0 | 0 | 
| **records** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.3440 | 0 | 0.2824 | 
| **recovery** | 0 | 0 | 0 | 0.6031 | 0 | 0 | 0.3479 | 0 | 0 | 0 | 
| **research** | 0 | 0 | 0.4086 | 0 | 0.5481 | 0 | 0 | 0.2574 | 0 | 0 | 
| **safety** | 0 | 0.3145 | 0 | 0 | 0.5481 | 0 | 0 | 0 | 0 | 0.2112 | 
| **services** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.4040 | 
| **staff** | 0 | 0 | 0 | 0 | 0 | 0.5625 | 0 | 0 | 0 | 0 | 
| **support** | 0 | 0 | 0 | 0 | 0 | 0 | 0.4977 | 0 | 0 | 0 | 
| **supports** | 0 | 0 | 0 | 0.6031 | 0 | 0 | 0 | 0 | 0.5750 | 0 | 
| **systems** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.4040 | 
| **teams** | 0 | 0 | 0 | 0 | 0 | 0 | 0.4977 | 0 | 0 | 0 | 
| **through** | 0 | 0 | 0 | 0 | 0 | 0 | 0.4977 | 0 | 0 | 0 | 
| **treatment** | 0 | 0 | 0.4086 | 0.4512 | 0 | 0 | 0.2602 | 0 | 0 | 0 | 
| **uses** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.4922 | 0 | 0 | 

### Phrase and Boolean Query Results

#### Phrase query: “patient care”

| Matched document | Starting positions | Phrase token ranges | 
| ----- | ----- | ----- | 
| **d1** | 1 | 1-2 | 
| **d5** | 1, 7 | 1-2, 7-8 | 
| **d6** | 4 | 4-5 | 

#### Phrase query: “medical research”

| Matched document | Starting positions | Phrase token ranges | 
| ----- | ----- | ----- | 
| **d3** | 1 | 1-2 | 
| **d5** | 5 | 5-6 | 
| **d8** | 1 | 1-2 | 

#### Boolean AND: “patient care AND medical research”

Expected intersection: d5

#### Boolean AND NOT: “patient care AND NOT medical research”

Expected result (documents containing the first phrase but not the second): d1, d6

#### Similarity for phrase query: “patient care”

Query vector length $= 0.400562$

| Query term | Weighted TF | Query TF-IDF | Normalized query weight | 
| ----- | ----- | ----- | ----- | 
| **patient** | 1.0000 | 0.0458 | 0.1142 | 
| **care** | 1.0000 | 0.3979 | 0.9935 | 

| Matched document | Similarity | 
| ----- | ----- | 
| **d1** | 1.0000 | 
| **d5** | 0.5473 | 
| **d6** | 0.2253 | 

Ranking (highest similarity first): d1 > d5 > d6

#### Similarity for phrase query: “medical research”

Query vector length $= 0.603342$

| Query term | Weighted TF | Query TF-IDF | Normalized query weight | 
| ----- | ----- | ----- | ----- | 
| **medical** | 1.0000 | 0.3010 | 0.4989 | 
| **research** | 1.0000 | 0.5229 | 0.8666 | 

| Matched document | Similarity | 
| ----- | ----- | 
| **d3** | 0.4715 | 
| **d5** | 0.6325 | 
| **d8** | 0.2970 | 

Ranking (highest similarity first): d5 > d3 > d8

#### Similarity for Boolean AND query: “patient care AND medical research”

Query vector length $= 0.724204$

| Query term | Weighted TF | Query TF-IDF | Normalized query weight | 
| ----- | ----- | ----- | ----- | 
| **patient** | 1.0000 | 0.0458 | 0.0632 | 
| **care** | 1.0000 | 0.3979 | 0.5495 | 
| **medical** | 1.0000 | 0.3010 | 0.4157 | 
| **research** | 1.0000 | 0.5229 | 0.7220 | 

| Matched document | Similarity | 
| ----- | ----- | 
| **d5** | 0.8296 | 

Ranking (highest similarity first): d5

#### Similarity for Boolean AND NOT query: “patient care AND NOT medical research”

Query vector length $= 0.400562$

| Query term | Weighted TF | Query TF-IDF | Normalized query weight | 
| ----- | ----- | ----- | ----- | 
| **patient** | 1.0000 | 0.0458 | 0.1142 | 
| **care** | 1.0000 | 0.3979 | 0.9935 | 

| Matched document | Similarity | 
| ----- | ----- | 
| **d1** | 1.0000 | 
| **d6** | 0.2253 | 

Ranking (highest similarity first): d1 > d6

### Calculation conventions

* $N = 10$ documents.

* Raw TF is the number of occurrences of a term in a document.

* DF is the number of documents containing the term.

* $\text{IDF} = \log_{10}(N / \text{DF})$.

* $\text{Weighted TF} = 1 + \log_{10}(\text{Raw TF})$ when $\text{Raw TF} > 0$; otherwise $0$.

* $\text{TF-IDF} = \text{Weighted TF} \times \text{IDF}$.

* $\text{Document vector length} = \text{square root of the sum of squared TF-IDF weights in that document}$.

* $\text{Normalized TF-IDF} = \text{TF-IDF} / \text{document vector length}$.

* Query terms are weighted with the same $1 + \log_{10}(\text{TF})$ rule and the collection IDF; query vector is normalized before computing the dot product with each normalized document vector.

* Phrase matching is exact and contiguous. Boolean AND uses the intersection of the two phrase-match sets. AND NOT keeps documents matching the first phrase and excludes documents matching the second phrase.