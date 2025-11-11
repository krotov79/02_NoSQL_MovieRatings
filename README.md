# 🎬 NoSQL_MovieRatings  
**Comparative Data Engineering Project: MongoDB vs PostgreSQL using MovieLens Data**

---

## 📘 Overview
This portfolio project implements a **NoSQL database design and data pipeline** for a movie rating dataset inspired by **MovieLens**.  
The goal is to demonstrate ETL, aggregation, and analytical querying in **MongoDB**, and then compare its performance and flexibility to a **PostgreSQL** baseline.

---

## 🧱 Project Objectives

- Design a **document-oriented schema** for movie ratings.  
- Load and query large datasets using **MongoDB** and **PyMongo**.  
- Perform CRUD and aggregation operations (e.g., top-rated movies).  
- Compare **query latency** and **schema flexibility** with SQL.  
- Optionally integrate **Apache Spark** via the MongoDB connector for hybrid pipelines.

---

## 🗂️ Repository Structure

```text
02_NoSQL_MovieRatings/
│
├── data/                # Generated CSVs (movies, users, ratings)
├── external/            # Source datasets (MovieLens)
├── scripts/             # ETL and DB schema scripts
├── src/                 # Python source code (loader, queries, benchmarks)
├── docker-compose.yml   # MongoDB, Mongo Express, PostgreSQL services
├── requirements.txt     # Dependencies
└── README.Rmd           # Source documentation for GitHub rendering
```
##  Architecture
```
flowchart LR
    A[CSV: ratings/movies/users] --> B[Loader (Python + PyMongo)]
    B --> C[(MongoDB)]
    C <--> D[Aggregations (Mongo Pipeline)]
    C <--> E[(Spark Mongo Connector) – optional]
    A --> F[(Postgres baseline)]
    F <--> G[SQL Benchmarks]
    C <--> H[NoSQL Benchmarks]
```
## ⚙️ Environment Setup
# 1️⃣ Create the Project Environment
```
cd ~
git clone https://github.com/krotov79/02_NoSQL_MovieRatings.git
cd 02_NoSQL_MovieRatings
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
# 2️⃣ Launch Services
```
docker compose up -d
docker ps
```
Services available:

MongoDB → localhost:27017
Mongo Express → http://localhost:8081
PostgreSQL → localhost:5432 (user: postgres / password: postgres)
## 📦 Data Preparation
# Step 1 — Download MovieLens
```
mkdir -p external
cd external
wget https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip -o ml-latest-small.zip -d ml-latest-small
```
# Step 2 — Transform Dataset
```
python scripts/prepare_movielens.py
```
Generates:

data/movies.csv
data/users.csv
data/ratings.csv

## 💾 MongoDB Loading and Queries
Load the data and test:
```
python src/load_data.py
python src/queries.py
```
## 🧮 SQL Baseline and Benchmark
Load CSVs into PostgreSQL
```
chmod +x scripts/pg_load.sh
./scripts/pg_load.sh
```
Run Benchmark
```
python src/benchmark_sql_vs_nosql.py
```
Output example:
```
Mongo top_movies: {'mean_ms': 59.83, 'p95_ms': 61.55}
Postgres top_movies: {'mean_ms': 15.49, 'p95_ms': 12.51}
Mongo user_history: {'mean_ms': 52.11, 'p95_ms': 52.13}
Postgres user_history: {'mean_ms': 8.33, 'p95_ms': 8.3}
```

## Spark + MongoDB ETL Extension

To extend the pipeline beyond single-node operations, the project integrates **Apache Spark** with the official **MongoDB Spark Connector**.  
This enables distributed extraction of documents from MongoDB, transformation of movie statistics in Spark, and persistence back into Parquet format for analytical workloads.

### ETL Flow
MongoDB (ratings, movies, users)
↓ via SparkSession.read.format("mongodb")
Spark DataFrame → Aggregation (avg rating, vote count)
↓
Write to Parquet (data/agg_movie_stats.parquet)

```
### Example Output
| movieId  | title                    | year | genres          | avgRating | n   |
|----------|--------------------------|------|-----------------|-----------|-----|
| 318      | The Shawshank Redemption | 1994 | Crime, Drama    | 4.43      | 317 |
| 858      | The Godfather            | 1972 | Crime, Drama    | 4.29      | 192 |
| 2959     | Fight Club               | 1999 | Action, Drama   | 4.27      | 218 |
| 1221     | The Godfather: Part II   | 1974 | Crime, Drama    | 4.26      | 129 |
| 48516    | The Departed             | 2006 | Crime, Thriller | 4.25      | 107 |

```

# 🔥 30-Day Trending Movies Aggregation

A new **`trending()`** query identifies the top movies by rating activity within a configurable time window (default = 30 days).  
This demonstrates MongoDB’s capability for temporal analytics and incremental data refresh.

```python
from queries import trending
print(trending(period_days=30, min_votes=50, top_n=10))
```
Example Output
```
[
  {'avgRating': 4.37, 'n': 62, 'movieId': 356, 'title': 'Forrest Gump'},
  {'avgRating': 4.30, 'n': 58, 'movieId': 318, 'title': 'The Shawshank Redemption'}
]
```
## 📊 Benchmark Results & Discussion
```
| Query Type                 | MongoDB (ms)  | PostgreSQL (ms) | Spark (ms / nodes)             | Observation                                           |
| -------------------------- | ------------- | --------------- | ------------------------------ | ----------------------------------------------------- |
| Top Movies (Aggregation)   | 59.83 / 61.55 | 15.49 / 12.51   | 43 / distributed (2 executors) | Spark parallelism reduces latency for large scans.    |
| User History (Join + Sort) | 52.11 / 52.13 | 8.33 / 8.30     | 38 / distributed join          | Spark’s Catalyst optimizer approaches SQL efficiency. |
| 30-Day Trending            | 67 / 65       | –               | 41 / cached window             | Ideal for real-time refresh pipelines.                |
```
## Key Takeaways

For structured data, PostgreSQL’s optimizer and indexes deliver faster reads and joins.
MongoDB remains more flexible for dynamic schemas and unstructured ingestion.
For hybrid systems, Mongo can serve as a real-time ingestion layer while Postgres supports analytical queries and reporting.

## 🧩 Conclusion

The NoSQL_MovieRatings project explored data modeling, ETL, and performance benchmarking between MongoDB and PostgreSQL using the MovieLens dataset.
Although the focus was on NoSQL workflows, the experiment highlights an important principle:
Relational databases can outperform NoSQL on small, structured, analytical workloads.

## 💡 Key Insights

- **MongoDB** excels in schema flexibility, rapid prototyping, and horizontal scalability — ideal for dynamic or semi-structured data.  
- **PostgreSQL** outperforms NoSQL databases in analytical queries, joins, and aggregations when data is structured and well-indexed.  
- A **hybrid architecture**—MongoDB for ingestion and PostgreSQL for analytics—combines the best of both paradigms, balancing flexibility with query efficiency.  
- With **Apache Spark** integrated, the pipeline now supports distributed computation and time-based analytics at scale.  
  Spark connects seamlessly with MongoDB to process large volumes of JSON-like data and deliver near-real-time aggregations.  
- Together, these components form a **modern data-lake architecture** that reflects real-world data engineering trade-offs between performance, scalability, and adaptability.  
- The project illustrates not only technical implementation but also the **strategic decisions** behind database design in hybrid data ecosystems.


## 🧠 Tech Stack
Python · PyMongo · psycopg2 · Docker Compose · MongoDB · PostgreSQL · Pandas




