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
## 📊 Benchmark Results & Discussion
```
Query Type	                MongoDB (ms)	            PostgreSQL (ms)	            Observation
Top Movies (Aggregation)	mean ≈ 59.83 / p95 ≈ 61.55	mean ≈ 15.49 / p95 ≈ 12.51	SQL’s query planner optimizes aggregates on small, indexed datasets efficiently.
User History (Join + Sort)	mean ≈ 52.11 / p95 ≈ 52.13	mean ≈ 8.33 / p95 ≈ 8.3	    PostgreSQL joins outperform MongoDB $lookup on moderate datasets.
```

## Key Takeaways

For structured data, PostgreSQL’s optimizer and indexes deliver faster reads and joins.
MongoDB remains more flexible for dynamic schemas and unstructured ingestion.
For hybrid systems, Mongo can serve as a real-time ingestion layer while Postgres supports analytical queries and reporting.

## 🧩 Conclusion

The NoSQL_MovieRatings project explored data modeling, ETL, and performance benchmarking between MongoDB and PostgreSQL using the MovieLens dataset.
Although the focus was on NoSQL workflows, the experiment highlights an important principle:
Relational databases can outperform NoSQL on small, structured, analytical workloads.

## Key Insights
MongoDB excels in schema flexibility, rapid development, and horizontal scaling.
PostgreSQL dominates analytical queries and joins when data is structured and well-indexed.
A hybrid architecture — MongoDB for ingestion, PostgreSQL for analytics — combines the strengths of both worlds.
This project demonstrates practical trade-offs in data engineering and database design.

## 🧠 Tech Stack
Python · PyMongo · psycopg2 · Docker Compose · MongoDB · PostgreSQL · Pandas




