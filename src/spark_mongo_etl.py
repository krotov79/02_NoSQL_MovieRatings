import argparse
from pyspark.sql import SparkSession, functions as F

def build_spark(mongo_uri: str):
    return (
        SparkSession.builder
        .appName("NoSQL-MovieRatings-SparkMongo")
        .config("spark.mongodb.read.connection.uri", mongo_uri)
        .config("spark.mongodb.write.connection.uri", mongo_uri)
        .getOrCreate()
    )

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mongoUri", default="mongodb://127.0.0.1/moviedb")
    p.add_argument("--outParquet", default="data/agg_movie_stats.parquet")
    p.add_argument("--writeBack", action="store_true",
                   help="write aggregated stats back to Mongo as collection agg_movie_stats")
    args = p.parse_args()

    spark = build_spark(args.mongoUri)

    ratings = (spark.read.format("mongodb")
               .option("database","moviedb").option("collection","ratings").load())
    movies  = (spark.read.format("mongodb")
               .option("database","moviedb").option("collection","movies").load())

    # Basic distributed aggregation: per-movie average & votes
    agg = (ratings.groupBy("movieId")
           .agg(F.avg("rating").alias("avgRating"), F.count("*").alias("n"))
           .filter(F.col("n") >= 100))

    out = (agg.join(movies, on="movieId", how="left")
             .select("movieId","title","year","genres","avgRating","n")
             .orderBy(F.desc("avgRating"), F.desc("n")))

    # Save to Parquet (portfolio deliverable)
    out.write.mode("overwrite").parquet(args.outParquet)

    if args.writeBack:
        (out.write.format("mongodb")
         .mode("overwrite")
         .option("database","moviedb")
         .option("collection","agg_movie_stats")
         .save())

    print("Spark ETL complete.")

if __name__ == "__main__":
    main()
