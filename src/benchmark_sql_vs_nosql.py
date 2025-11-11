import time, statistics, psycopg2
from pymongo import MongoClient

TRIALS = 5

def timeit(fn):
    t=[]
    for _ in range(TRIALS):
        t0=time.perf_counter(); fn(); t.append(time.perf_counter()-t0)
    return {
        "mean_ms": round(1000*statistics.mean(t),2),
        "p95_ms":  round(1000*sorted(t)[int(0.95*(TRIALS-1))],2)
    }

# ----- Mongo -----
mcli = MongoClient("mongodb://localhost:27017")
mdb = mcli["moviedb"]

def mongo_top_movies():
    list(mdb.ratings.aggregate([
        {"$group":{"_id":"$movieId","avgRating":{"$avg":"$rating"},"n":{"$sum":1}}},
        {"$match":{"n":{"$gte":100}}},
        {"$sort":{"avgRating":-1,"n":-1}},
        {"$limit":20}
    ], allowDiskUse=True))

def mongo_user_history():
    top_user = list(mdb.ratings.aggregate([
        {"$group":{"_id":"$userId","n":{"$sum":1}}},
        {"$sort":{"n":-1}}, {"$limit":1}
    ]))[0]["_id"]
    list(mdb.ratings.aggregate([
        {"$match":{"userId":top_user}},
        {"$sort":{"ts":-1}},
        {"$limit":200},
        {"$lookup":{"from":"movies","localField":"movieId","foreignField":"movieId","as":"m"}},
        {"$unwind":"$m"},
        {"$project":{"_id":0,"movieId":1,"rating":1,"ts":1,"title":"$m.title"}}
    ]))

# ----- Postgres -----
pg = psycopg2.connect("dbname=moviedb user=postgres password=postgres host=localhost port=5432")
pg.autocommit=True

def pg_top_movies():
    with pg.cursor() as cur:
        cur.execute("""
            WITH agg AS (
              SELECT movieid, avg(rating) AS avgr, count(*) AS n
              FROM ratings GROUP BY movieid
            )
            SELECT movieid, avgr, n FROM agg WHERE n >= 100
            ORDER BY avgr DESC, n DESC LIMIT 20;
        """); cur.fetchall()

def pg_user_history():
    with pg.cursor() as cur:
        cur.execute("""
            SELECT userid FROM (
              SELECT userid, count(*) AS n FROM ratings GROUP BY userid
              ORDER BY n DESC LIMIT 1
            ) t
        """); uid = cur.fetchone()[0]
        cur.execute("""
            SELECT r.movieid, r.rating, r.ts, m.title
            FROM ratings r
            JOIN movies m ON m.movieid = r.movieid
            WHERE r.userid = %s
            ORDER BY r.ts DESC
            LIMIT 200
        """, (uid,)); cur.fetchall()

if __name__ == "__main__":
    print("Mongo top_movies:", timeit(mongo_top_movies))
    print("Postgres top_movies:", timeit(pg_top_movies))
    print("Mongo user_history:", timeit(mongo_user_history))
    print("Postgres user_history:", timeit(pg_user_history))
