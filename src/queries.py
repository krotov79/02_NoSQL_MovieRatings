from pymongo import MongoClient
db = MongoClient("mongodb://localhost:27017")["moviedb"]

def top_movies(min_votes=1000, top_n=10):
    pipe = [
        {"$group":{"_id":"$movieId","avgRating":{"$avg":"$rating"},"n":{"$sum":1}}},
        {"$match":{"n":{"$gte":min_votes}}},
        {"$sort":{"avgRating":-1,"n":-1}},
        {"$limit":top_n},
        {"$lookup":{"from":"movies","localField":"_id","foreignField":"movieId","as":"movie"}},
        {"$unwind":"$movie"},
        {"$project":{"_id":0,"movieId":"$_id","title":"$movie.title","avgRating":1,"n":1}}
    ]
    return list(db.ratings.aggregate(pipe))

def trending(period_days=30, min_votes=50, top_n=20):
    import datetime
    since = datetime.datetime.utcnow() - datetime.timedelta(days=period_days)
    pipeline = [
        {"$match":{"ts":{"$gte":since}}},
        {"$group":{"_id":"$movieId","avgRating":{"$avg":"$rating"},"n":{"$sum":1}}},
        {"$match":{"n":{"$gte":min_votes}}},
        {"$sort":{"n":-1,"avgRating":-1}},
        {"$limit":top_n},
        {"$lookup":{"from":"movies","localField":"_id","foreignField":"movieId","as":"movie"}},
        {"$unwind":"$movie"},
        {"$project":{"_id":0,"movieId":"$_id","title":"$movie.title","avgRating":1,"n":1}}
    ]
    return list(db.ratings.aggregate(pipeline))


if __name__ == "__main__":
    print(top_movies())
