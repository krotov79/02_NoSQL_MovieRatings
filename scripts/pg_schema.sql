DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS users;

CREATE TABLE movies (
  movieid   INT PRIMARY KEY,
  title     TEXT,
  year      INT,
  genres    TEXT
);

CREATE TABLE users (
  userid    INT PRIMARY KEY,
  name      TEXT,
  joindate  DATE,
  country   TEXT,
  age       INT,
  genres    TEXT
);

CREATE TABLE ratings (
  userid    INT,
  movieid   INT,
  rating    DOUBLE PRECISION,
  ts        BIGINT,
  FOREIGN KEY (userid) REFERENCES users(userid),
  FOREIGN KEY (movieid) REFERENCES movies(movieid)
);

CREATE INDEX idx_ratings_movie_ts  ON ratings (movieid, ts DESC);
CREATE INDEX idx_ratings_user_ts   ON ratings (userid, ts DESC);
CREATE INDEX idx_ratings_movieuser ON ratings (movieid, userid);
