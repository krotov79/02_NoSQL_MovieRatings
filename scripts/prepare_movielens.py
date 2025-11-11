#!/usr/bin/env python3
"""
Prepare MovieLens -> project CSVs

Input (autodetected):
  external/ml-latest-small/{movies.csv,ratings.csv}
  or
  external/ml-25m/{movies.csv,ratings.csv}

Output:
  data/movies.csv   (movieId,title,year,genres)
  data/ratings.csv  (userId,movieId,rating,timestamp)
  data/users.csv    (userId,name,joinDate,country,age,genres)
"""

import csv
import re
import os
import time
import random
import datetime as dt
from pathlib import Path
from collections import defaultdict, Counter

# ---------- Paths ----------
REPO = Path(__file__).resolve().parents[1]
EXTERNAL = REPO / "external"
DATA = REPO / "data"
DATA.mkdir(parents=True, exist_ok=True)

CANDIDATES = [
    EXTERNAL / "ml-latest-small",
    EXTERNAL / "ml-25m",
]

# fix common nested unzip: external/ml-latest-small/ml-latest-small/*
def _flatten_if_nested(root: Path):
    nested = root / root.name
    if nested.exists() and nested.is_dir():
        for p in nested.iterdir():
            target = root / p.name
            if not target.exists():
                p.replace(target)
        try:
            nested.rmdir()
        except OSError:
            pass

SRC_DIR = None
for cand in CANDIDATES:
    if cand.exists():
        _flatten_if_nested(cand)
        if (cand / "movies.csv").exists() and (cand / "ratings.csv").exists():
            SRC_DIR = cand
            break

if SRC_DIR is None:
    raise SystemExit(
        "MovieLens CSVs not found. Put them in external/ml-latest-small or external/ml-25m "
        "and ensure movies.csv and ratings.csv exist."
    )

MOVIES_IN = SRC_DIR / "movies.csv"
RATINGS_IN = SRC_DIR / "ratings.csv"

MOVIES_OUT = DATA / "movies.csv"
RATINGS_OUT = DATA / "ratings.csv"
USERS_OUT = DATA / "users.csv"

# ---------- Helpers ----------
YEAR_RE = re.compile(r"\((\d{4})\)\s*$")

def extract_year(title: str):
    if not title:
        return None
    m = YEAR_RE.search(title)
    return int(m.group(1)) if m else None

def clean_title(title: str, year: int | None):
    if title and year and title.rstrip().endswith(f"({year})"):
        # remove trailing " (1995)"
        return title.rstrip()[: -(len(str(year)) + 2)].rstrip()
    return title

def main():
    now = int(time.time())
    trending_window_days = 180  # map all ratings into last 180 days

    # 1) movies.csv (normalize title/year/genres)
    movies = {}  # movieId -> (title, year, [genres])
    with MOVIES_IN.open(newline="", encoding="utf-8") as f, MOVIES_OUT.open("w", newline="", encoding="utf-8") as g:
        rd = csv.DictReader(f)
        wr = csv.writer(g)
        wr.writerow(["movieId", "title", "year", "genres"])
        for row in rd:
            try:
                mid = int(row["movieId"])
            except Exception:
                # skip malformed ids
                continue
            raw_title = row.get("title", "")
            year = extract_year(raw_title)
            title = clean_title(raw_title, year)
            genres = [g for g in (row.get("genres") or "").split("|") if g and g != "(no genres listed)"]
            movies[mid] = (title, year, genres)
            wr.writerow([mid, title, year if year is not None else "", "|".join(genres)])

    # 2) ratings.csv (write with recent timestamps; collect user stats)
    user_first_ts: dict[int, int] = {}
    user_genre_counts: dict[int, Counter] = defaultdict(Counter)

    random.seed(42)  # deterministic mapping
    with RATINGS_IN.open(newline="", encoding="utf-8") as f, RATINGS_OUT.open("w", newline="", encoding="utf-8") as g:
        rd = csv.DictReader(f)
        wr = csv.writer(g)
        wr.writerow(["userId", "movieId", "rating", "timestamp"])
        for row in rd:
            try:
                uid = int(row["userId"])
                mid = int(row["movieId"])
                rating = float(row["rating"])
            except Exception:
                continue

            # map to recent timestamp in last 180 days for trending demo
            ts = now - random.randint(0, trending_window_days * 24 * 3600)

            wr.writerow([uid, mid, rating, ts])

            if uid not in user_first_ts or ts < user_first_ts[uid]:
                user_first_ts[uid] = ts

            if mid in movies:
                for gname in movies[mid][2]:
                    user_genre_counts[uid][gname] += 1

    # 3) users.csv (synthetic demographics + top-2 genres)
    countries = ["TR", "VN", "PL", "CA", "US", "DE", "SE", "GB", "RU", "UA", "KZ", "FR", "ES", "IT", "NL"]
    random.seed(42)

    with USERS_OUT.open("w", newline="", encoding="utf-8") as g:
        wr = csv.writer(g)
        wr.writerow(["userId", "name", "joinDate", "country", "age", "genres"])
        for uid in sorted(user_first_ts.keys()):
            join_dt = dt.datetime.utcfromtimestamp(user_first_ts[uid]).strftime("%Y-%m-%d")
            country = random.choice(countries)
            age = random.randint(18, 60)
            top_genres = [g for g, _ in user_genre_counts[uid].most_common(2)]
            wr.writerow([uid, f"user_{uid}", join_dt, country, age, "|".join(top_genres)])

    print(f"Wrote: {MOVIES_OUT} {RATINGS_OUT} {USERS_OUT}")
    # quick sanity counts
    print("Samples:")
    for p in (MOVIES_OUT, RATINGS_OUT, USERS_OUT):
        with p.open(encoding="utf-8") as f:
            n = sum(1 for _ in f) - 1  # minus header
        print(f"  {p.name}: {n:,} rows")

if __name__ == "__main__":
    main()

