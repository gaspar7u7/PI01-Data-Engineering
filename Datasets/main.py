from fastapi import FastAPI, Query, HTTPException
import pandas as pd

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/word_count/{platform}/{word}")
async def get_word_count(platform: str, word: str):
    df = pd.read_csv(f"{platform}_titles-score.csv")
    count = df[df["title"].str.contains(word, case=False, na=False)].shape[0]
    return {"platform": platform, "word_count": count}

@app.get("/get_score_count/{platform}/{score}/{year}")
async def get_score_count(platform: str, score: int, year: int):
    df = pd.read_csv(f"{platform}_titles-score.csv")
    score_count = df[(df["score"] > score) & (df["release_year"] == year)].shape[0]
    return {"platform": platform, "score_count": score_count}


@app.get("/get_second_score/{platform}")
def get_second_score(platform: str):
    dataframe = pd.read_csv(f"{platform}_titles-score.csv")
    dataframe = dataframe.sort_values(by=["title"])
    dataframe = dataframe.sort_values(by=["score"], ascending=False)
    second_score = dataframe.iloc[1]["title"]
    score = dataframe.iloc[1]["score"]
    return {"title": second_score, "score": score}

@app.get("/get_longest/{platform}/{duration_type}/{year}")
def get_longest(platform: str, duration_type: str, year: int):
    dataframe = pd.read_csv(f"{platform}_titles-score.csv")
    dataframe = dataframe[(dataframe["duration_type"] == duration_type) & (dataframe["release_year"] == year)]
    dataframe = dataframe.sort_values(by=["duration_int"], ascending=False)
    title = dataframe.iloc[0]["title"]
    duration = dataframe.iloc[0]["duration_int"]
    return {"title": title, "duration": duration, "duration_type": duration_type}

@app.get("/get_rating_count/{rating}")
def get_rating_count(rating: str):
    dataframe = pd.concat([pd.read_csv(f) for f in ["amazon_titles-score.csv", "hulu_titles-score.csv", "netflix_titles-score.csv"]])
    count = dataframe[dataframe["rating"] == rating]["rating"].count()
    return {"rating": rating, "count": count}