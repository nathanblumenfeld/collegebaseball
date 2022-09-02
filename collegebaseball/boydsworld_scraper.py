"""
boydsworld_scraper

A scraper module for boydsworld.com historical game results

created by Nathan Blumenfeld in November 2021
"""
import pandas as pd
import requests
from io import StringIO


def boydsworld_team_results(school, start, end=None, vs="all",
                            parse_dates=True):
    """
    A function to scrape game results data, from boydsworld.com
    Valid 1992 to 2021, d1 only

    Args:
        school (str): team whose games to select
        start (int): the start year of games, 1992 <= start <= 2022
        end (int):  the end season of games, 1992 <= end <= 2022

        vs (str): school to filter games against. default: 'all'
        parse_dates (bool): whether to parse data into datetime64

    Returns:
        Dataframe of all games played for a given team inclusive of start & end
        data from boydsworld.com

    """
    try:
        df = (_get_data(school, start, end=end, vs=vs, parse_dates=parse_dates)
              .pipe(_enrich_data, school)
              .pipe(_set_dtypes)
              .drop(columns=["team_1", "team_1_score", "team_2",
                             "team_2_score"])
              .sort_values(by="date", axis=0, ascending=True)
              )
        return df
    except:
        print(f'''no records found for {school} between {start} and {end}''')
        return pd.DataFrame()


def _get_data(school, start, end=None, vs="all", parse_dates=True):
    """
    A helper function to send GET request to boydsworld.com and parse data
    """
    col_names = ["date", "team_1", "team_1_score",
                 "team_2", "team_2_score", "field"]
    url = 'http://www.boydsworld.com/cgi/scores.pl'
    if end is None:
        end = start
    try:
        payload = {"team1": school, "firstyear": str(start), "team2": vs,
                   "lastyear": str(end), "format": "HTML", "submit": "Fetch"}
        with requests.Session() as s:
            r = s.get(url, params=payload)
        response = r.text
        io = StringIO(response).read()
        dfs = pd.read_html(io=io, parse_dates=parse_dates)
        df = dfs[1].dropna(how="all", axis=1, inplace=False)
        if len(df.columns) != len(col_names):
            print("no records found")
            return pd.DataFrame()
        else:
            df.columns = col_names
            if parse_dates:
                df.loc[:, 'date'] = pd.to_datetime(
                    df.loc[:, 'date'], infer_datetime_format=True)
            return df
    except:
        return pd.DataFrame()


def _enrich_data(df, school):
    """
    A helper function that adds the following columns to a given DataFrame:

        opponent (str): opponent for each game.
        runs_allowed (int): the number of runs scored by team_1 in each game
        runs_scored (int): the number of runs scored by team_1 in each game
        run_difference (int): the difference between team_1's runs scored
            and runs allowed for each game
    """
    wins = df[(df["team_1"] == school) &
              (df["team_1_score"] > df["team_2_score"])].copy()
    losses = df[(df["team_2"] == school) &
                (df["team_1_score"] > df["team_2_score"])].copy()
    wins.loc[:, "runs_scored"] = wins.loc[:, "team_1_score"]
    wins.loc[:, "runs_allowed"] = wins.loc[:, "team_2_score"]
    wins.loc[:, "opponent"] = wins.loc[:, "team_2"]
    losses.loc[:, "runs_scored"] = losses.loc[:, "team_2_score"]
    losses.loc[:, "runs_allowed"] = losses.loc[:, "team_1_score"]
    losses.loc[:, "opponent"] = losses.loc[:, "team_1"]
    df = pd.concat([wins, losses])
    df.loc[:, "run_difference"] = df.loc[:, "runs_scored"] \
        - df.loc[:, "runs_allowed"]
    return df


def _set_dtypes(df):
    """
    A helper function to sets the datatypes of newly added columns
    """
    df.loc[:, "run_difference"] = df.loc[:, "run_difference"].astype(int)
    df.loc[:, "runs_allowed"] = df.loc[:, "runs_allowed"].astype(int)
    df.loc[:, "runs_scored"] = df.loc[:, "runs_scored"].astype(int)
    return df
