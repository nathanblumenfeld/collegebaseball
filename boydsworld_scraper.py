"""
Scraper module for boydsworld.com historical game results 

Nathan Blumenfeld
November 11th 2021
"""
# Imports
import pandas as pd
import numpy as np
import requests
from io import StringIO
from datetime import date
import lxml 

# GLOBALS
URL = "http://www.boydsworld.com/cgi/scores.pl"

# MAIN FUNCTION
def get_games(team_1,start,end=None,team_2="all",col_names=["date", "team_1", "team_1_score", "team_2", "team_2_score", "field"],parse_dates=True,url=URL):
    """
    Returns: a dataframe of all games played for a given team inclusive of given start & end year
    Data from boydsworld.com

    Parameter team_name: team whose games to select 
    Precondition: lowercase str 
    Parameter start: the start year of games. To select only games from one year, leave  
    Precondition: start is an int >= 1992
    Parameter end: the end year of games
    Precondition: end is an int <= 2020
    """
    df = (load_data(team_1,start,end=end,team_2=team_2,parse_dates=parse_dates,url=url)
            .pipe(enrich_data,team_1)
            .pipe(set_dtypes)
            .drop(columns=["team_1","team_1_score","team_2","team_2_score"])
            .sort_values(by="date",axis=0,ascending=True)
          )
    # boydsworld sometimes struggles with single year inquiries 
    return df

# HELPER FUNCTIONS
def load_data(team_1,start,end=None,team_2="all",col_names=["date", "team_1", "team_1_score", "team_2", "team_2_score", "field"],parse_dates=True,url=URL):
    """
    Returns: DataFrame
    """
    if end is None: 
        end = start
    # build payload
    payload = {"team1":team_1,"firstyear":str(start),"team2":team_2,"lastyear":str(end),"format":"HTML","submit":"Fetch"}
    # start Requests session
    s = requests.Session()
    # send GET request
    r = requests.get(url, params=payload)
    response = r.text
    io = StringIO(response).read()
    # parse HTML into DataFrame
    dfs = pd.read_html(io=io, parse_dates=parse_dates)
    df = dfs[1].dropna(how="all", axis=1)
    # reset column names
    if len(df.columns) != len(col_names):
        print("no records were found. If you believe this is a mistake, please open a bug report")
        return pd.DataFrame()
    df.columns = col_names
    if parse_dates:
        # make sure dates are parsed as type datetime64[ns]
#         df = df.astype({"date":"datetime64[ns]"})
        df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
#         df['date'] = pd.to_datetime(df['date']).dt.date
    return df

def enrich_data(df, team_1):
    """
    Returns: copy of the given DataFrame with the following columns added
    
    opponent (str): team_1's opponent for each game.
    runs_allowed (int): the number of runs scored by team_1 in each game
    runs_scored (int): the number of runs scored by team_1 in each game
    run_difference (int): the difference between team_1's runs scored and runs allowed for each game
    """
    wins = df[(df["team_1"] == team_1) & (df["team_1_score"] > df["team_2_score"])].copy()
    losses = df[(df["team_2"] == team_1) & (df["team_1_score"] > df["team_2_score"])].copy()
    # set for wins
    wins.loc[:,"runs_scored"] = wins.loc[:,"team_1_score"]
    wins.loc[:,"runs_allowed"] = wins.loc[:,"team_2_score"]
    wins.loc[:,"opponent"] = wins.loc[:,"team_2"]
    # set for losses
    losses.loc[:,"runs_scored"] = losses.loc[:,"team_2_score"]
    losses.loc[:,"runs_allowed"] = losses.loc[:,"team_1_score"]       
    losses.loc[:,"opponent"] = losses.loc[:,"team_1"]      
    # combine dfs
    df = pd.concat([wins,losses])
    # set run difference 
    df.loc[:,"run_difference"] = df.loc[:,"runs_scored"] - df.loc[:,"runs_allowed"]
    return df 

def set_dtypes(df):
    """
    Sets the datatype of newly added columns
    """
    df.loc[:,"run_difference"] = df.loc[:,"run_difference"].astype(int)
    df.loc[:,"runs_allowed"] = df.loc[:,"runs_allowed"].astype(int)
    df.loc[:,"runs_scored"] = df.loc[:,"runs_scored"].astype(int)
    return df
