"""
A module to calculate actual and expected winning percentages from boysworld.com data

created by Nathan Blumenfeld
"""

import pandas as pd
import boydsworld_scraper

# GLOBAL VARIABLES
ROUND_TO = 3

def calculate_actual_win_pct(team_1=None, games=None, start=None, end=None, round_to=ROUND_TO):
    """
    Returns: The actual (i.e. experimental) winning percentage of a given team over given games. 

    actual winning percentage = # game won / # games plated
   
    Parameter team_name: team to return actual winning % for 
    Preconditions: team_name is a string format ex. "Cornell," "Colgate"
    Parameter games: games over which to calculate
    Precondition: games is a DataFrame returned by getGames() function
    Parameter start: the start year of games
    Precondition: start is an int
    Parameter end: the end year of games
    Precondition: end is an int
    """
    if games is None: 
            assert start is not None, "if not supplying a DataFrame of games, must specify start"
            games = boydsworld_scraper.get_games(team_1, start, end=end)     
    if len(games) > 0: 
        actual_win_pct = len(games[games.run_difference > 0]) / len(games)
    else: 
        actual_win_pct = 0
    return round(actual_win_pct, round_to)

def calculate_pythagenpat_win_pct(team_1=None, games=None, start=None, end=None, round_to=ROUND_TO):
    """
    Returns: The PythagenPat winning percentage expectation of a given team over given games. 

    W% = R^x/(R^x + RA^x)
    where x = (RPG)^.287
    Developed by David Smyth and Patriot

    Parameter team_name: team to return expected winning % for 
    Preconditions: team_name is a string format ex. "Cornell," "Colgate"
    Parameter games: games over which to calculate
    Precondition: games is a DataFrame returned by getGames() function
    Parameter start: the start year of games
    Precondition: start is an int
    Parameter end: the end year of games
    Precondition: end is an int
    """
    if games is None: 
        assert start is not None, "if not supplying a DataFrame of games, must specify start"
        games = boydsworld_scraper.get_games(team_1, start, end=end)
    runs_scored_total = games.runs_scored.sum()
    runs_allowed_total = games.runs_allowed.sum()
    games_played_count = len(games) 
    if len(games) == 0: 
        expected_win_pct = 0 
    else: 
        runs_per_game = runs_scored_total / games_played_count
        x = runs_per_game ** 0.287
        expected_win_pct = (runs_scored_total**x)/((runs_scored_total**x)+(runs_allowed_total**x))
    return round(expected_win_pct, round_to)