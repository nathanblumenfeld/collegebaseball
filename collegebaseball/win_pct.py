"""
win_pct 

A module to calculate actual and expected winning percentages 
from boysworld.com data

Created by Nathan Blumenfeld in Winter 2022
"""
import pandas as pd

# GLOBAL VARIABLES
_ROUND_TO = 3

def calculate_actual_win_pct(school, games):
    """
    A function to calculate the winning percentage as
    # games won / # games plated

    Args: 
        school (str): team to return actual winning % for 
        games (DataFrame from get_games()): output of getGames() function
        
    Returns: 
        A tuple of (the actual (i.e. experimental) winning percentage as a float, 
        # of wins as int, # ties as int, and # losses as int)
    """
    if len(games) > 0: 
        wins = len(games[games.run_difference > 0])
        ties = len(games[games.run_difference == 0])
        losses = len(games[games.run_difference < 0])
        wins_and_losses = len(games[games.run_difference != 0])
        res = wins / wins_and_losses
    else: 
        res = 0
    return round(res, _ROUND_TO), int(wins), int(ties), int(losses)

def calculate_pythagenpat_win_pct(school, games):
    """
    A function to calculate the the PythagenPat expectated winning percentage.
    Pythagenpat Expectation formula (developed by David Smyth and Patriot): 
    
        W% = R^x/(R^x + RA^x)
        where x = (RPG)^.287
        Developed by David Smyth and Patriot

    Args: 
        school (str): team to return expected winning % for 
        games (DataFrame): games over which to calculate
        start (int, YYYY): season to start at, inclusive
        end (int, YYYY): season to end at, inclusive
        
    Returns: 
        A tuple of (the expected winning percentage as a float, total run differential as int)
    """
    runs_scored_total = games.runs_scored.sum()
    runs_allowed_total = games.runs_allowed.sum()
    games_played_total = len(games) 
    total_run_difference = runs_scored_total - runs_allowed_total 
    
    if len(games) == 0: 
        res = 0 
    else: 
        runs_per_game = runs_scored_total / games_played_total
        x = runs_per_game ** 0.287
        numerator = (runs_scored_total ** x)
        demoninator = (runs_scored_total ** x) + (runs_allowed_total ** x)
        res = numerator / demoninator
        
    return round(res, _ROUND_TO), int(total_run_difference)