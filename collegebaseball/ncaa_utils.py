"""
ncaa_utils.py

utilities for collegebaseball's ncaa_scraper

created by Nathan Blumenfeld in Summer 2022
"""
import numpy as np


def _format_names(original: str):
    """
    A helper function to turn names from "Last, First" to "First Last"
    Args:
        original (str): the name to reformat. must be in form "Last, First"
    Returns:
        str of format "First Last"
    Examples:
        _format_names("Blumenfeld, Nathan")
        "Nathan Blumenfeld"
    """
    try:
        split = original.split(',')
        split.reverse()
        res = ' '.join(split).strip().title()
    except:
        res = np.nan
    return res


def _eliminate_dashes(df):
    """
    A helper function to replace the weird dashes the NCAA uses with 0.00

    Args:
        df (DataFrame):
    Returns:
        Dataframe with dashes replaced by 0.00 (not a copy!)
    """
    formats = ['None', '<NA>', '', ' ', '-', '--', '---']
    for i in formats:
        df.replace(i, 0.00, inplace=True)
    return df


def _transform_stats(df):
    """
    A helper function to transform raw data obtained with get_career_stats
    Args:
        df: DataFrame output from get_career_stats function
    Returns:
        DataFrame
    """
    df = _eliminate_dashes(df)
    cols = df.columns
    if 'Player' in cols:
        df.rename(columns={'Player': 'name'}, inplace=True)
        cols = df.columns
    if 'name' in cols:
        df.loc[:, 'name'] = df.loc[:, 'name'].apply(_format_names)
        df.loc[:, 'name'] = df.loc[:, 'name'].astype('string')
    if 'Team' in cols:
        df = df.rename(columns={'Team': 'school_id'}, inplace=False)
        cols = df.columns
    # need to do this after name formatting, which relies on commas
    df = df.replace(',', '', regex=True)
    df = df.fillna(value=0.00, inplace=False)
    # we're going to calculate OBP/BA/SLG ourselves
    # G and RBI2out are unreliable
    drops = ['OBPct', 'BA', 'SlgPct', 'RBI2out', 'G']
    for i in drops:
        if i in cols:
            df = df.drop(columns=[i], inplace=False)
    data_types = {
        'int8': ['division', 'innings_played'],
        'int32': ['opponent_id', 'season_id', 'school_id', 'game_id'],
        'int16': ['runs_scored', 'runs_allowed', 'run_difference',
                  'season', 'GP', 'GS', 'BB', 'Jersey', 'DP', 'H', 'DP' 'R',
                  'ER', 'SO', 'TB', '2B', '3B', 'HR', 'RBI', 'R', 'AB', 'HBP',
                  'SF', 'K', 'SH', 'Picked', 'SB', 'IBB', 'CS', 'OPP DP',
                  'SHO', 'BF', 'P-OAB', '3B-A', '2B-A', 'Bk', 'HR-A', 'WP',
                  'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 'GO',
                  'FO', 'W', 'L', 'HB', 'SV', 'KL', 'pickoffs', 'OrdAppeared',
                  'App', 'GDP', 'PO', 'A', 'TC', 'E', 'CI', 'PB',
                  'SBA', 'CSB', 'IDP', 'TP'],
        'bool': ['extras'],
        'float': ['ERA', 'IP'],
        'string': ['Yr', 'Pos', 'date', 'Year', 'school', 'opponent_name', 'school_name']
    }
    for i in data_types.keys():
        for j in data_types[i]:
            if j in cols:
                df[j] = df[j].astype(i)
    # keeping as 64 byte to leave space for new potential player uuids
    if 'stats_player_seq' in cols:
        df.loc[:, 'stats_player_seq'] = df.stats_player_seq.astype('string')
        df.loc[:, 'stats_player_seq'] = df.stats_player_seq.str.replace(
            r'\D+', '')
        df.loc[:, 'stats_player_seq'] = df.stats_player_seq.astype('int64')
    if 'date' in cols:
        df.loc[:, 'date'] = df.loc[:, 'date'].astype('string')
        df.loc[:, 'season'] = df.loc[:, 'date'].str[-4:]
        df.loc[:, 'season'] = df.loc[:, 'season'].astype('int32')
    if 'Year' in cols:
        df.loc[:, 'Year'] = df.loc[:, 'Year'].astype('string')
        df.loc[:, 'season'] = df.loc[:, 'Year'].str[:4]
        df.loc[:, 'season'] = df.loc[:, 'season'].astype('int32')
        df.drop(columns=['Year'], inplace=True)
    if 'Pos' in cols:
        df = df.rename(columns={'Pos': 'pos'}, inplace=False)
        cols = df.columns
    if 'Pitches' in cols:
        df = df.rename(columns={'Pitches': 'pitches'}, inplace=False)
        df.loc[:, 'pitches'] = df.loc[:, 'pitches'].astype('string')
        df.loc[:, 'pitches'] = df.loc[:, 'pitches'].astype('float')
        df.loc[:, 'pitches'] = df.loc[:, 'pitches'].astype('int32')
    if 'ERA' in cols:
        df.loc[:, 'ERA'] = df.loc[:, 'ERA'].round(4)
    if 'IP' in cols:
        df.loc[:, 'IP'] = df.loc[:, 'IP'].round(4)
    # drops duplicated columns (stats.ncaa.org sometimes has this issue)
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def _has_no_id(tag):
    return tag.name == 'tr' and not tag.has_attr('id')


#    n.b. due to inconsistencies in NCAA formatting and bad code
#    (e.g. ghost columns), we are hard-coding these so things don't
#    get too messy
team_gamelog_headers = {
    'batting': {
        2022: ['G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR',
               'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'OPP DP',
               'CS', 'Picked', 'SB', 'IBB',
               'GDP', 'RBI2out', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'game_id',
               'school_id'],
        2021: ['G', 'R', 'AB', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'OPP DP',
               'CS', 'Picked', 'SB', 'IBB', 'RBI2out', 'date', 'field'
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'game_id',
               'school_id'],
        2020: ['G', 'R', 'AB', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'OPP DP',
               'CS', 'Picked', 'SB', 'IBB', 'RBI2out', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2019: ['G', 'R', 'AB', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP',
               'CS', 'Picked', 'SB', 'IBB', 'RBI2out', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2018: ['G', 'R', 'AB', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP',
               'CS', 'Picked', 'SB', 'IBB', 'RBI2out', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2017: ['G', 'R', 'AB', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP',
               'CS', 'Picked', 'SB', 'IBB', 'RBI2out', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2016: ['G', 'R', 'AB', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP',
               'CS', 'Picked', 'SB', 'RBI2out', 'IBB', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2015: ['G', 'R', 'AB', 'H', '2B',
               '3B', 'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K',
               'DP', 'SB', 'CS', 'Picked', 'IBB', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2014: ['G', 'AB', 'R', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP', 'SB',
               'CS', 'Picked', 'IBB', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed', 'run_difference',
               'result', 'game_id', 'school_id'],
        2013: ['AB', 'H', 'TB', 'R', '2B', '3B',
               'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP', 'SB',
               'CS', 'Picked', 'IBB', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed', 'run_difference',
               'result', 'game_id', 'school_id'],
        2012: ['AB', 'R', 'H', '2B', '3B', 'HR',
               'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP', 'SB', 'CS',
               'Picked', 'IBB', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed', 'run_difference',
               'result', 'game_id', 'school_id']
    },
    'pitching': {
        2022: ['G', 'App', 'GS', 'IP', 'CG', 'H',
               'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A',
               '3B-A', 'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run',
               'Inh Run Score', 'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W',
               'L', 'SV', 'OrdAppeared', 'KL', 'pickoffs', 'date',
               'field', 'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'game_id',
               'school_id'],
        2021: ['App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO',
               'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP',
               'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA',
               'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared',
               'KL', 'pickoffs', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2020: ['App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO',
               'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP',
               'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA',
               'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared',
               'KL', 'pickoffs', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed', 'run_difference',
               'result', 'game_id', 'school_id'],
        2019: ['App', 'G', 'GS', 'IP', 'CG', 'H',
               'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A',
               'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2018: ['App', 'G', 'GS', 'IP', 'CG', 'H',
               'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A',
               'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2017: ['App', 'G', 'GS', 'IP', 'CG', 'H',
               'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A',
               'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed', 'run_difference',
               'result', 'game_id', 'school_id'],
        2016: ['App', 'G', 'GS', 'IP', 'CG', 'H',
               'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A',
               'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2015: ['App', 'G', 'GS', 'IP', 'H', 'R',
               'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A',
               'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2014: ['App', 'GS', 'IP', 'H', 'R', 'ER',
               'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk',
               'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'game_id',
               'school_id'],
        2013: ['App', 'GS', 'IP', 'H',
               'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A',
               '3B-A', 'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run',
               'Inh Run Score', 'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W',
               'L', 'SV', 'OrdAppeared', 'KL', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'game_id',
               'school_id'],
        2012: ['App', 'GS', 'IP', 'H', 'R', 'ER',
               'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk',
               'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played'
               'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'game_id',
               'school_id'],
    },
    'fielding':  {
        2022: ['G', 'PO', 'A', 'TC', 'E', 'CI', 'PB', 'SBA', 'CSB', 'IDP',
               'TP', 'date', 'field', 'season_id', 'opponent_id',
               'opponent_name', 'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2021: ['G', 'PO', 'A', 'TC', 'E', 'CI',
               'PB', 'SBA', 'CSB', 'IDP', 'TP', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2020: ['G', 'PO', 'A', 'TC', 'E', 'CI',
               'PB', 'SBA', 'CSB', 'IDP', 'TP', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2019: ['G', 'PO', 'A', 'TC', 'E', 'CI',
               'PB', 'SBA', 'CSB', 'IDP', 'TP', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2018: ['G', 'PO', 'TC', 'A', 'E', 'CI',
               'PB', 'SBA', 'CSB', 'IDP', 'TP', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2017: ['G', 'G', 'PO', 'TC', 'A', 'E',
               'CI', 'PB', 'SBA', 'CSB', 'IDP', 'TP', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2016: ['G', 'PO', 'A', 'E', 'CI', 'PB',
               'SBA', 'CSB', 'IDP', 'TP', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed', 'run_difference',
               'result', 'game_id', 'school_id'],
        2015: ['G', 'PO', 'A', 'E', 'CI', 'PB',
               'SBA', 'CSB', 'IDP', 'TP', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2014: ['G', 'PO', 'A', 'E', 'CI', 'PB',
               'SBA', 'CSB', 'IDP', 'TP', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2013: ['PO', 'A', 'E', 'CI', 'PB', 'SBA',
               'CSB', 'IDP', 'TP', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id'],
        2012: ['PO', 'A', 'E', 'CI', 'PB', 'SBA',
               'CSB', 'IDP', 'TP', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id']
    }
}

player_gamelog_headers = {
    'batting': {
        2022: ['G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR',
               'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'OPP DP',
               'CS', 'Picked', 'SB', 'IBB',
               'GDP', 'RBI2out', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'game_id',
               'school_id', 'stats_player_seq'],
        2021: ['G', 'R', 'AB', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'OPP DP',
               'CS', 'Picked', 'SB', 'IBB', 'RBI2out', 'date', 'field'
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'game_id',
               'school_id', 'stats_player_seq'],
        2020: ['G', 'R', 'AB', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'OPP DP',
               'CS', 'Picked', 'SB', 'IBB', 'RBI2out', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2019: ['G', 'R', 'AB', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP',
               'CS', 'Picked', 'SB', 'IBB', 'RBI2out', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2018: ['G', 'R', 'AB', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP',
               'CS', 'Picked', 'SB', 'IBB', 'RBI2out', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2017: ['G', 'R', 'AB', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP',
               'CS', 'Picked', 'SB', 'IBB', 'RBI2out', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2016: ['G', 'R', 'AB', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP',
               'CS', 'Picked', 'SB', 'RBI2out', 'IBB', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2015: ['G', 'R', 'AB', 'H', '2B',
               '3B', 'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K',
               'DP', 'SB', 'CS', 'Picked', 'IBB', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2014: ['G', 'AB', 'R', 'H', '2B', '3B',
               'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP', 'SB',
               'CS', 'Picked', 'IBB', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed', 'run_difference',
               'result', 'game_id', 'school_id', 'stats_player_seq'],
        2013: ['AB', 'H', 'TB', 'R', '2B', '3B',
               'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP', 'SB',
               'CS', 'Picked', 'IBB', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed', 'run_difference',
               'result', 'game_id', 'school_id', 'stats_player_seq'],
        2012: ['AB', 'R', 'H', '2B', '3B', 'HR',
               'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP', 'SB', 'CS',
               'Picked', 'IBB', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed', 'run_difference',
               'result', 'game_id', 'school_id', 'stats_player_seq']
    },
    'pitching': {
        2022: ['G', 'App', 'GS', 'IP', 'CG', 'H',
               'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A',
               '3B-A', 'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run',
               'Inh Run Score', 'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W',
               'L', 'SV', 'OrdAppeared', 'KL', 'pickoffs', 'date',
               'field', 'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'game_id',
               'school_id', 'stats_player_seq'],
        2021: ['App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO',
               'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP',
               'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA',
               'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared',
               'KL', 'pickoffs', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2020: ['App', 'G', 'GS', 'IP', 'CG', 'H', 'R', 'ER', 'BB', 'SO',
               'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk', 'HR-A', 'WP',
               'HB', 'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA',
               'Pitches', 'GO', 'FO', 'W', 'L', 'SV', 'OrdAppeared',
               'KL', 'pickoffs', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed', 'run_difference',
               'result', 'game_id', 'school_id', 'stats_player_seq'],
        2019: ['App', 'G', 'GS', 'IP', 'CG', 'H',
               'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A',
               'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2018: ['App', 'G', 'GS', 'IP', 'CG', 'H',
               'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A',
               'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2017: ['App', 'G', 'GS', 'IP', 'CG', 'H',
               'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A',
               'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed', 'run_difference',
               'result', 'game_id', 'school_id', 'stats_player_seq'],
        2016: ['App', 'G', 'GS', 'IP', 'CG', 'H',
               'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A',
               'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2015: ['App', 'G', 'GS', 'IP', 'H', 'R',
               'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A',
               'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2014: ['App', 'GS', 'IP', 'H', 'R', 'ER',
               'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk',
               'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'game_id',
               'school_id', 'stats_player_seq'],
        2013: ['App', 'GS', 'IP', 'H',
               'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A',
               '3B-A', 'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run',
               'Inh Run Score', 'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W',
               'L', 'SV', 'OrdAppeared', 'KL', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'game_id',
               'school_id', 'stats_player_seq'],
        2012: ['App', 'GS', 'IP', 'H', 'R', 'ER',
               'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk',
               'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
               'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
               'OrdAppeared', 'KL', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played'
               'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'game_id',
               'school_id', 'stats_player_seq'],
    },
    'fielding':  {
        2022: ['G', 'PO', 'A', 'TC', 'E', 'CI', 'PB', 'SBA', 'CSB', 'IDP',
               'TP', 'date', 'field', 'season_id', 'opponent_id',
               'opponent_name', 'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2021: ['G', 'PO', 'A', 'TC', 'E', 'CI',
               'PB', 'SBA', 'CSB', 'IDP', 'TP', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2020: ['G', 'PO', 'A', 'TC', 'E', 'CI',
               'PB', 'SBA', 'CSB', 'IDP', 'TP', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2019: ['G', 'PO', 'A', 'TC', 'E', 'CI',
               'PB', 'SBA', 'CSB', 'IDP', 'TP', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2018: ['G', 'PO', 'TC', 'A', 'E', 'CI',
               'PB', 'SBA', 'CSB', 'IDP', 'TP', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2017: ['G', 'G', 'PO', 'TC', 'A', 'E',
               'CI', 'PB', 'SBA', 'CSB', 'IDP', 'TP', 'date', 'field',
               'season_id', 'opponent_id', 'opponent_name',
               'innings_played', 'extras', 'runs_scored',
               'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2016: ['G', 'PO', 'A', 'E', 'CI', 'PB',
               'SBA', 'CSB', 'IDP', 'TP', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played',
               'extras', 'runs_scored', 'runs_allowed', 'run_difference',
               'result', 'game_id', 'school_id', 'stats_player_seq'],
        2015: ['G', 'PO', 'A', 'E', 'CI', 'PB',
               'SBA', 'CSB', 'IDP', 'TP', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2014: ['G', 'PO', 'A', 'E', 'CI', 'PB',
               'SBA', 'CSB', 'IDP', 'TP', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2013: ['PO', 'A', 'E', 'CI', 'PB', 'SBA',
               'CSB', 'IDP', 'TP', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq'],
        2012: ['PO', 'A', 'E', 'CI', 'PB', 'SBA',
               'CSB', 'IDP', 'TP', 'date', 'field', 'season_id',
               'opponent_id', 'opponent_name', 'innings_played', 'extras',
               'runs_scored', 'runs_allowed', 'run_difference', 'result',
               'game_id', 'school_id', 'stats_player_seq']
    }
}
# try reading these automatically?
# html changes year to year make it difficult to automatically read tables
# and NCAA likes to mess with the columns in different years
# so this in -elegant solution is what we have
available_stat_ids = {
    'batting': {
        2022: {
            'two_outs': '17200',
            'vs_RH': '17201',
            'runners_on': '17206',
            'with_RISP': '17207',
            'vs_LH': '17208',
            'bases_empty': '17214',
            'bases_loaded': '17215'
        },
        2021: {
            'two_outs': '17120',
            'vs_RH': '17121',
            'runners_on': '17126',
            'with_RISP': '17127',
            'vs_LH': '17128',
            'bases_empty': '17134',
            'bases_loaded': '17135'
        },
        2020: {
            'two_outs': '17060',
            'vs_RH': '17061',
            'runners_on': '17066',
            'with_RISP': '17067',
            'vs_LH': '17068',
            'bases_empty': '17074',
            'bases_loaded': '17075'
        },
        2019: {
            'two_outs': '16908',
            'vs_RH': '16909',
            'runners_on': '16914',
            'with_RISP': '16915',
            'vs_LH': '16916',
            'bases_empty': '16922',
            'bases_loaded': '16923'
        },
        2018: {
            'two_outs': '11960',
            'vs_RH': '11961',
            'runners_on': '11966',
            'with_RISP': '11967',
            'vs_LH': '11968',
            'bases_empty': '11974',
            'bases_loaded': '11975'
        },
        2017: {
            'two_outs': '10580',
            'vs_RH': '10581',
            'runners_on': '10586',
            'with_RISP': '10587',
            'vs_LH': '10588',
            'bases_empty': '10594',
            'bases_loaded': '10595'
        },
        2016: {
            'two_outs': '10534',
            'vs_RH': '10535',
            'runners_on': '10540',
            'with_RISP': '10541',
            'vs_LH': '10542',
            'bases_empty': '10548',
            'bases_loaded': '10549'
        },
        2015: {
            'two_outs': '10280',
            'vs_RH': '10281',
            'runners_on': '10286',
            'with_RISP': '10287',
            'vs_LH': '10288',
            'bases_empty': '10294',
            'bases_loaded': '10295'
        },
        2014: {
            'two_outs': '10200',
            'vs_RH': '10201',
            'runners_on': '10206',
            'with_RISP': '10207',
            'vs_LH': '10208',
            'bases_empty': '10214',
            'bases_loaded': '10215'
        },
        2013: {
            'two_outs': '10160',
            'vs_RH': '10161',
            'runners_on': '10166',
            'with_RISP': '10167',
            'vs_LH': '10168',
            'bases_empty': '10174',
            'bases_loaded': '10175'
        },
        2012: {
            'two_outs': '10092',
            'vs_RH': '10093',
            'runners_on': '10098',
            'with_RISP': '10099',
            'vs_LH': '10100',
            'bases_empty': '10106',
            'bases_loaded': '10107'
        }
    },
    'pitching': {
        2022: {
            'runners_on': '17217',
            'vs_LH': '17218',
            'two_outs': '17219',
            'bases_empty': '17220',
            'with_RISP': '17221',
            'bases_loaded': '17224',
            'vs_RH': '17225'
        },
        2021: {
            'runners_on': '17137',
            'vs_LH': '17138',
            'two_outs': '17139',
            'bases_empty': '17140',
            'with_RISP': '17141',
            'bases_loaded': '17144',
            'vs_RH': '17145'
        },
        2020: {
            'runners_on': '17077',
            'vs_LH': '17078',
            'two_outs': '17079',
            'bases_empty': '17080',
            'with_RISP': '17081',
            'bases_loaded': '17084',
            'vs_RH': '17085'
        },
        2019: {
            'runners_on': '16925',
            'vs_LH': '16926',
            'two_outs': '16927',
            'bases_empty': '16928',
            'with_RISP': '16929',
            'bases_loaded': '16932',
            'vs_RH': '16933'
        },
        2018: {
            'runners_on': '11977',
            'vs_LH': '11978',
            'two_outs': '11979',
            'bases_empty': '11980',
            'with_RISP': '11981',
            'bases_loaded': '11984',
            'vs_RH': '11985'
        },
        2017: {
            'runners_on': '10597',
            'vs_LH': '10598',
            'two_outs': '10599',
            'bases_empty': '10600',
            'with_RISP': '10601',
            'bases_loaded': '10604',
            'vs_RH': '10605'
        },
        2016: {
            'runners_on': '10551',
            'vs_LH': '10552',
            'two_outs': '10553',
            'bases_empty': '10554',
            'with_RISP': '10555',
            'bases_loaded': '10558',
            'vs_RH': '10559'
        },
        2015: {
            'runners_on': '10297',
            'vs_LH': '10298',
            'two_outs': '10299',
            'bases_empty': '10300',
            'with_RISP': '10301',
            'bases_loaded': '10304',
            'vs_RH': '10305'
        },
        2014: {
            'runners_on': '10217',
            'vs_LH': '10218',
            'two_outs': '10219',
            'bases_empty': '10220',
            'with_RISP': '10221',
            'bases_loaded': '10224',
            'vs_RH': '10225'
        },
        2013: {
            'runners_on': '10177',
            'vs_LH': '10178',
            'two_outs': '10179',
            'bases_empty': '10180',
            'with_RISP': '10181',
            'bases_loaded': '10184',
            'vs_RH': '10185'
        },
        2012: {
            'runners_on': '10109',
            'vs_LH': '10110',
            'two_outs': '10111',
            'bases_empty': '10112',
            'with_RISP': '10113',
            'bases_loaded': '10116',
            'vs_RH': '10117'
        }
    }
}


def _parse_score(score):
    if score == '-':
        return 0, 0, 0, 'cancelled', 0, False
    score = score.replace('W', '')
    score = score.replace('L', '')
    score = score.replace('T', '')
    if '(' in score:
        ip = score.split(
            '(')[-1].split(')')[0].strip()
        if int(ip) > 9:
            extras = 'True'
        else:
            extras = 'False'
        score = score.split('(')[0].strip()
    else:
        ip = '9'
        extras = 'False'
    scores = score.split('-')
    scored = int(scores[0].strip())
    allowed = int(scores[-1].strip())
    difference = scored - allowed
    if difference > 0:
        result = 'win'
    elif difference < 0:
        result = 'loss'
    else:
        result = 'tie'
    return scored, allowed, difference, result, ip, extras


def _parse_opponent_info(opponent_info):
    if opponent_info[0] == '@':
        field = 'away'
        opponent_name = opponent_info.split(
            '@')[-1].strip()
    elif '@' in opponent_info:
        field = 'neutral'
        opponent_name = opponent_info.split(
            '@')[0].strip()
    else:
        field = 'home'
        opponent_name = opponent_info
    return opponent_name, field
