"""
lookup.py

lookup functions for collegebaseball

created by Nathan Blumenfeld in Summer 2022
"""

from collegebaseball import guts


def lookup_season_ids(season):
    """
    A function that finds the year_stat_category_ids of a given season
    Args:
        season (int, YYYY)
    Returns:
        tuple of (season_id, batting_id, pitching_id) for desired season
    """
    df = guts.get_seasons_table()
    season_row = df.loc[df['season'] == season]
    season_id = season_row.values[0][1]
    batting_id = season_row.values[0][2]
    pitching_id = season_row.values[0][3]
    fielding_id = season_row.values[0][4]
    return int(season_id), int(batting_id), int(pitching_id), int(fielding_id)


def lookup_season_reverse(season_id):
    """
    A function that finds the year_stat_category_ids and season of a season_id
    Args:
        season_id (int): NCAA season_id
    Returns:
        tuple of (season_id, batting_id, pitching_id) for desired season
    """
    df = guts.get_seasons_table()
    season_row = df.loc[df['season_id'] == season_id]
    season = season_row['season'].values[0]
    batting_id = season_row['batting_id'].values[0]
    pitching_id = season_row['pitching_id'].values[0]
    fielding_id = season_row['fielding_id'].values[0]
    return int(season), int(batting_id), int(pitching_id), int(fielding_id)


def lookup_season_id(season):
    """
    A function that finds the year_stat_category_ids for a given season

    Args:
        season (int, YYYY)

    Returns:
        season_id as an int
    """
    df = guts.get_seasons_table()
    season_row = df.loc[df['season'] == season]
    season_id = season_row.values[0][1]
    return int(season_id)


def lookup_season_id_reverse(season_id):
    """
    A function that finds the year_stat_category_ids for a given season

    Args:
        season (int, YYYY)

    Returns:
        season_id as an int
    """
    df = guts.get_seasons_table()
    season_row = df.loc[df['season_id'] == season_id]
    season = season_row.values[0][0]
    return int(season)


def lookup_seasons_played(stats_player_seq):
    """
    A function to find the final and debut seasons of a given player

    Args:
        stats_player_seq (int): NCAA player_id

    Returns:
        tuple of ints: (debut season, most recent season)
    """
    df = guts.get_players_history_table()
    row = df.loc[df.stats_player_seq == stats_player_seq]
    return int(row['debut_season'].values[0]), int(row['season_last'].values[0])


def lookup_school(school_name):
    """
    A function to find a school's id and division from it's name

    Args:
        school_name (str): the name of the school

    Returns:
        tuple of school_id (int) and division (int)

    Examples:
        lookup_school("cornell")
        >>> 167, 1
    """
    df = guts.get_schools_table()
    school_row = df.loc[(df.ncaa_name == school_name)]
    if len(school_row) == 0:
        school_row = df.loc[(df.bd_name == school_name)]
    if len(school_row) == 0:
        return f'''could not find school {school_name}'''
    else:
        return int(school_row['school_id'].values[0]), int(school_row['division'].values[0])


def lookup_school_reverse(school_id):
    """
    A function to find a school's name and division from a school_id

    Args:
        school_id as int

    Returns:
        tuple of school_name (str), division (int)

    Examples:
        lookup_school_reverse(167)
        >>> "Cornell", 1
    """
    df = guts.get_schools_table()
    school_row = df.loc[(
        df.school_id == school_id)]
    if len(school_row) == 0:
        return f'''could not find school {school_id}'''
    else:
        return str(school_row['ncaa_name'].values[0]), int(school_row['division'].values[0])


def lookup_player(player_name, school):
    """
    A function to find a player's id from their name and school

    Args:
        player_name (str): name of player to lookup
        school: the name of the player's school (str) or school_id (int)

    Returns:
        The player_id of the player (int)

    Examples:
        lookup_player("Jake Gelof", "Virginia")
        >>> 2486499
    """
    df = guts.get_player_lu_table()
    player_row = df.loc[
        df.name == player_name.title(
        )]
    player_row = player_row.loc[player_row.school == school]
    if len(player_row) == 0:
        return f'''could not find player {player_name}'''
    else:
        return int(player_row['stats_player_seq'].values[0])


def lookup_player_reverse(player_id, season):
    """
    A function to find a player's name and school from their player_id

    Args:
        player_id (str): name of player to lookup
        season (int): YYYY

    Returns:
        player_name (str), school_name (str), school_id (int)

    """
    df = guts.get_rosters_table()
    row = df.loc[(df.stats_player_seq == player_id)
                 & (df.season == season)]
    if len(row) == 0:
        return f'''could not find player {player_id}'''
    else:
        return str(row['name'].values[0]), str(row['school'].values[0]), int(row['school_id'].values[0])


def _lookup_school_info(x):
    """
    a function to handle the school/school_id input types

    """
    if type(x) is int:
        school_id = x
        ncaa_name, division = lookup_school_reverse(school_id)
    elif type(x) is str:
        school_id, division = lookup_school(x)
        ncaa_name = x
    return str(ncaa_name), int(school_id), int(division)


def _lookup_season_info(x):
    """
    handling season/season_id input types

    """
    if len(str(x)) == 4:
        season = x
        season_id, batting_id, pitching_id, fielding_id = lookup_season_ids(x)
    elif len(str(x)) == 5:
        season_id = x
        season, batting_id, pitching_id, fielding_id = lookup_season_reverse(
            x)
    return season, season_id, batting_id, pitching_id, fielding_id


def _lookup_season_basic(x):
    """
    handling season/season_id input types

    """
    if len(str(x)) == 4:
        season = x
        season_id = lookup_season_id(x)
    elif len(str(x)) == 5:
        season_id = x
        season = lookup_season_id_reverse(x)
    return season, season_id
