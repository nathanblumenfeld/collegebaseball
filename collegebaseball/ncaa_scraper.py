"""
ncaa_scraper

A module to scrape and parse college baseball statistics from stats.ncaa.org

Originally created by Nathan Blumenfeld in Spring 2022
"""
import pandas as pd
from time import sleep
import random
from bs4 import BeautifulSoup, Tag
import numpy as np
from collegebaseball import datasets, metrics
from requests import Session


# GET request options
_HEADERS = {'User-Agent': 'Mozilla/5.0'}
_TIMEOUT = 4


def ncaa_team_stats(school, season, variant, include_advanced=True,
                    split=None):
    """
    Obtains player-level aggregate single-season batting/pitching stats totals
    for all players from a given school

    Args:
        school: schools (str) or NCAA school_id (int)
        season: season (int, YYYY) or NCAA season_id (int), valid 2013-2022
        variant (str): 'batting', 'pitching', or 'fielding'
        include_advanced (bool, optional). Whether to
         automatically calcuate advanced metrics, Defaults to True
        split (str, optional): 'vs_LH', 'vs_RH', 'runners_on', 'bases_empty',
        'bases_loaded', 'with_RISP', 'two_outs'

    Returns:
       pd.DataFrame

    data from stats.ncaa.org. valid 2013-2022
    """
    season, season_id, batting_id, pitching_id, fielding_id = lookup_season_info(
        season)
    school, school_id = lookup_school_info(school)
    if variant == 'batting':
        year_stat_category_id = batting_id
    elif variant == 'pitching':
        year_stat_category_id = pitching_id
    else:
        year_stat_category_id = fielding_id
    url = 'https://stats.ncaa.org/team/'+str(school_id)+'/stats'
    payload = {'game_sport_year_ctl_id': str(season_id),
               'id': str(season_id),
               'year_stat_category_id': str(year_stat_category_id)
               }
    if split is not None and variant != 'fielding':
        available_stat_id = available_stat_ids[variant][season][split]
        payload['available_stat_id'] = available_stat_id
    with Session() as s:
        r = s.get(url, params=payload, headers=_HEADERS)
    if r.status_code == 403:
        print('An error occurred with the GET Request')
        print('403 Error: NCAA blocked request')
        return pd.DataFrame()
    soup = BeautifulSoup(r.text, features='lxml')
    if len(soup.find_all(name='table', id='stat_grid')) < 1:
        return pd.DataFrame()
    table = soup.find_all(name='table', id='stat_grid')[-1]
    if table is None:
        print('no data found')
        return pd.DataFrame()
    headers = [x for x in table.thead.tr.stripped_strings]
    headers.insert(headers.index('Player'), 'stats_player_seq')
    if season == 2022 and variant == 'batting':
        headers.remove('RBI2out')
    rows = []
    for i in table.tbody.find_all('tr'):
        row = []
        for j in i.find_all('td'):
            if 'data-order' in j.attrs:
                row.append(j.attrs['data-order'])
            else:
                if j.find('a') is not None:
                    if 'href' in j.a.attrs:
                        row.append(
                            int(j.a.attrs['href'].split('&')[-1].split('=')[-1]))
                    else:
                        row.append('-')
                row.append(j.string)
        if len(row) > len(headers):
            row = row[:-1]
        rows.append(row)
    df = pd.DataFrame(rows, columns=headers)
    if len(df) < 1:
        return pd.DataFrame()
    df.columns = headers
    df['season'] = season
    res = _transform_stats(df)
    if variant == 'batting':
        if include_advanced:
            if len(res) > 0:
                res = metrics.add_batting_metrics(res)
                res = res.loc[res.PA > 0]
    elif variant == 'pitching':
        if 'App' in res.columns:
            res = res.loc[res.App > 0]
        if include_advanced:
            if len(res) > 0:
                res = metrics.add_pitching_metrics(res)
    return res


def ncaa_team_totals(school, season, variant, include_advanced=True,
                     split=None):
    """
    Obtains team-level aggregate single-season batting/pitching stats
    totals for all teams

    Args:
        school: schools (str) or NCAA school_id (int)
        season: season (int, YYYY) or NCAA season_id (int), valid 2013-2022
        variant (str): 'batting', 'pitching', or 'fielding'
        include_advanced (bool, optional). Whether to
         automatically calcuate advanced metrics, Defaults to True
        split (str, optional): 'vs_LH', 'vs_RH', 'runners_on', 'bases_empty',
        'bases_loaded', 'with_RISP', 'two_outs'

    Returns:
        pd.DataFrame

    data from stats.ncaa.org. valid 2013-2022
    """
    school, school_id = lookup_school_info(school)
    season, season_id, batting_id, pitching_id, fielding_id = lookup_season_info(
        season)
    if variant == 'batting':
        year_stat_category_id = batting_id
    elif variant == 'pitching':
        year_stat_category_id = pitching_id
    else:
        year_stat_category_id = fielding_id
    url = 'https://stats.ncaa.org/team/'+str(school_id)+'/stats'
    payload = {'game_sport_year_ctl_id': str(season_id),
               'id': str(season_id),
               'year_stat_category_id': str(year_stat_category_id)
               }
    if split is not None and variant != 'fielding':
        available_stat_id = available_stat_ids[variant][season][split]
        payload['available_stat_id'] = available_stat_id
    with Session() as s:
        r = s.get(url, params=payload, headers=_HEADERS)
    if r.status_code == 403:
        print('An error occurred with the GET Request')
        print('403 Error: NCAA blocked request')
        return pd.DataFrame()
    soup = BeautifulSoup(r.text, features='lxml')
    if len(soup.find_all(name='table', id='stat_grid')) < 1:
        print('no data found')
        return pd.DataFrame()
    table = soup.find(name='table', id='stat_grid')
    if table is None:
        print('no data found')
        return pd.DataFrame()
    headers = [x for x in table.thead.tr.stripped_strings]
    if season == 2022 and variant == 'batting':
        headers.remove('RBI2out')
    rows = []
    for i in table.tfoot.find_all('tr'):
        row = []
        for j in i.find_all('td'):
            if 'data-order' in j.attrs:
                row.append(j.attrs['data-order'])
            else:
                row.append(j.string)
        if len(row) > len(headers):
            row = row[:-1]
        rows.append(row)
    df = pd.DataFrame(rows, columns=headers)
    df['season'] = season
    res = _transform_stats(df)
    cols_to_drop = ['Jersey', 'Yr', 'pos', 'GP', 'GS', 'App']
    for i in cols_to_drop:
        if i in res.columns:
            res = res.drop(columns=[i], inplace=False)
    if variant == 'batting':
        if include_advanced:
            if len(res) > 0:
                res = metrics.add_batting_metrics(res)
                res = res.loc[res.PA > 0]
    elif variant == 'pitching':
        if include_advanced:
            if len(res) > 0:
                res = metrics.add_pitching_metrics(res)
    return res


def ncaa_player_game_logs(player, season, variant, school=None):
    """
    Obtains aggregate player-level game-by-game stats

    Args:
        player: player name (str) or NCAA stats_player_seq (int)
        variant (str): 'batting', 'pitching', or 'fielding'
        season: season as (int, YYYY) or NCAA season_id (int), valid 2013-2022
        school (optional, if not passing a stats_player_seq): school name (str)
            or NCAA school_id (int)

    Returns:
        pd.DataFrame

    data from stats.ncaa.org. valid 2013-2022
    """
    season, season_id, batting_id, pitching_id, fielding_id = lookup_season_info(
        season)
    if school is not None:
        school, school_id = lookup_school_info(school)
    if type(player) == int:
        player_id = player
        try:
            player_name, school, school_id = lookup_player_reverse(
                player_id, season)
        except:
            print('no records found')
            return pd.DataFrame()
    elif type(player) == str:
        player_name = player
        if school is not None:
            player_id = lookup_player_id(player_name, school)
        else:
            return 'must give a player_id if no school given'
    stats_player_seq = str(player_id)
    headers = _lookup_log_headers(season, variant)
    headers.append('stats_player_seq')
    if variant == 'batting':
        year_stat_category_id = batting_id
    elif variant == 'pitching':
        year_stat_category_id = pitching_id
    else:
        year_stat_category_id = fielding_id
    payload = {'game_sport_year_ctl_id': str(season_id),
               'org_id': str(school_id),
               'stats_player_seq': str(stats_player_seq),
               'year_stat_category_id': str(year_stat_category_id)}
    url = 'https://stats.ncaa.org/player/game_by_game?'
    with Session() as s:
        r = s.get(url, params=payload, headers=_HEADERS)
    soup = BeautifulSoup(r.text, features='lxml')
    table = soup.find_all('table')[3]
    rows = []
    prev_game_id = 0
    for val in table.find_all(_has_no_id)[3:]:
        row = []
        for i in val.children:
            if isinstance(i, Tag):
                if i.a:
                    if len(i.find_all('a')) == 1:
                        if 'box_score' in i.a.get('href'):
                            score = i.a.string.strip()
                            game_id = i.a.get('href').split('/')[-2]
                        elif 'team' in i.a.get('href'):
                            opponent_id = i.a.get('href').split('/')[2]
                            opponent_name, field = _parse_opponent_info(
                                i.a.text.strip())
                elif 'data-order' in i.attrs:
                    row.append(i.get('data-order'))
                elif '/' in i.string:
                    date = i.string
                else:
                    opponent_name, field = _parse_opponent_info(i.text.strip())
                    opponent_id = '-'
        runs_scored, runs_allowed, run_diff, result, ip, extras = _parse_score(
            score)
        row.append(date)
        row.append(field)
        row.append(season_id)
        row.append(opponent_id)
        row.append(opponent_name)
        row.append(ip)
        row.append(extras)
        row.append(runs_scored)
        row.append(runs_allowed)
        row.append(run_diff)
        row.append(result)
        row.append(game_id)
        row.append(school_id)
        if len(row) == (len(headers)-1) and prev_game_id != game_id:
            row.append(int(player_id))
            rows.append(row)
    res = pd.DataFrame(rows, columns=headers)
    if not res.empty:
        res.loc[:, 'season'] = season
        res = _transform_stats(res)
    return res


def ncaa_team_game_logs(school, season, variant):
    """
    Obtains aggregate team-level game-by-game stats

    Args:
        school: school name (str) or NCAA school_id (int)
        seasons: seasons as (int, YYYY) or NCAA season_id (list), 2013-2022
        variant (str): 'batting', 'pitching', or 'fielding'

    Returns:
        pd.DataFrame

    data from stats.ncaa.org. valid 2013-2022
    """
    season, season_id, batting_id, pitching_id, fielding_id = lookup_season_info(
        season)
    school, school_id = lookup_school_info(school)
    headers = _lookup_log_headers(season, variant)
    if variant == 'batting':
        year_stat_category_id = batting_id
    elif variant == 'pitching':
        year_stat_category_id = pitching_id
    else:
        year_stat_category_id = fielding_id
    payload = {'game_sport_year_ctl_id': str(season_id),
               'org_id': str(school_id),
               'stats_player_seq': '-100',
               'year_stat_category_id': str(year_stat_category_id)}
    url = 'https://stats.ncaa.org/player/game_by_game?'
    with Session() as s:
        r = s.get(url, params=payload, headers=_HEADERS)
    soup = BeautifulSoup(r.text, features='lxml')
    table = soup.find_all('table')[3]
    rows = []
    prev_game_id = 0
    for val in table.find_all(_has_no_id)[3:]:
        row = []
        for i in val.children:
            if isinstance(i, Tag):
                if i.a:
                    if len(i.find_all('a')) == 1:
                        if 'box_score' in i.a.get('href'):
                            score = i.a.string.strip()
                            game_id = i.a.get('href').split('/')[-2]
                        elif 'team' in i.a.get('href'):
                            opponent_id = i.a.get('href').split('/')[2]
                            opponent_name, field = _parse_opponent_info(
                                i.a.text.strip())
                elif 'data-order' in i.attrs:
                    row.append(i.get('data-order'))
                elif '/' in i.string:
                    date = i.string
                else:
                    opponent_name, field = _parse_opponent_info(i.text.strip())
                    opponent_id = '-'
        runs_scored, runs_allowed, run_diff, result, ip, extras = _parse_score(
            score)
        row.append(date)
        row.append(field)
        row.append(season_id)
        row.append(opponent_id)
        row.append(opponent_name)
        row.append(ip)
        row.append(extras)
        row.append(runs_scored)
        row.append(runs_allowed)
        row.append(run_diff)
        row.append(result)
        row.append(game_id)
        row.append(school_id)
        if (len(row) == len(headers)) and (game_id != prev_game_id):
            prev_game_id = game_id
            rows.append(row)
    res = pd.DataFrame(rows, columns=headers)
    if not res.empty:
        res.loc[:, 'season'] = season
        res = _transform_stats(res)
    return res


def ncaa_team_results(school, season):
    """
    Obtains the results of completed games for a given school/season

    Retrieves data of completed games for a given team from stats.ncaa.org
    Args:
        school: school name (str) or NCAA school_id (int)
        season: season (int, YYYY) or NCAA season_id (int), valid 2013-2022
    Returns:
        pd.DataFrame

    data from stats.ncaa.org. valid 2013-2022
    """
    data = ncaa_team_game_logs(school=school, season=season, variant='batting')
    res = data[['game_id', 'date', 'field', 'opponent_name', 'opponent_id',
                'innings_played', 'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'school_id', 'season_id']]
    return res


def ncaa_team_season_roster(school, season):
    """
    Retrieves the single-season roster for a given school/season

    Args:
        school: school name (str) or NCAA school_id (int)
        season: season as (int, YYYY) or NCAA season_id (int), valid 2012-2022

    Returns:
        pd.DataFrame

    data from stats.ncaa.org, valid for 2013-2022.
    """
    ncaa_name, school_id = lookup_school_info(school)
    season, season_id, batting_id, pitching_id, fielding_id = lookup_season_info(
        season)
    request_body = 'https://stats.ncaa.org/team/'
    request_body += f'''{str(school_id)}/roster/{str(season_id)}'''
    with Session() as s:
        r = s.get(request_body, headers=_HEADERS)
    soup = BeautifulSoup(r.text, features='lxml')
    res = []
    if (season in [2019, 14781, 2022, 15860]):
        num_values = 7
        col_names = ['jersey', 'stats_player_seq', 'name', 'position',
                     'height', 'class_year', 'games_played',
                     'games_started']
    else:
        num_values = 6
        col_names = ['jersey', 'stats_player_seq', 'name', 'position',
                     'class_year', 'games_played', 'games_started']
    for index, value in enumerate(soup.find_all('td')):
        if index % num_values == 0:
            details = []
            res.append(details)
        if index % num_values == 1:
            try:
                details.append((value.contents[0].get('href')[-7:]))
                details.append(value.contents[0].string)
            except:
                details.append(None)
                details.append(value.contents[0])
        else:
            try:
                details.append(value.contents[0])
            except:
                details.append(None)
    df = pd.DataFrame(res)
    df.columns = col_names
    df.stats_player_seq = df.stats_player_seq.astype('str')
    df.stats_player_seq = df.stats_player_seq.str.replace('=', '')
    df = df.loc[df.stats_player_seq != 'None']
    df.stats_player_seq = df.stats_player_seq.astype('int64')
    df['season'] = season
    df['season_id'] = season_id
    df['school'] = ncaa_name
    df['school_id'] = school_id
    df['school_id'] = df['school_id'].astype('int32')
    df['season'] = df['season'].astype('int16')
    df['season_id'] = df['season_id'].astype('int32')
    df.name = df.name.apply(_format_names)
    return df


def ncaa_team_roster(school, seasons):
    """
    Retrieves a blindly concattenated roster across multiple seasons

    Args:
        school/school_id (str or int): name of school or school_id
        seasons (list of ints): list of season as (int, YYYY)
         or NCAA season_id (int), valid 2013-2022

    Returns:
        pd.DataFrame

    data from stats.ncaa.org. valid 2013-2022
    """
    if len(seasons) == 1:
        return ncaa_team_season_roster(school, seasons[0])
    else:
        roster = pd.DataFrame()
        for season in set(seasons):
            sleep(random.uniform(0, _TIMEOUT))
            try:
                new = ncaa_team_season_roster(school, season)
                if 'height' in new.columns:
                    new = new.drop(columns=['height'], inplace=False)
                roster = pd.concat([roster, new])
                if 'Unnamed: 0' in roster.columns:
                    roster = roster.drop(columns=['Unnamed: 0'], inplace=False)
            except:
                continue
    return roster


def ncaa_career_stats(stats_player_seq, variant):
    """
    Transmits GET request to stats.ncaa.org, parses career stats

    Args:
        stats_player_seq (int): the NCAA player_id
        variant (str): 'batting', 'pitching', or 'fielding'

    Returns:
        pd.DataFrame

    data from stats.ncaa.org. valid 2013-2022
    """
    season = lookup_seasons_played(stats_player_seq)[0]
    season, season_id, batting_id, pitching_id, fielding_id = lookup_season_info(
        season)
    if variant == 'batting':
        year_stat_category_id = batting_id
    elif variant == 'pitching':
        year_stat_category_id = pitching_id
    else:
        year_stat_category_id = fielding_id
    payload = {'id': str(season_id), 'stats_player_seq': str(stats_player_seq),
               'year_stat_category_id': str(year_stat_category_id)}
    url = 'https://stats.ncaa.org/player/index'
    with Session() as s:
        r = s.get(url, params=payload, headers=_HEADERS)
    if r.status_code == 403:
        print('403 Error: NCAA blocked request')
        return pd.DataFrame()
    try:
        soup = BeautifulSoup(r.text, features='lxml')
        table = soup.find_all('table')[2]
        headers = []
        for val in table.find_all('th'):
            headers.append(val.string.strip())
        rows = []
        row = []
        for val in table.find_all('td'):
            if 'data-order' in val.attrs:
                row.append(val['data-order'])
            elif val.a is not None:
                row.append(val.a.attrs['href'].split('/')[2])
            elif val.string.strip() != 'Career' and 'width' not in val.attrs:
                if row != []:
                    rows.append(row)
                row = []
                row.append(val.string.strip())
            else:
                if val.string.strip() != 'Career':
                    row.append(val.string.strip())
        df = pd.DataFrame(rows)
        df.columns = headers
        df = _transform_stats(df)
        return df
    except:
        print('no records found')
        return pd.DataFrame()


def lookup_season_ids(season):
    """
    A function that finds the year_stat_category_ids of a given season
    Args:
        season (int, YYYY)
    Returns:
        tuple of (season_id, batting_id, pitching_id) for desired season
    """
    df = pd.read_parquet(datasets.get_season_lu_path())
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
    df = pd.read_parquet(datasets.get_season_lu_path())
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
    df = pd.read_parquet(datasets.get_season_lu_path())
    season_row = df.loc[df['season'] == season]
    season_id = season_row.values[0][1]
    return int(season_id)


def lookup_seasons_played(stats_player_seq):
    """
    A function to find the final and debut seasons of a given player
    Args:
        stats_player_seq (int): NCAA player_id
    Returns:
        tuple of ints: (debut season, most recent season)
    """
    df = pd.read_csv(datasets.get_players_history_path())
    row = df.loc[df.stats_player_seq == stats_player_seq]
    return int(row['debut_season'].values[0]), int(row['season_last'].values[0])


def lookup_school_id(school):
    """
    A function to find a school's id from it's name
    Args:
        school (str): the name of the school
    Returns:
        school_id as int
    Examples:
        lookup_school_id("cornell")
        >>> 167
    """
    df = pd.read_parquet(datasets.get_school_path())
    school_row = df.loc[(df.ncaa_name == school)]
    if len(school_row) == 0:
        school_row = df.loc[(df.bd_name == school)]
    if len(school_row) == 0:
        return f'''could not find school {school}'''
    else:
        return int(school_row['school_id'].values[0])


def lookup_school_reverse(school_id):
    """
    A function to find a school's name from a school_id
    Args:
        school_id as int
    Returns:
        school (str): the name of the school
    Examples:
        lookup_school_reverse(167)
        >>> "Cornell"
    """
    df = pd.read_parquet(datasets.get_school_path())
    school_row = df.loc[(
        df.school_id == school_id)]
    if len(school_row) == 0:
        return f'''could not find school {school_id}'''
    else:
        return str(school_row['ncaa_name'].values[0])


def lookup_player_id(player_name, school):
    """
    A function to find a player's id from their name and school

    Args:
        player_name (str): name of player to lookup
        school: the name of the player's school (str) or school_id (int)

    Returns:
        The player_id of the player (int)

    Examples:
        lookup_player_id("Jake Gelof", "Virginia")
        >>> 2486499
    """
    df = pd.read_parquet(datasets.get_player_id_lu_path())
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
        player_name (str): name of player to lookup
        school: either the ncaa_name of the school (str) of the player

    Returns:
        The name and school of the player as a strings

    Examples:
        lookup_player_id("Jake Gelof", "Virginia")
        >>> 2486499

    """
    df = pd.read_parquet(datasets.get_rosters_path())
    row = df.loc[(df.stats_player_seq == player_id)
                 & (df.season == season)]
    if len(row) == 0:
        return f'''could not find player {player_id}'''
    else:
        return str(row['name'].values[0]), str(row['school'].values[0]), int(row['school_id'].values[0])


def lookup_school_info(x):
    """
    a function to handle the school/school_id input types

    """
    if type(x) is int:
        school_id = x
        ncaa_name = lookup_school_reverse(school_id)
    elif type(x) is str:
        school_id = lookup_school_id(x)
        ncaa_name = x
    return str(ncaa_name), int(school_id)


def lookup_season_info(x):
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
    # need to do this after name formatting, which relies on commas
    df = df.replace(',', '', regex=True)
    df.fillna(value=0.00, inplace=True)
    # set dtypes
    data_types = {
        'int32': ['opponent_id', 'season_id', 'school_id', 'innings_played',
                  'game_id', 'runs_scored', 'runs_allowed', 'run_difference',
                  'season', 'GP', 'GS', 'BB', 'Jersey', 'DP', 'H', 'DP' 'R',
                  'ER', 'SO', 'TB', '2B', '3B', 'HR', 'RBI', 'R', 'AB', 'HBP',
                  'SF', 'K', 'SH', 'Picked', 'SB', 'IBB', 'CS', 'OPP DP',
                  'SHO', 'BF', 'P-OAB', '3B-A', '2B-A', 'Bk', 'HR-A', 'WP',
                  'IBB', 'Inh Run', 'Inh Run Score', 'SHA', 'SFA', 'GO',
                  'FO', 'W', 'L', 'HB', 'SV', 'KL', 'pickoffs', 'OrdAppeared',
                  'App', 'season', 'GDP', 'PO', 'A', 'TC', 'E', 'CI', 'PB',
                  'SBA', 'CSB', 'IDP', 'TP'],
        'bool': ['extras'],
        'float': ['ERA', 'IP'],
        'string': ['Yr', 'Pos', 'date', 'Year']
    }
    for i in data_types.keys():
        for j in data_types[i]:
            if j in cols:
                df[j] = df[j].astype(i)
    # we're going to calculate OBP/BA/SLG ourselves
    # G and RBI2out are unreliable
    drops = ['OBPct', 'BA', 'SlgPct', 'RBI2out', 'G']
    for i in drops:
        if i in cols:
            df.drop(columns=[i], inplace=True)
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
    if 'Team' in cols:
        df = df.rename(columns={'Team': 'school_id'}, inplace=False)
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
    return df


def _has_no_id(tag):
    return tag.name == 'tr' and not tag.has_attr('id')


def _parse_score(score):
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


def _lookup_log_headers(season, variant):
    """
    handling season/season_id input types for game log functions
    n.b. due to inconsistencies in NCAA formatting and bad code
    (e.g. ghost columns), we are hard-coding these so things don't
    get too messy
    """
    if variant == 'batting':
        batting_headers_dict = {
            2022: ['G', 'R', 'AB', 'H', '2B', '3B', 'TB', 'HR',
                   'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'OPP DP',
                   'CS', 'Picked', 'SB', 'IBB',
                   'GDP', 'RBI2out', 'date', 'field', 'season_id',
                   'opponent_id', 'opponent_name', 'innings_played',
                   'extras', 'runs_scored', 'runs_allowed',
                   'run_difference', 'result',  'game_id',
                   'school_id'],
            2021: ['G', 'R', 'AB', 'H', '2B', '3B',
                   'TB', 'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'OPP DP',
                   'CS', 'Picked', 'SB', 'IBB', 'RBI2out', 'date', 'field'
                   'season_id', 'opponent_id', 'opponent_name',
                   'innings_played', 'extras', 'runs_scored', 'runs_allowed',
                   'run_difference', 'result',  'game_id',
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
                   'result',  'game_id', 'school_id'],
            2013: ['AB', 'H', 'TB', 'R', '2B', '3B',
                   'HR', 'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP', 'SB',
                   'CS', 'Picked', 'IBB', 'date', 'field', 'season_id',
                   'opponent_id', 'opponent_name', 'innings_played',
                   'extras', 'runs_scored', 'runs_allowed', 'run_difference',
                   'result',  'game_id', 'school_id'],
            2012: ['AB', 'R', 'H', '2B', '3B', 'HR',
                   'RBI', 'BB', 'HBP', 'SF', 'SH', 'K', 'DP', 'SB', 'CS',
                   'Picked', 'IBB', 'date', 'field', 'season_id',
                   'opponent_id', 'opponent_name', 'innings_played',
                   'extras', 'runs_scored', 'runs_allowed', 'run_difference',
                   'result',  'game_id', 'school_id']}
        headers = batting_headers_dict[season]
    elif variant == 'pitching':
        pitching_headers_dict = {
            2022: ['G', 'App', 'GS', 'IP', 'CG', 'H',
                   'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A',
                   '3B-A', 'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run',
                   'Inh Run Score', 'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W',
                   'L', 'SV', 'OrdAppeared', 'KL', 'pickoffs', 'date',
                   'field', 'season_id', 'opponent_id', 'opponent_name',
                   'innings_played', 'extras', 'runs_scored', 'runs_allowed',
                   'run_difference', 'result',  'game_id',
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
                   'result',  'game_id', 'school_id'],
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
                   'result',  'game_id', 'school_id'],
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
                   'run_difference', 'result',  'game_id',
                   'school_id'],
            2013: ['App', 'GS', 'IP', 'H',
                   'R', 'ER', 'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A',
                   '3B-A', 'Bk', 'HR-A', 'WP', 'HB', 'IBB', 'Inh Run',
                   'Inh Run Score', 'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W',
                   'L', 'SV', 'OrdAppeared', 'KL', 'date', 'field',
                   'season_id', 'opponent_id', 'opponent_name',
                   'innings_played', 'extras', 'runs_scored', 'runs_allowed',
                   'run_difference', 'result',  'game_id',
                   'school_id'],
            2012: ['App', 'GS', 'IP', 'H', 'R', 'ER',
                   'BB', 'SO', 'SHO', 'BF', 'P-OAB', '2B-A', '3B-A', 'Bk',
                   'HR-A', 'WP', 'HB', 'IBB', 'Inh Run', 'Inh Run Score',
                   'SHA', 'SFA', 'Pitches', 'GO', 'FO', 'W', 'L', 'SV',
                   'OrdAppeared', 'KL', 'date', 'field', 'season_id',
                   'opponent_id', 'opponent_name', 'innings_played'
                   'extras', 'runs_scored', 'runs_allowed',
                   'run_difference', 'result',  'game_id',
                   'school_id'],
        }
        headers = pitching_headers_dict[season]
    else:
        fielding_headers_dict = {
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
                   'result',  'game_id', 'school_id'],
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
                   'game_id', 'school_id']}
        headers = fielding_headers_dict[season]
    return headers


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
