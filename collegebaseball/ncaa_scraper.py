"""
ncaa_scraper

A module to scrape and parse college baseball statistics from stats.ncaa.org

created by Nathan Blumenfeld in Spring 2022
"""
import pandas as pd
from time import sleep
import random
from bs4 import BeautifulSoup, Tag
from collegebaseball import metrics, ncaa_utils, lookup
from requests import Session


# GET request options
_HEADERS = {'User-Agent': 'Mozilla/5.0'}
_TIMEOUT = 4


def ncaa_team_stats(school, season, variant, include_advanced=True,
                    split=None):
    """
    Obtains player-level single-season aggregate stats
     for all players from a given school, from stats.ncaa.org

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
    """
    season, season_id, batting_id, pitching_id, fielding_id = lookup._lookup_season_info(
        season)
    school, school_id, division = lookup._lookup_school_info(school)
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
        available_stat_id = ncaa_utils.available_stat_ids[variant][season][split]
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
    df['division'] = division
    res = ncaa_utils._transform_stats(df)
    if variant == 'batting':
        if include_advanced:
            if len(res) > 0:
                res = metrics.add_batting_metrics(res)
                res = res.loc[res.PA > 0]
    elif variant == 'pitching':
        if split is None:
            if 'App' in res.columns:
                res = res.loc[res.App > 0]
        if include_advanced:
            if len(res) > 0:
                res = metrics.add_pitching_metrics(res)
    return res


def ncaa_career_stats(stats_player_seq, variant, include_advanced=True):
    """
    Obtains season-aggregate stats for all seasons in a given player's
     collegiate career, from stats.ncaa.org 

    Args:
        stats_player_seq (int): the NCAA player_id
        variant (str): 'batting', 'pitching', or 'fielding'
        include_advanced (bool, optional). Whether to
         automatically calcuate advanced metrics, Defaults to True

    Returns:
        pd.DataFrame
    """
    season = lookup.lookup_seasons_played(stats_player_seq)[0]
    season, season_id, batting_id, pitching_id, fielding_id = lookup._lookup_season_info(
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
        elif val.text.strip() != 'Career' and 'width' not in val.attrs:
            if row != []:
                rows.append(row)
            row = []
            row.append(val.text.strip())
        else:
            if val.text.strip() != 'Career':
                row.append(val.text.strip())
    df = pd.DataFrame(rows)
    df.columns = headers
    df = ncaa_utils._transform_stats(df)
    school, school_id, division = lookup._lookup_school_info(
        int(df['school_id'].values[0]))
    df['division'] = division
    df['school'] = school
    df['division'] = df['division'].astype('int8')
    df['school'] = df['school'].astype('string')
    if variant == 'batting':
        if include_advanced:
            if len(df) > 0:
                df = metrics.add_batting_metrics(df)
                df = df.loc[df.PA > 0]
    elif variant == 'pitching':
        if include_advanced:
            if len(df) > 0:
                df = metrics.add_pitching_metrics(df)
    return df


def ncaa_team_totals(school, season, variant, include_advanced=True,
                     split=None):
    """
    Obtains team-level aggregate single-season stats for a given team, 
     from stats.ncaa.org

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
    """
    school, school_id, division = lookup._lookup_school_info(school)
    season, season_id, batting_id, pitching_id, fielding_id = lookup._lookup_season_info(
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
        available_stat_id = ncaa_utils.available_stat_ids[variant][season][split]
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
    df['division'] = division
    res = ncaa_utils._transform_stats(df)
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


def ncaa_player_game_logs(player, season, variant, school=None, include_advanced=True):
    """
    Obtains player-level game-by-game stats for a given player in 
     a given season, from stats.ncaa.org

    Args:
        player: player name (str) or NCAA stats_player_seq (int)
        variant (str): 'batting', 'pitching', or 'fielding'
        season: season as (int, YYYY) or NCAA season_id (int), valid 2013-2022
        school (optional, if not passing a stats_player_seq): school name (str)
            or NCAA school_id (int)

    Returns:
        pd.DataFrame
    """
    season, season_id, batting_id, pitching_id, fielding_id = lookup._lookup_season_info(
        season)
    if type(player) == int:
        player_id = player
        try:
            player_name, school, school_id = lookup.lookup_player_reverse(
                player_id, season)
            school, school_id, division = lookup._lookup_school_info(school)
        except:
            print('no records found')
            return pd.DataFrame()
    elif type(player) == str:
        player_name = player
        if school is not None:
            player_id = lookup.lookup_player(player_name, school)
            school, school_id, division = lookup._lookup_school_info(school)
        else:
            return 'must give a player_id if no school given'
    stats_player_seq = str(player_id)
    headers = ncaa_utils.player_gamelog_headers[variant][season]
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
    if table is None:
        print('no data found')
        return pd.DataFrame()
    rows = []
    prev_game_id = 0
    for val in table.find_all(ncaa_utils._has_no_id)[3:]:
        row = []
        for i in val.children:
            if isinstance(i, Tag):
                if i.a:
                    if len(i.find_all('a')) >= 1:
                        if 'box_score' in i.find_all('a')[-1].get('href'):
                            score = i.a.string.strip()
                            game_id = i.a.get('href').split('/')[-2]
                        elif 'team' in (i.find_all('a'))[-1].get('href'):
                            opponent_id = i.find_all(
                                'a')[-1].get('href').split('/')[2]
                            opponent_name, field = ncaa_utils._parse_opponent_info(
                                i.find_all('a')[-1].text.strip())
                        elif 'game/index' in i.find_all('a')[-1].get('href'):
                            game_id = i.find_all(
                                'a')[-1].get('href').split('/')[-1].split('?')[0]
                            score = i.a.string.strip()
                        else:
                            opponent_name, field = ncaa_utils._parse_opponent_info(
                                i.text.strip())
                            opponent_id = '-'
                elif 'data-order' in i.attrs:
                    row.append(i.get('data-order'))
                elif '/' in i.string:
                    date = i.string
                elif i.text.strip() == '-':
                    score = '-'
                    game_id = '-'
                else:
                    opponent_name, field = ncaa_utils._parse_opponent_info(
                        i.text.strip())
                    opponent_id = '-'
        runs_scored, runs_allowed, run_diff, result, ip, extras = ncaa_utils._parse_score(
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
        if len(row) == (len(headers)-1) and ((game_id != prev_game_id) or (result == 'cancelled')):
            prev_game_id = game_id
            row.append(int(player_id))
            rows.append(row)
    res = pd.DataFrame(rows, columns=headers)
    if not res.empty:
        res['season'] = season
        res['division'] = division
        res = res.loc[res.field.isin(['away', 'home', 'neutral'])]
        res = ncaa_utils._transform_stats(res)
    if variant == 'batting':
        if include_advanced:
            if len(res) > 0:
                res = metrics.add_batting_metrics(res)
                res = res.loc[res.PA > 0]
    elif variant == 'pitching':
        if include_advanced:
            if len(res) > 0:
                res = res.loc[res.IP > 0]
                res = metrics.add_pitching_metrics(res)
    return res


def ncaa_team_game_logs(school, season, variant, include_advanced=True):
    """
    Obtains team-level game-by-game stats for a given team in a given 
     season, from stats.ncaa.org

    Args:
        school: school name (str) or NCAA school_id (int)
        seasons: seasons as (int, YYYY) or NCAA season_id (list), 2013-2022
        variant (str): 'batting', 'pitching', or 'fielding'
        include_advanced (bool, optional). Whether to
         automatically calcuate advanced metrics, Defaults to True

    Returns:
        pd.DataFrame
    """
    season, season_id, batting_id, pitching_id, fielding_id = lookup._lookup_season_info(
        season)
    school, school_id, division = lookup._lookup_school_info(school)
    headers = ncaa_utils.team_gamelog_headers[variant][season]
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
    for val in table.find_all(ncaa_utils._has_no_id)[3:]:
        row = []
        for i in val.children:
            if isinstance(i, Tag):
                if i.a:
                    if len(i.find_all('a')) >= 1:
                        if 'box_score' in i.find_all('a')[-1].get('href'):
                            score = i.a.string.strip()
                            game_id = i.a.get('href').split('/')[-2]
                        elif 'team' in (i.find_all('a'))[-1].get('href'):
                            opponent_id = i.find_all(
                                'a')[-1].get('href').split('/')[2]
                            opponent_name, field = ncaa_utils._parse_opponent_info(
                                i.find_all('a')[-1].text.strip())
                        elif 'game/index' in i.find_all('a')[-1].get('href'):
                            game_id = i.find_all(
                                'a')[-1].get('href').split('/')[-1].split('?')[0]
                            score = i.a.string.strip()
                        else:
                            opponent_name, field = ncaa_utils._parse_opponent_info(
                                i.text.strip())
                            opponent_id = '-'
                elif 'data-order' in i.attrs:
                    row.append(i.get('data-order'))
                elif '/' in i.string:
                    date = i.string
                elif i.text.strip() == '-':
                    score = '-'
                    game_id = '-'
                else:
                    opponent_name, field = ncaa_utils._parse_opponent_info(
                        i.text.strip())
                    opponent_id = '-'
        runs_scored, runs_allowed, run_diff, result, ip, extras = ncaa_utils._parse_score(
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
        if (len(row) == len(headers)) and ((game_id != prev_game_id) or (result == 'cancelled')):
            prev_game_id = game_id
            rows.append(row)
    res = pd.DataFrame(rows, columns=headers)
    if not res.empty:
        res['season'] = season
        res['division'] = division
        res = res.loc[res.field.isin(['away', 'home', 'neutral'])]
        res = ncaa_utils._transform_stats(res)
    if variant == 'batting':
        if include_advanced:
            if len(res) > 0:
                res = metrics.add_batting_metrics(res)
                res = res.loc[res.PA > 0]
    elif variant == 'pitching':
        if include_advanced:
            if len(res) > 0:
                res = res.loc[res.IP > 0]
                res = metrics.add_pitching_metrics(res)
    return res


def ncaa_team_results(school, season):
    """
    Obtains the results of games for a given school in a given 
     season, from stats.ncaa.org

    Args:
        school: school name (str) or NCAA school_id (int)
        season: season (int, YYYY) or NCAA season_id (int), valid 2013-2022

    Returns:
        pd.DataFrame
    """
    data = ncaa_team_game_logs(
        school, season, variant='batting', include_advanced=False)
    res = data[['game_id', 'date', 'field', 'opponent_name', 'opponent_id',
                'innings_played', 'extras', 'runs_scored', 'runs_allowed',
               'run_difference', 'result', 'school_id', 'season_id', 'division']]
    return res


def ncaa_team_season_roster(school, season):
    """
    Retrieves the single-season roster for a given school in a 
     given season, from stats.ncaa.org

    Args:
        school: school name (str) or NCAA school_id (int)
        season: season as (int, YYYY) or NCAA season_id (int), valid 2012-2022

    Returns:
        pd.DataFrame
    """
    school, school_id, division = lookup._lookup_school_info(school)
    season, season_id = lookup._lookup_season_basic(season)
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
    df['school'] = school
    df['school_id'] = school_id
    df['division'] = division
    df['school'] = df['school'].astype('string')
    df['school_id'] = df['school_id'].astype('int32')
    df['season'] = df['season'].astype('int16')
    df['division'] = df['division'].astype('int16')
    df['season_id'] = df['season_id'].astype('int32')
    df.name = df.name.apply(ncaa_utils._format_names)
    return df


def ncaa_team_roster(school, seasons):
    """
    Retrieves a blindly concattenated roster for a given tea
     across the given seasons, from stats.ncaa.org

    Args:
        school/school_id (str or int): name of school or school_id
        seasons (list of ints): list of season as (int, YYYY)
         or NCAA season_id (int), valid 2013-2022

    Returns:
        pd.DataFrame
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
