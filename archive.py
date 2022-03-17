def get_team_records(school_id, start, end, show_progress = True, print_interval = 10, request_timeout = 0.1):
#     """
#     Returns a table of all career season records of players who played for a given team within start, end interval
    
#     Inputs
#     ----
#     conference (str)
#     show_progress (bool): If True, prints progress of function call
#     (default: True)
#     print_interval (int): If show_progress, the interval between progress updates
#     outputs 
#     request_timeout (float): length in seconds between successive calls to database
    
#     Outputs
#     -----
#     DataFrame 
#     """
#     # getting roster
#     roster = get_multiyear_roster(school_id, start, end, request_timeout)
#     df = roster.groupby(by='stats_player_seq').agg('first').reset_index()
#     print('roster acquired')
#     num_calls = len(df)
    
#     res = pd.DataFrame()
#     for index, row in df.iterrows():
#         try:
#             new = get_career_stats(stats_player_seq = row['stats_player_seq'], season_id = row['season_id'], school_id = row['school_id'])
#             new['school_id'] = row['school_id']
#             new['stats_player_seq'] = row['stats_player_seq']
#             new['name'] = row['name']
#         except:
#             print('failure: '+str(row['name'])+' ('+ str(row['stats_player_seq'])+') | season: '+str(row['season'])+' ('+str(row['season_id'])+') | school_id: '+' ('+str(row['school_id'])+')')
#             new = pd.DataFrame()
#             new['school_id'] = row['school_id']
#             new['stats_player_seq'] = row['stats_player_seq']
#             new['name'] = row['name']
#         res = pd.concat([res, new])
#         time.sleep(random.uniform(0, request_timeout))
#     return res.reset_index()
    pass 

def get_career_stats_old(stats_player_seq, season_id, school_id, headers = {'User-Agent':'Mozilla/5.0'}):
#     """
#     Transmits GET request to stats.ncaa.org, parses career stats  into DataFrame

#     Inputs
#     -----
#     player_id (int)
#     season_id (int)
#     team_id (int)
#     headers (dict): to include with GET request. NCAA is not kind to robots
#     (default: {'User-Agent':'Mozilla/5.0'})


#     Outputs
#     -----
#     DataFrame indexed by stats_seq_id, season_id
    
#     https://stats.ncaa.org/player/index?id=15204&org_id=746&stats_player_seq=2306451&year_stat_category_id=14761
    
#     """
#     # craft GET request to NCAA site    
#     payload = {'game_sport_year_ctl_id':str(season_id), 'stats_player_seq':str(stats_player_seq), 'org_id':str(school_id)}
#     url = 'https://stats.ncaa.org/player/game_by_game'
#     # send request
#     try:
#         r = requests.get(url, params = payload, headers = headers)
#     except:
#         print('An error occurred with the GET Request')
#         if r.status_code == 403:
#             print('403 Error: NCAA blocked request')
#         return pd.DataFrame()
    
#     try: 
#         # parse data
#         soup = BeautifulSoup(r.text, features = 'lxml')
#         table = soup.find_all('table')[2]

#         # get table headers
#         headers = []
#         for val in table.find_all('th'):
#             headers.append(val.string.strip())

#         # get table data
#         rows = []
#         row = []
#         for val in table.find_all('td'): # TODO: cleanup as much as possible
#             # data is also encoded in data-order attr of td elements
#             if 'data-order' in val.attrs:
#                 row.append(val['data-order'])
#             elif val.a is not None:
#                 row.append(val.a.attrs['href'].split('/')[2])
#             elif val.string.strip() != 'Career' and 'width' not in val.attrs:
#                 if row != []:
#                     rows.append(row)
#                 row = []
#                 row.append(val.string.strip())
#             else:
#                 if val.string.strip() != 'Career':
#                     row.append(val.string.strip())

#         # Turn into DataFrame
#         df = pd.DataFrame(rows)
#         df.columns = headers
#         df = transform_career_stats(df)
#         return df
#     except: 
#         print('no records found')
#         return pd.DataFrame()
    pass

def calculate_woba(player_row, weights_df=WEIGHTS_DF, round_to = ROUND_TO):
#     """
#     Returns (float): the weighted on base average of a given player based on the following formula: 

#     wOBA = (wBB×uBB + wHBP×HBP + w1B×1B + w2B×2B + w3B×3B +
#     wHR×HR) / PA
#     """
#     # get linear weights for given season
#     season = player_row['season']
#     print(season)
#     weights_row = weights_df.loc[weights_df['season'] == season]
#     # to avoid div by zero
#     if not player_row['PA'] == 0:
#         woba = ((weights_row['wBB'] * player_row['BB']
#                 + weights_row['wHBP'] * player_row['HBP']
#                 + weights_row['w1B'] * player_row['1B']
#                 + weights_row['w2B'] * player_row['2B']
#                 + weights_row['w3B'] * player_row['3B']
#                 + weights_row['wHR'] * player_row['HR'])
#                 / player_row['PA'])
#         try:
#             woba = woba.iloc[0]
#         except:
#             woba = 0.00
#     else: 
#         woba = 0.00
#     return round(woba, round_to)
    pass 

def calculate_wraa(player_row, weights_df=WEIGHTS_DF, round_to = ROUND_TO):
#     """
#     Returns (float): the weighted runs created above average of a given player based on the following formula: 

#     wRAA = [(wOBA − leagueWOBA) / wOBAscale] ∗ PA
#     """
#     # get linear weights for given season
#     weights_row = weights_df.loc[weights_df.season == player_row['season']]
#     if not player_row['PA'] == 0:
#         wraa = (((player_row['wOBA'] - weights_row['wOBA']) / weights_row['wOBAScale']) * player_row['PA'])
#         try:
#             wraa = wraa.iloc[0]
#         except: 
#             wraa = 0.00
#     else:
#         wraa = 0.00
#     return round(wraa, round_to)
    pass 

def calculate_wrc(player_row, weights_df=WEIGHTS_DF, round_to = ROUND_TO):
#     """
#     Returns (float): the weighted runs created of a given player based on the following formula: 

#     wRC = [((wOBA - lgwOBA) / wOBAScale) + (lgR / PA))] * PA
#     """
#     weights_row = weights_df.loc[weights_df.season == player_row['season']]
#     if not player_row['PA'] == 0:
#         wrc = (((player_row['wOBA'] - weights_row['wOBA']) / weights_row['wOBAScale']) + weights_row['R/PA']) * player_row['PA']
#         try:
#             wrc = wrc.iloc[0]
#         except:
#             wrc = 0.00
#     else: 
#         wrc = 0.00
#     return round(wrc, round_to)
    pass 

def calculate_pa_manual(AB, BB, SF, SH, HBP, IBB):
#     """
#     Returns (int): the number of plate appearances by a given player based on the following formula: 
    
#     PA = AB + BB + SF + SH + HBP - IBB
#     """    
#     PA = player_row['AB'] + player_row['BB'] + player_row['SF'] + player_row['SH'] + player_row['HBP'] - player_row['IBB']
#     return PA      
    pass