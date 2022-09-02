Game-by-Game Stats
==================

Player-level Game Logs
----------------------
.. py:function:: ncaa_scraper.ncaa_player_game_logs(player, season, variant, school=None, include_advanced=True):
   
   Obtains player-level game-by-game stats for a given player in a given season, from stats.ncaa.org 

   :player: ncaa_name (str) or stats_player_seq (int)
   :season: season (int, YYYY) or NCAA season_id (int), valid 2013-2022
   :variant (str): 'batting', 'pitching', or 'fielding'
   :school (bool, optional): if a player name is not given to player argument,
    the name of player's most recent school
   :include_advanced (bool, optional): whether to
      automatically calcuate advanced metrics, Defaults to True
   :return (pd.DataFrame):


Team-level Game Logs
----------------------
.. py:function:: ncaa_scraper.ncaa_team_game_logs(school, season, variant, include_advanced=True):
   
   Obtains team-level game-by-game stats for a given team in a given season, from stats.ncaa.org

   :school: school name (str) or NCAA school_id (int)
   :season: season as (int, YYYY) or NCAA season_id (list), 2013-2022
   :variant (str): 'batting', 'pitching', or 'fielding'
   :include_advanced (bool, optional): whether to
      automatically calcuate advanced metrics, Defaults to True
   :return (pd.DataFrame):
