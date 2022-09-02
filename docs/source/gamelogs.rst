==================
Game-by-Game Stats
==================

Player-level Game Logs
----------------------
.. py:function:: ncaa_scraper.ncaa_player_game_logs(player, season, variant, school=None)
   
   Obtains aggregate player-level game-by-game stats from stats.ncaa.org
  
   :player: ncaa_name (str) or school_id (int)
   :variant: season (int, YYYY) or NCAA season_id (int), valid 2013-2022
   :season (str): 'batting', 'pitching', or 'fielding'
   :school (bool, optional): whether 
   :return (pd.DataFrame):


Team-level Game Logs
----------------------
.. py:function:: ncaa_scraper.ncaa_team_game_logs(school, season, variant):
   
   Obtains aggregate team-level game-by-game stats from stats.ncaa.org

   :school: school name (str) or NCAA school_id (int)
   :season: season as (int, YYYY) or NCAA season_id (list), 2013-2022
   :variant (str): 'batting', 'pitching', or 'fielding'
   :returns (pd.DataFrame):
