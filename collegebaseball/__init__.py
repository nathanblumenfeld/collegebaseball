from .ncaa_scraper import ncaa_team_season_roster, ncaa_team_roster, \
    ncaa_career_stats, ncaa_team_stats, ncaa_team_totals, \
    ncaa_team_game_logs, ncaa_player_game_logs, ncaa_team_results
from .lookup import lookup_season_ids, lookup_season_reverse, \
    lookup_season_id, lookup_seasons_played, lookup_school, \
    lookup_player, _lookup_season_info, _lookup_school_info, \
    _lookup_season_basic, lookup_season_id_reverse, \
    lookup_player_reverse, lookup_school_reverse
from .metrics import calculate_woba_manual, calculate_wraa_manual, calculate_wrc_manual, \
    add_batting_metrics, add_pitching_metrics
from .boydsworld_scraper import boydsworld_team_results
from .win_pct import calculate_actual_win_pct, calculate_pythagenpat_win_pct
from .guts import get_player_lu_path, get_player_lu_table, \
    get_linear_weights_path, get_linear_weights_table, \
    get_players_history_path, get_players_history_table, \
    get_schools_path, get_schools_table, \
    get_seasons_path, get_seasons_table, \
    get_rosters_path, get_rosters_table, \
    get_season_linear_weights
from .download_utils import download_rosters, \
    download_player_game_logs, download_season_rosters, \
    download_team_results, download_team_stats, \
    download_team_totals

import sys
import warnings

if not sys.warnoptions:
    warnings.simplefilter("ignore")

__version__ = '1.3.0-alpha'
__author__ = 'Nathan Blumenfeld'
