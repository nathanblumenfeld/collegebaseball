from importlib import resources

def get_school_table():
    """

    """
    with resources.path("collegebaseball.data", "schools.parquet") as f:
        data_file_path = f
    return data_file_path

def get_player_id_lu_table():
    """

    """
    with resources.path("collegebaseball.data", "player_id_lookup.parquet") as f:
        data_file_path = f
    return data_file_path

def get_season_lu_table():
    """

    """
    with resources.path("collegebaseball.data", "seasons.parquet") as f:
        data_file_path = f
    return data_file_path


def get_players_history_table():
    """

    """
    with resources.path("collegebaseball.data", "players_history.parquet") as f:
        data_file_path = f
    return data_file_path

def get_linear_weights_table():
    """

    """
    with resources.path("collegebaseball.data", "d1_linear_weights.parquet") as f:
        data_file_path = f
    return data_file_path

