from importlib import resources


def get_school_path():
    """

    """
    with resources.path("collegebaseball.data",
                        "schools.parquet") as f:
        data_file_path = f
    return data_file_path


def get_player_id_lu_path():
    """

    """
    with resources.path("collegebaseball.data",
                        "player_id_lookup.parquet") as f:
        data_file_path = f
    return data_file_path


def get_season_lu_path():
    """

    """
    with resources.path("collegebaseball.data",
                        "seasons.parquet") as f:
        data_file_path = f
    return data_file_path


def get_rosters_path():
    """

    """
    with resources.path("collegebaseball.data",
                        "rosters_2013_2022_all.parquet") as f:
        data_file_path = f
    return data_file_path


def get_players_history_path():
    """

    """
    with resources.path("collegebaseball.data",
                        "players_history.csv") as f:
        data_file_path = f
    return data_file_path


# provided by Robert Fray
def get_linear_weights_path():
    """

    """
    with resources.path("collegebaseball.data",
                        "d1_linear_weights.parquet") as f:
        data_file_path = f
    return data_file_path
