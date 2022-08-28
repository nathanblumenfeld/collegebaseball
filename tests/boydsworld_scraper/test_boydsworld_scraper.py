import pytest
from collegebaseball import boydsworld_scraper
from time import sleep
import random


_TIMEOUT = 1


@ pytest.fixture()
def generate_boydsworld_games():
    teams = [
        ('Cornell', 2002, 2018),
        ('Harvard', 2019, 2019)
    ]
    generated_data = []
    for i in teams:
        sleep(random.uniform(0, _TIMEOUT))
        data = boydsworld_scraper.boydsworld_team_results(i[0], i[1], i[2])
        generated_data.append(data)
    return generated_data


def test_boydsworld_games(generate_boydsworld_games):
    for i in generate_boydsworld_games:
        assert i is not None
