import sqlite3
import sql_data_base_functions as sql_fun
import time
import requests

LAST_MATCH = 6463287905  # случайно выбранный айди матча
SOURCE = 'https://api.opendota.com/api'  # ссылка для работы с api OpenDota


def generate_matches_id(matches_count: int, offset=0):
    """Генерирует список существующих айди матчей"""
    id_array = []
    for i in range(LAST_MATCH - offset, LAST_MATCH - matches_count - offset, -1):
        id_array.append(i)
    return id_array


def get_account_ids(match_id: int):
    """Возвращает список айди всех игроков участвоваших в матче"""
    match_obj = requests.get(SOURCE + '/matches/' + str(match_id)).json()
    try:
        match_obj = match_obj['players']
    except KeyError:
        return []
    players_id = []
    for i in range(10):
        players_id.append(match_obj[i]['account_id'])
    while None in players_id:
        players_id.remove(None)
    return players_id


def get_player(player_id: int):
    """Возвращает словарь с айди игрока и его ммр, основываясь на его ммр"""
    time.sleep(1)
    player_obj = requests.get(SOURCE + '/players/' + str(player_id)).json()
    try:
        player_obj = {'account_id': player_id, 'mmr': player_obj['mmr_estimate']['estimate']}
    except KeyError:
        player_obj = None
    return player_obj


def get_matches(player_id):
    """Возвращает список матчей игрока с нужными данными"""
    time.sleep(1)
    response = requests.get(SOURCE + '/players/' + str(player_id) +
                            '/matches/?game_mode=22&limit=100&lobby_type=7').json()
    match_array = []
    for i in range(len(response)):
        try:
            match_obj = {'match_id': response[i]['match_id'],
                         'win_or_loose': 1 if (not (response[i]['player_slot'] >= 4 ^ response[i]['radiant_win']))
                         else 0}
            match_array.append(match_obj)
        except KeyError:
            pass
        except TypeError:
            pass
    return match_array


def record_players(players_count: int, offset=0):
    """Записывает айди и ммр игроков в базу данных"""
    matches_count = players_count // 10 + 1
    while players_count != 0:
        id_array = generate_matches_id(matches_count, offset)
        offset += matches_count
        for i in id_array:
            player_ids = get_account_ids(i)
            for j in player_ids:
                player_obj = get_player(j)
                if player_obj:
                    sql_fun.insert_user(player_obj['account_id'], player_obj['mmr'])
                    players_count -= 1
                    print('Players to record left: ' + str(players_count))
                if players_count == 0:
                    break


def record_matches(offset=0):
    """Записывает матчи игрока в базу данных"""
    players_id = sql_fun.get_players_id()[slice(offset, -1)]
    counter = offset
    for i in players_id:
        for j in get_matches(i):
            sql_fun.insert_match(j['match_id'], i, j['win_or_loose'])
        counter += 1
        print("Players writen: " + str(counter) + ' player_id: ' + str(i))


if __name__ == '__main__':
    """Для записи всех необходимых данных, нужно сначала записать айди игроков в базу данных, 
    а после получить их игры"""
    # p_offset = 8178
    # record_matches(p_offset)
    sql_fun.test_select()
