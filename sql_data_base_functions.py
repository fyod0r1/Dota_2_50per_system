import sqlite3


"""Базовые функции для работы с базой данных через SQL"""
connection = sqlite3.Connection("my_database.db")
cursor = connection.cursor()


def create_tables():
    """Создает таблицы игроков и игр"""
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS players
        (player_id INTEGER UNIQUE, mmr INTEGER, games_count INTEGER)"""
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS matches
        (match_id INTEGER UNIQUE, player_id INTEGER, win_or_loose INTEGER)"""
    )


def reset_tables():
    """Функция которая удаляет таблицы и создает новые пустые"""
    cursor.execute(
        """DROP TABLE IF EXISTS players"""
    )
    cursor.execute(
        """DROP TABLE IF EXISTS matches"""
    )
    create_tables()


def insert_user(player_id: int, mmr: int):
    """Функция для записи айди игрока и его ммр, с исключением дублирующих строк"""
    cursor.execute(
        f"""INSERT or IGNORE INTO players
        VALUES ({player_id}, {mmr}, '0')"""
    )
    connection.commit()


def insert_match(match_id: int, player_id: int, win_or_loose: int):
    """Функция для записи айди матча, игрока, и исхода матча для игрока с исключением дублирующих строк"""
    cursor.execute(
        f"""INSERT or IGNORE INTO matches
        VALUES ({match_id}, {player_id}, {win_or_loose})"""
    )
    connection.commit()


def get_players_id():
    """Возвращает айди игрокв находящиеся в базе данных"""
    data = cursor.execute(
        """SELECT player_id FROM players"""
    )
    buf = []
    for i in data.fetchall():
        buf.append(i[0])
    data = buf
    return data


def win_and_loose_by_player():
    """Возвращает таблицу с матчей для работы в jupyter notebook"""
    wl_list = cursor.execute(
        """SELECT player_id, win_or_loose FROM matches 
        WHERE player_id IN (SELECT player_id FROM matches GROUP BY player_id HAVING COUNT(win_or_loose) >= 100) 
        ORDER BY player_id"""
    )
    return wl_list


def test_select():
    print(cursor.execute("""SELECT * FROM players""").fetchall())
    print(cursor.execute("""SELECT * FROM matches""").fetchall())


def count_matches_recorded():
    print(cursor.execute("""SELECT COUNT(DISTINCT player_id) as count FROM matches""").fetchall())


if __name__ == '__main__':
    print(win_and_loose_by_player())

