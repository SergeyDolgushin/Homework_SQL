from msilib import schema
import sqlalchemy
from sqlalchemy import MetaData, Table, Integer, String, Column, Text, DateTime, Boolean, ForeignKey
from sqlalchemy.dialects import postgresql
import json
import os

db = 'postgresql://myuser:1@localhost:5432/myuserdb'


def deleteAll(connection):
    # request_sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog');";
    request_sql = f"DELETE FROM albums_artists;"
    connection.execute(request_sql)
    request_sql = f"DELETE FROM artists;"
    connection.execute(request_sql)
    request_sql = f"DELETE FROM tracks;"
    connection.execute(request_sql)
    request_sql = f"DELETE FROM albums;"
    connection.execute(request_sql)
   



def readSavedJsonSpotify(file_name = 'genresSpotify.json'):
    with open(os.getcwd() + '/' + file_name, encoding = "utf-8") as f:
        r = json.load(f)
    return r    

def findNumberColumns(connection, table_name = 'table_name'):
    request_sql = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}';"
    # request_sql = f"SELECT COUNT(*) FROM {table_name};"
    # request_sql = f"SELECT * FROM {table_name};"
    request_sql = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE INFORMATION_SCHEMA.COLUMNS.TABLE_NAME = '{table_name}';" 
    # request_sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog');";
    # request_sql = f"SELECT COUNT(*) FROM {table_name};"
    print(request_sql)
    sel = connection.execute(request_sql).fetchall()
    print(sel)

def insertToTable(connection, name_table, qunatity_column = 2, **kwarg):

    print(kwarg)
    if (qunatity_column == 2):
        for i, item in enumerate(kwarg['genres']):
            print(item)
            request_sql = f"INSERT INTO {name_table}(id, genre_name) VALUES({i+1}, '{item}');"
            print(request_sql)
            connection.execute(request_sql)

def buildTables(connection):
    # заполнение базы из JSON файлов в каталоге /temp
    artist_id = 0
    album_id = 0
    albums_artists_id = 0
    track_id = 0

    lst_files = os.listdir(os.getcwd() + '\\temp')
    for file in lst_files:
        if file.endswith(".json"):
            with open(os.getcwd() + '\\temp\\' + file) as f:
                responce = json.load(f)
                request_sql = f"INSERT INTO artists(id, artist_name) VALUES({artist_id}, '{responce['artists']}');"
                connection.execute(request_sql)
                print(responce['artists'])
                for album in responce['albums']:
                    request_sql = f"INSERT INTO albums(id, album_name, album_date) VALUES({album_id}, '{album['album']}', '{int(album['release_date'][:4])}');" 
                    connection.execute(request_sql) 
                    request_sql = f"INSERT INTO albums_artists(id, artist_id, album_id) VALUES({albums_artists_id}, {artist_id}, {album_id});"
                    connection.execute(request_sql) 
                    print(album['album'])
                    albums_artists_id += 1
                    for track in album['tracks']:
                        request_sql = f"INSERT INTO tracks(id, track_name, duration, album_id) VALUES({track_id}, '{track['track']}', {track['duration']}, {album_id});"
                        connection.execute(request_sql) 
                        print(track['track'])
                        track_id += 1
                    album_id += 1
                artist_id += 1

def createTable(engine):

    metadata = MetaData()
   
    user = Table('users', metadata,
        Column('id', Integer(), primary_key=True),
        Column('user', String(80), nullable=False),
    )

    preference = Table('preference', metadata,
        Column('id', Integer(), primary_key=True),
        Column('user_id', Integer(), ForeignKey("users.id")),
        Column('artist_id', Integer(), ForeignKey("artists.id"))
    )

    metadata.create_all(engine)
    return user


if __name__ == '__main__':
    
    engine = sqlalchemy.create_engine(db)
    connection = engine.connect() 
        
    findNumberColumns(connection, table_name = 'tracks')
    # deleteAll(connection)
    # insertToTable('genres', 1, **readSavedJsonSpotify()) 

    # заполнение базы данных на основе информации из списка JSON в папке temp
    # buildTables(connection)
                
    sel = connection.execute("""
        SELECT album_name, album_date FROM albums 
        WHERE album_date = 2018;
        """).fetchmany(100)
    print(sel)

    # выборка по условию
    # [('Горизонт событий @ ВТБ Арена (Live)', 2018), ('Концерт в Кремле (Live)', 2018), ('Немного похоже на блюз (Remastered)', 2018), ('Галя ходи', 2018), ('Встречная полоса', 2018), ('Всыпал снег не случайно', 2018), ('Вечерний чай (Live)', 2018), ('Морская.20 (Live)', 2018), ('ВОСТОК X CЕВЕРОЗАПАД', 2018)]

    print(connection.execute("""SELECT track_name, duration FROM tracks
        ORDER BY duration DESC
        LIMIT 1;
        """).fetchall())
    # время в мс
    # [('Эшелон', 672844)]

    sel = connection.execute("""
        SELECT track_name FROM tracks 
        WHERE duration >= (3*60+30)*1000;
        """).fetchmany(30)
    print(sel)

    # [('Айлавью',), ('Нисхождение',), ('Сирота',), ('Вольно!',), ('Как на войне',), ('Молитва',), ('Джиги дзаги',), ('Позорная звезда',), ('Я буду там',), ('Сытая свинья',), ('Канкан',), ('Пантера',), ('Бесса мэ...',), ('Эпилог',), ('Щёкотно',), ('Нисхождение',), ('Вольно!',), ('Позорная звезда',), ('Халигаликришна',), ('Тоска без конца',), ('Абордаж',), ('Вечная любовь',), ('Гетеросексуалист',), ('Дворник',), ('Два корабля',), ('Ураган',), ('Моряк',), ('Поход',), ('Корвет уходит в небеса',), ('Эпилог (Предисловие)',)]

    sel = connection.execute("""
        SELECT collection_name FROM collections 
        WHERE collection_date BETWEEN 2018 AND 2020;
        """).fetchmany(30)
    print(sel)

    # [('Русский рок',), ('Лучшее, Алиса',), ('Лучшее, Мумий Тролль',)]

    sel = connection.execute("""
        SELECT artist_name FROM artists 
        WHERE artist_name NOT LIKE '%% %%';
        """).fetchmany(30)
    print(sel)

    # [('Алиса',), ('БИ-2',), ('Чайф',), ('ДДТ',), ('Сплин',)]

    sel = connection.execute("""
        SELECT track_name FROM tracks 
        WHERE track_name LIKE '%% мой%%';
        """).fetchmany(30)
    print(sel)
    # [('Город мой',), ('Рок - мой выбор',), ('Рок - мой выбор',), ('Город мой',), ('Город мой - Live',), ('Город мой',)]

    sel = connection.execute("""
        SELECT track_name FROM tracks 
        WHERE track_name LIKE '%%My%%';
        """).fetchmany(30)
    print(sel)  

    # [('Rock My Soul',), ('Dancing My Blues Away',), ('Lose My Heart in You',), ('Boss Man Cut My Chains',), ('The Soul of My Father`s Shadow',), ('My Blue World Says Hello',), ('My Luck',)]

    createTable(engine)
    