from msilib import schema
import sqlalchemy
from sqlalchemy import MetaData, Table, Integer, String, Column, Text, DateTime, Boolean, ForeignKey
from sqlalchemy.dialects import postgresql
import json
import os

db = 'postgresql://myuser:1@localhost:5432/myuserdb'

engine = sqlalchemy.create_engine(db)
connection = engine.connect() 


# 1 количество исполнителей в каждом жанре;

sel = connection.execute("""
        SELECT genre_id, COUNT(artist_id) FROM genres_artists
            GROUP BY genre_id;""").fetchmany(30)
print(sel)  
    # [(3, 2), (5, 1), (56, 1), (45, 2), (2, 1), (78, 1)]

# 2 количество треков, вошедших в альбомы 2019-2020 годов;
sel = connection.execute("""
        SELECT album_name, COUNT(album_id) FROM albums 
        JOIN tracks  ON albums.id = tracks.album_id
        WHERE album_date BETWEEN 2019 AND 2020
        GROUP BY album_name;
        """).fetchmany(100)
print(sel)
# [('Горизонт событий с оркестром (Live)', 20), ('Слова на бумаге', 9), ('Призраки Завтра', 8), ('Вира и майна', 11), ('Посолонь', 15), ('Би-2 & Prague Metropolitan Symphonic Orchestra Vol. 3', 19), ('One Fine Day', 9), ('Тайком', 7), ('ПОСЛЕ ЗЛА', 8), ('Лето без интернета', 12), ('Нечётный воин 4. Часть 2 (Retro Edition)', 12), ('Нечётный воин 4. Часть 1', 12)]
    
# 3 средняя продолжительность треков по каждому альбому;
sel = connection.execute("""
        SELECT album_name, ROUND(AVG(duration),1) FROM albums 
        JOIN tracks  ON albums.id = tracks.album_id
        GROUP BY album_name;
        """).fetchmany(10)
print(sel)
# [('Blue Guitars II - Country Blues', Decimal('278163.3')), ('Горизонт событий с оркестром (Live)', Decimal('289772.9')), ('Скаzки', Decimal('207250.9')), ('Слова на бумаге', Decimal('264705.3')), ('Прекрасная любовь', Decimal('251045.1')), ('Раздвоение Личности', Decimal('189550.2')), ('Зимняя акустика. Снежные сны (Live)', Decimal('266825.0')), ('Эксцесс', Decimal('280198.5')), ('Коварство и любовь', Decimal('168665.4')), ('ЧАЙФ. 30 лет (Рождённый в Свердловске) [The Best]', Decimal('231602.4'))]

# 8 средняя продолжительность треков по каждому альбому;
sel = connection.execute("""
        SELECT artist_name, duration FROM artists a
        JOIN albums_artists aa ON a.id = aa.artist_id
        JOIN albums al ON aa.album_id = al.id
        JOIN tracks t ON al.id = t.album_id
        WHERE duration = (SELECT MIN(duration) FROM tracks);
        """).fetchmany(100)
print(sel)
# [('Сплин', 27619)]

#  5 названия сборников, в которых присутствует конкретный исполнитель (выберите сами)
sel = connection.execute("""
        SELECT collection_name FROM artists a
        JOIN albums_artists aa ON a.id = aa.artist_id
        JOIN albums al ON aa.album_id = al.id
        JOIN tracks tr ON al.id = tr.album_id
        JOIN collections_tracks ct ON tr.id = ct.track_id
        JOIN collections c ON ct.collection_id = c.id
        WHERE artist_name = 'Агата Кристи'
        GROUP BY collection_name;
        """).fetchmany(100)
print(sel)

# [('Русский рок',)]

# 6 название альбомов, в которых присутствуют исполнители более 1 жанра;
sel = connection.execute("""
        SELECT artist_name, COUNT(genre_name), album_name FROM albums a
        JOIN albums_artists aa ON a.id = aa.album_id
        JOIN artists ar ON aa.artist_id = ar.id
        JOIN genres_artists ga ON ar.id = ga.artist_id
        JOIN genres g ON ga.genre_id = g.id
        
        GROUP BY a.id, artist_name
        HAVING COUNT(genre_id) > 1
        ORDER BY COUNT(genre_id) DESC

        ;
        """).fetchmany(50)
print(sel)
# [('Чайф', 3, 'Оранжевое настроение-II'), ('Чайф', 3, 'Шекогали'), ('Чайф', 3, 'Дети гор'), ('Чайф', 3, 'Теория струн'), ('Чайф', 3, 'Оранжевое настроение-V'), ('Чайф', 3, 'Слова на бумаге'), ('Чайф', 3, 'Дерьмонтин'), ('Чайф', 3, 'Четвёртый стул'), ('Чайф', 3, 'Концерт'), ('Чайф', 3, 'От себя'), ('Чайф', 3, 'Дуля с маком'), ('Чайф', 3, 'Время не ждёт'), ('Чайф', 3, 'Концерт'), ('Чайф', 3, 'Реальный мир'), ('Чайф', 3, 'Не беда'), ('Чайф', 3, 'Оранжевое настроение'), ('Чайф', 3, 'Кино, вино и домино'), ('Чайф', 3, 'Оранжевое настроение - IV'), ('Чайф', 3, 'ЧАЙФ. 30 лет (Рождённый в Свердловске) [The Best]'), ('Чайф', 3, 'Пусть всё будет так, как ты захочешь'), ('Чайф', 3, '48'), ('Чайф', 3, 'Немного похоже на блюз (Remastered)'), ('Чайф', 3, 'Давай вернёмся'), ('Чайф', 3, 'Зимняя акустика. Снежные сны (Live)'), ('Чайф', 3, 'Оранжевое настроение-III'), ('Чайф', 3, 'Изумрудные хиты'), ('Алиса', 2, '20.12'), ('Алиса', 2, 'Саботаж'), ('Алиса', 2, 'Посолонь'), ('Алиса', 2, 'Цирк'), ('Алиса', 2, 'Ъ'), ('Алиса', 2, 'Пульс хранителя дверей лабиринта'), ('Алиса', 2, 'Стать Севера'), ('Алиса', 2, 'Эксцесс'), ('Алиса', 2, 'Изгой'), ('Алиса', 2, 'Мы вместе 20 лет, Ч. 1 (Live)')]

# 4 все исполнители, которые не выпустили альбомы в 2020 году;
sel = connection.execute("""
        SELECT artist_name FROM artists a 
        WHERE artist_name NOT IN (SELECT artist_name FROM artists a
        JOIN albums_artists aa ON a.id = aa.artist_id
        JOIN albums al ON aa.album_id = al.id
        WHERE album_date = 2020
        GROUP BY artist_name)
        GROUP BY artist_name;
        """).fetchmany(50)
print(sel)


# 7 наименование треков, которые не входят в сборники;
sel = connection.execute("""
        SELECT track_name FROM tracks
        WHERE track_name NOT IN(SELECT track_name FROM tracks tr
        JOIN collections_tracks ct ON tr.id = ct.track_id
        JOIN collections c ON ct.collection_id = c.id
        WHERE track_id IN (SELECT ct.track_id FROM collections_tracks)
        GROUP BY track_name);
        """).fetchmany(100)
print()
print(sel)


# 9 название альбомов, содержащих наименьшее количество треков.

sel = connection.execute("""
        SELECT album_name, COUNT(album_id) FROM albums al
        JOIN tracks tr ON al.id = tr.album_id
        GROUP BY album_name, album_id
        HAVING COUNT(album_id) <= 
        (SELECT COUNT(album_id) FROM albums al 
        JOIN tracks tr ON al.id = tr.album_id
        GROUP BY album_name
        ORDER BY COUNT(album_id)
        LIMIT 1);
        """).fetchmany(100)
print()
print(sel)