create table if not exists Artists (
id serial primary key,
artist_name varchar(60) not null
);

create table if not exists Genres (
id serial primary key,
genre_name varchar(40) unique not null
);

create table if not exists Genres_Artist (
id serial primary key,
genre_id integer references Genres(id),
artist_id integer references Artists(id)
);

create table if not exists Albums (
id serial primary key,
album_name text not null,
album_date  integer not null
);

create table if not exists Album_Artist (
id serial primary key,
album_id integer references Albums(id),
artist_id integer references Artists(id)
);


create table if not exists Tracks (
id serial primary key,
track_name text not null,
duration integer not null,
album_id integer references Albums(id)
);

create table if not exists Collection (
id serial primary key,
collection_name text not null,
collection_date integer not null
);

create table if not exists Collection_Tracks (
id serial primary key,
collection_id integer references Collection(id),
track_id integer references Tracks(id)
);






