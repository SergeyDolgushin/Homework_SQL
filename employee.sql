
create table if not exists Employee (
id serial primary key,
parent_id int references Employee(id),
name varchar(60) not null,
title varchar(40) not null
);

create table if not exists Department (
id serial primary key,
name_id int references Employee(id),
department text,
name varchar(60) not null
);






