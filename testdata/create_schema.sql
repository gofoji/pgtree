CREATE SCHEMA IF not EXISTS foo;
CREATE SCHEMA
    AUTHORIZATION joe;
CREATE SCHEMA hollywood
    CREATE TABLE films (title text, release date, awards text[])
    CREATE VIEW winners AS
    SELECT title, release FROM films WHERE awards IS NOT NULL;
drop schema boohoo;