CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE NOT NULL,
    rating FLOAT,
    type TEXT NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL
);

CREATE TABLE IF NOT EXISTS content.person_film_work(
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    person_id uuid NOT NULL,
    film_work_id uuid NOT NULL,
    role TEXT NOT NULL,
    created timestamp with time zone NOT NULL
);

CREATE TABLE IF NOT EXISTS content.person(
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name TEXT NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL
);

CREATE TABLE IF NOT EXISTS content.genre_film_work(
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    genre_id uuid NOT NULL,
    film_work_id uuid NOT NULL,
    created timestamp with time zone NOT NULL
);

CREATE TABLE IF NOT EXISTS content.genre(
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL
);

ALTER TABLE content.person_film_work 
    ADD CONSTRAINT fk_person_film_work_to_person 
    FOREIGN KEY (person_id) REFERENCES content.person(id) 
    ON DELETE CASCADE;

ALTER TABLE content.person_film_work 
    ADD CONSTRAINT fk_person_film_work_to_film_work 
    FOREIGN KEY (film_work_id) REFERENCES content.film_work(id) 
    ON DELETE CASCADE;

ALTER TABLE content.genre_film_work 
    ADD CONSTRAINT fk_genre_film_work_to_genre 
    FOREIGN KEY (genre_id) REFERENCES content.genre(id) 
    ON DELETE CASCADE;

ALTER TABLE content.genre_film_work 
    ADD CONSTRAINT fk_genre_film_work_to_film_work 
    FOREIGN KEY (film_work_id) REFERENCES content.film_work(id) 
    ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_film_work_title ON content.film_work(title);
CREATE INDEX IF NOT EXISTS idx_film_work_rating ON content.film_work(rating);
CREATE INDEX IF NOT EXISTS idx_film_work_type ON content.film_work(type);
CREATE INDEX IF NOT EXISTS idx_film_work_creation_date ON content.film_work(creation_date);

CREATE INDEX IF NOT EXISTS idx_person_full_name ON content.person(full_name);

CREATE INDEX IF NOT EXISTS idx_genre_name ON content.genre(name);

CREATE INDEX IF NOT EXISTS idx_genre_film_work_genre_id ON content.genre_film_work(genre_id);
CREATE INDEX IF NOT EXISTS idx_genre_film_work_film_work_id ON content.genre_film_work(film_work_id);

CREATE INDEX IF NOT EXISTS idx_person_film_work_person_id ON content.person_film_work(person_id);
CREATE INDEX IF NOT EXISTS idx_person_film_work_film_work_id ON content.person_film_work(film_work_id);
CREATE INDEX IF NOT EXISTS idx_person_film_work_role ON content.person_film_work(role);