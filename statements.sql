CREATE TABLE genre (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(32) UNIQUE
);

CREATE TABLE title_type (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(32) UNIQUE
);

CREATE TABLE title (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tconst VARCHAR(16) UNIQUE,
    title_type_id UUID,
    primary_title VARCHAR(512),
    original_title VARCHAR(512),
    is_adult BOOLEAN,
    start_year SMALLINT,
    end_year SMALLINT,
    runtime_minutes INT,
    FOREIGN KEY (title_type_id) REFERENCES title_type(id)
);

CREATE TABLE title_to_genre (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title_id UUID,
    genre_id UUID,
    FOREIGN KEY (title_id) REFERENCES title(id),
    FOREIGN KEY (genre_id) REFERENCES genre(id)
);
