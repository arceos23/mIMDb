import csv
import psycopg
import os
import time

POSTGRES_URL = f"postgresql://{os.environ.get('PG_LOCAL_USER')}:{os.environ.get('PG_LOCAL_PASSWORD')}@localhost:5423"


def store_data():
    try:
        with open("./IMDb_data/title.basics.tsv", "r", newline="") as csvfile:
            start_time = time.perf_counter()

            reader = csv.DictReader(csvfile, delimiter="\t")

            with psycopg.connect(
                f"host=localhost port={os.environ.get('PG_PORT')} connect_timeout=10 user={os.environ.get('PG_LOCAL_USER')} password={os.environ.get('PG_LOCAL_PASSWORD')}"
            ) as conn:
                with conn.cursor() as cur:
                    for i, row in enumerate(reader):
                        # Insert genres
                        genres = row["genres"].split(",")
                        genre_ids = []
                        if genres:
                            for genre in genres:
                                genre_ids.append(
                                    cur.execute(
                                        """
                                        WITH e AS (
                                            INSERT INTO genre2 (name)
                                            VALUES (%s)
                                            ON CONFLICT (name) DO NOTHING
                                            RETURNING id
                                        )
                                        SELECT *
                                        FROM e
                                        UNION
                                            SELECT id
                                            FROM genre2
                                            WHERE name = %s;
                                    """,
                                        [genre, genre],
                                    ).fetchone()[0]
                                )

                        # Insert title type
                        title_type_id = cur.execute(
                            """
                                WITH e AS (
                                    INSERT INTO title_type2 (name)
                                    VALUES (%s)
                                    ON CONFLICT (name)
                                    DO NOTHING
                                    RETURNING id
                                )
                                SELECT *
                                FROM e
                                UNION
                                    SELECT id
                                    FROM title_type2
                                    WHERE name = %s;
                            """,
                            [row["titleType"], row["titleType"]],
                        ).fetchone()[0]

                        # Insert title
                        title_id = cur.execute(
                            """
                                INSERT INTO title2 (tconst, title_type_id, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                RETURNING id;
                            """,
                            [
                                row["tconst"],
                                title_type_id,
                                row["primaryTitle"],
                                row["originalTitle"],
                                row["isAdult"],
                                (
                                    int(row["startYear"])
                                    if row["startYear"] != "\\N"
                                    else None
                                ),
                                (
                                    int(row["endYear"])
                                    if row["endYear"] != "\\N"
                                    else None
                                ),
                                (
                                    int(row["runtimeMinutes"])
                                    if row["runtimeMinutes"] != "\\N"
                                    else None
                                ),
                            ],
                        ).fetchone()[0]

                        # Insert many-to-many mapping between titles and genres
                        for genre_id in genre_ids:
                            cur.execute(
                                """
                                    INSERT INTO title_to_genre2 (title_id, genre_id)
                                    VALUES (%s, %s);
                                """,
                                [
                                    title_id,
                                    genre_id,
                                ],
                            )

                        if i % 10000 == 0:
                            conn.commit()

            conn.commit()
            end_time = time.perf_counter()
            print(
                f"Successfully saved title.basics.tsv in {end_time - start_time} seconds.\n"
            )

    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")


def main():
    store_data()


if __name__ == "__main__":
    main()
