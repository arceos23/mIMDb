import polars as pl
import os
import time

POSTGRES_URL = f"postgresql://{os.environ.get('PG_LOCAL_USER')}:{os.environ.get('PG_LOCAL_PASSWORD')}@localhost:5423"


def store_data():
    try:
        df = pl.read_csv(
            "./IMDb_data/title.basics.tsv",
            separator="\t",
            null_values=["\\N"],
            quote_char=None,
        )

        # Get unique genres and insert into database
        unique_genres = set()
        for row in df["genres"]:
            if row:
                unique_genres.update(row.split(","))
        df_genres = pl.DataFrame({"name": list(unique_genres)})

        start_time = time.perf_counter()
        df_genres.write_database("genre", POSTGRES_URL, if_table_exists="append")
        end_time = time.perf_counter()
        print(
            f"Successfully saved genres to PostgreSQL in {end_time - start_time} seconds.\n"
        )

        # Get unique title types and insert into database
        start_time = time.perf_counter()
        df_title_types = pl.DataFrame({"name": df["titleType"].unique()})
        df_title_types.write_database(
            "title_type", POSTGRES_URL, if_table_exists="append"
        )
        end_time = time.perf_counter()
        print(
            f"Successfully saved title types to PostgreSQL in {end_time - start_time} seconds.\n"
        )

        # Insert titles into database
        df = df.drop(
            "genres"
        )  # Remove genres since they will be stored in a many-to-many table

        title_types = pl.read_database_uri(
            "SELECT id, name FROM title_type;", POSTGRES_URL
        )
        title_type_to_id = {dict["name"]: dict["id"] for dict in title_types.to_dicts()}
        df = df.with_columns(
            pl.col("titleType").replace(title_type_to_id).alias("titleType")
        )  # Replace title type with its corresponding ID in PostgreSQL

        df = df.with_columns(pl.col("isAdult").cast(pl.Boolean).alias("isAdult"))

        df = df.rename(
            {
                "titleType": "title_type_id",
                "primaryTitle": "primary_title",
                "originalTitle": "original_title",
                "isAdult": "is_adult",
                "startYear": "start_year",
                "endYear": "end_year",
                "runtimeMinutes": "runtime_minutes",
            }
        )

        start_time = time.perf_counter()
        df.write_database("title", POSTGRES_URL, if_table_exists="append")
        end_time = time.perf_counter()
        print(
            f"Successfully saved titles to PostgreSQL in {end_time - start_time} seconds.\n"
        )

    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")


def main():
    store_data()


if __name__ == "__main__":
    main()
