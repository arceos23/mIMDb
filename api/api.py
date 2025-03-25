from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, UUID4
from psycopg.rows import class_row
import psycopg
import os

app = FastAPI()


class Title(BaseModel):
    id: UUID4  # id stored as version 4 UUID in PostgreSQL
    tconst: str
    title_type_id: UUID4
    primary_title: str | None = None
    original_title: str | None = None
    is_adult: bool | None = None
    start_year: int | None = None
    end_year: int | None = None
    runtime_minutes: int | None = None


@app.get("/titles")
async def root(id_="", offset="0", limit="10"):
    try:
        with psycopg.connect(
            f"host=localhost port={os.environ.get('PG_PORT')} connect_timeout=10 user={os.environ.get('PG_LOCAL_USER')} password={os.environ.get('PG_LOCAL_PASSWORD')}"
        ) as conn:
            with conn.cursor(row_factory=class_row(Title)) as cur:
                if not id_:
                    cur = cur.execute(
                        """
                            SELECT id, tconst, title_type_id, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes
                            FROM title
                            OFFSET %s
                            LIMIT %s;
                        """,
                        [offset, limit],
                    )
                    return [record for record in cur]
                else:
                    cur = cur.execute(
                        """
                            SELECT id, tconst, title_type_id, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes
                            FROM title
                            WHERE id = %s;
                        """,
                        [id_],
                    )
                    return cur.fetchone()

    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise HTTPException(status_code=500, detail="Internal server error")
