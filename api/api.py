from fastapi import FastAPI, HTTPException
import psycopg
import os

app = FastAPI()


@app.get("/titles")
async def root(id_="", offset="0", limit="10"):
    try:
        with psycopg.connect(
            f"host=localhost port={os.environ.get('PG_PORT')} connect_timeout=10 user={os.environ.get('PG_LOCAL_USER')} password={os.environ.get('PG_LOCAL_PASSWORD')}"
        ) as conn:
            with conn.cursor() as cur:
                if not id_:
                    records = cur.execute(
                        """
                            SELECT id, primary_title
                            FROM title
                            OFFSET %s
                            LIMIT %s;
                        """,
                        [offset, limit],
                    )
                    return [{"id": record[0], "title": record[1]} for record in records]
                else:
                    records = cur.execute(
                        """
                            SELECT id, primary_title
                            FROM title
                            WHERE id = %s;
                        """,
                        [id_],
                    )
                    return [{"id": record[0], "title": record[1]} for record in records]

    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise HTTPException(status_code=500, detail="Internal server error")
