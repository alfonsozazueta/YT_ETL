import logging
logger = logging.getLogger(__name__)

table = "yt_api"

def coerce_params(row: dict) -> dict:
    """
    Convierte row (json-style o db-style) a las keys exactas que pide la tabla:
    Video_ID, Video_Title, Upload_Date, Duration, Video_Views, Likes_Count, Comments_Count
    """
    # Caso 1: viene de JSON (staging load)
    if "video_id" in row:
        return {
            "Video_ID": row["video_id"],
            "Video_Title": row["title"],
            "Upload_Date": row["publishedAt"],
            "Duration": row["duration"],
            "Video_Views": row.get("viewCount"),
            "Likes_Count": row.get("likeCount"),
            "Comments_Count": row.get("commentCount"),
        }

    # Caso 2: viene de DB (RealDictCursor) / transform (core)
    return {
        "Video_ID": row["Video_ID"],
        "Video_Title": row["Video_Title"],
        "Upload_Date": row["Upload_Date"],
        "Duration": row["Duration"],
        "Video_Views": row.get("Video_Views"),
        "Likes_Count": row.get("Likes_Count"),
        "Comments_Count": row.get("Comments_Count"),
    }


def insert_rows(cur, conn, schema, row):
    params = coerce_params(row)

    try:
        logger.error(f"[DEBUG] insert schema={schema} params_keys={list(params.keys())}")

        cur.execute(
            f"""
            INSERT INTO {schema}.{table}
                ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
            VALUES
                (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s);
            """,
            params,
        )

        conn.commit()
        logger.info(f'Inserted row with Video_ID: {params["Video_ID"]}')

    except Exception as e:
        conn.rollback()
        logger.error(f'Error inserting row with Video_ID: {params.get("Video_ID")} - {e}')
        raise


def update_rows(cur, conn, schema, row):
    params = coerce_params(row)

    try:
        logger.error(f"[DEBUG] update schema={schema} params_keys={list(params.keys())}")

        cur.execute(
            f"""
            UPDATE {schema}.{table}
            SET
                "Video_Title" = %(Video_Title)s,
                "Duration" = %(Duration)s,
                "Video_Views" = %(Video_Views)s,
                "Likes_Count" = %(Likes_Count)s,
                "Comments_Count" = %(Comments_Count)s
            WHERE
                "Video_ID" = %(Video_ID)s
                AND "Upload_Date" = %(Upload_Date)s;
            """,
            params,
        )

        conn.commit()
        logger.info(f'Updated row with Video_ID: {params["Video_ID"]}')

    except Exception as e:
        conn.rollback()
        logger.error(f'Error updating row with Video_ID: {params.get("Video_ID")} - {e}')
        raise


def delete_rows(cur, conn, schema, ids_to_delete):
    """
    ids_to_delete: list[str]
    """
    try:
        if not ids_to_delete:
            return

        # psycopg2 soporta IN %s con tuple
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN %s;
            """,
            (tuple(ids_to_delete),),
        )

        conn.commit()
        logger.info(f"Deleted rows with Video_IDs: {ids_to_delete}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting rows with Video_IDs: {ids_to_delete} - {e}")
        raise