from psycopg2.extras import execute_values
import datetime
import logging


logger = logging.getLogger(__name__)


def upsert_many(
    cur,
    table: str,
    columns: list[str],
    primary_keys: list[str],
    data: list[dict],
    include_updated_at: bool = True,
):
    if include_updated_at:
        now = datetime.datetime.now(datetime.timezone.utc)
        for row in data:
            row["updated_at"] = now
        columns.append("updated_at")

    column_str = ", ".join(columns)
    primary_key_str = ", ".join(primary_keys)
    on_conflict_update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in primary_keys])
    template = "(" + ", ".join([f"%({col})s" for col in columns]) + ")"
    query = f"""
        INSERT INTO {table} ({column_str})
        VALUES %s
        ON CONFLICT ({primary_key_str})
        DO UPDATE SET 
            {on_conflict_update_str}
    """
    logger.debug("Executing query: %s", query)
    execute_values(cur, query, data, template)
