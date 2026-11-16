import uuid
from pathlib import Path
import hashlib
import duckdb
import os
import subprocess

DB_PATH = Path(r"C:\Users\91994\OneDrive\Desktop\MarktStorm\database\marktstorm.duckdb")

class DuckDBWriter:
    def __init__(self):
        self.conn = duckdb.connect(str(DB_PATH))


    def create_snapshot(self):

        temp_path = DB_PATH.parent / "marktstorm-snapshot-tmp.duckdb"

        source_db = self.conn.execute(
            "SELECT current_database()"
        ).fetchone()[0]

        if temp_path.exists():
            temp_path.unlink()

        temp_sql_path = temp_path.as_posix().replace("'", "''")
        source_name = source_db.replace('"', '""')

        self.conn.execute(
            f"ATTACH '{temp_sql_path}' AS grafana_tmp "
            "(STORAGE_VERSION 'v1.5.0')"
        )

        try:
            self.conn.execute(f"""
                COPY FROM DATABASE "{source_name}" TO grafana_tmp
            """)
        finally:
            self.conn.execute("DETACH grafana_tmp")

        subprocess.run([
            "docker", "cp",
            str(temp_path),
            "grafana:/var/lib/grafana/duckdb/marktstorm-snapshot-tmp.duckdb"
        ], check=True)

        subprocess.run([
            "docker", "exec", "grafana",
            "mv", "-f",
            "/var/lib/grafana/duckdb/marktstorm-snapshot-tmp.duckdb",
            "/var/lib/grafana/duckdb/marktstorm-snapshot.duckdb"
        ], check=True)

        temp_path.unlink()



        




    def insert_order_metrics(self, order, topic, partition, offset):
        message_key = f"{topic}-{partition}-{offset}"

        rid = int.from_bytes(
            hashlib.sha256(message_key.encode()).digest()[:8],
            byteorder="big"
        ) & 0x7FFFFFFFFFFFFFFF

        try:
            self.conn.execute(
                """
                INSERT INTO order_metrics(
                    id,
                    groupType,
                    groupKey,
                    orderCount,
                    totalQuantity,
                    totalRevenue,
                    windowStart,
                    windowEnd
                    )
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT (id) DO NOTHING
                """,
                [
                    rid,
                    order["groupType"],
                    order["groupKey"],
                    order["orderCount"],
                    order["totalQuantity"],
                    order["totalRevenue"],
                    order["windowStart"],
                    order["windowEnd"]
                ]
            )
        except Exception:
            raise 

    def insert_product_metrics(self, order, topic, partition, offset):
        message_key = f"{topic}-{partition}-{offset}"

        rid = int.from_bytes(
            hashlib.sha256(message_key.encode()).digest()[:8],
            byteorder="big"
        ) & 0x7FFFFFFFFFFFFFFF

        try:
            self.conn.execute(
                """
                INSERT INTO product_metrics(
                    id,
                    groupType,
                    groupKey,
                    orderCount,
                    totalQuantity,
                    totalRevenue,
                    product,
                    category,
                    windowStart,
                    windowEnd
                    )
                VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT (id) DO NOTHING
                """,
                [
                    rid,
                    order["groupType"],
                    order["groupKey"],
                    order["orderCount"],
                    order["totalQuantity"],
                    order["totalRevenue"],
                    order["product"],
                    order["category"],
                    order["windowStart"],
                    order["windowEnd"]
                ]
            )
        except Exception:
            raise 

    def insert_user_metrics(self, order, topic, partition, offset):
        message_key = f"{topic}-{partition}-{offset}"

        rid = int.from_bytes(
            hashlib.sha256(message_key.encode()).digest()[:8],
            byteorder="big"
        ) & 0x7FFFFFFFFFFFFFFF

        try:
            self.conn.execute(
                """
                INSERT INTO user_metrics(
                    id,
                    groupType,
                    groupKey,
                    orderCount,
                    totalQuantity,
                    totalRevenue,
                    windowStart,
                    windowEnd
                    )
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT (id) DO NOTHING
                """,
                [
                    rid,
                    order["groupType"],
                    order["groupKey"],
                    order["orderCount"],
                    order["totalQuantity"],
                    order["totalRevenue"],
                    order["windowStart"],
                    order["windowEnd"]
                ]
            )
        except Exception:
            raise 

    def insert_dead_letter(self, dl, topic, partition, offset):
        message_key = f"{topic}-{partition}-{offset}"
        rid = int.from_bytes(
            hashlib.sha256(message_key.encode()).digest()[:8],
            byteorder="big"
        ) & 0x7FFFFFFFFFFFFFFF

        try:
            self.conn.execute(
                """
                INSERT INTO dead_letter(
                    id,
                    message,
                    error,
                    rejectedAt
                    )
                VALUES(?,?,?,?)
                ON CONFLICT(id) DO NOTHING
                """,
                [
                    rid,
                    dl["message"],
                    dl["error"],
                    dl["rejectedAt"]
                ]
            )
        except Exception:
            raise


    def close(self):
        self.conn.close()

