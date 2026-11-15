import uuid
from pathlib import Path
import hashlib
import duckdb

DB_PATH = Path(r"C:\Users\91994\OneDrive\Desktop\MarktStorm\database\marktstorm.duckdb")

class DuckDBWriter:
    def __init__(self):
        self.conn = duckdb.connect(str(DB_PATH))

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
