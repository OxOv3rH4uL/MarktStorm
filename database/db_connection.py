import duckdb
from pathlib import Path

DB_PATH = Path(r"C:\Users\91994\OneDrive\Desktop\MarktStorm\database\marktstorm.duckdb")
SQL_PATH = Path(r"C:\Users\91994\OneDrive\Desktop\MarktStorm\database\schema.sql")


# print(type(DB_PATH))
def get_conn():
    return duckdb.connect(str(DB_PATH))

def initialize_db():
    conn = get_conn()
    schema = SQL_PATH.read_text(encoding="UTF-8")
    conn.execute(schema)

    print("DB Initialized i guess")
    return conn

if __name__ == "__main__":
    conn = initialize_db()
    conn.close()


    