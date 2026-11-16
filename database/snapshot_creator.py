# import duckdb
# from pathlib import Path
# DB_PATH = Path(r"C:\Users\91994\OneDrive\Desktop\MarktStorm\database\marktstorm.duckdb")

# conn = duckdb.connect("marktstorm.duckdb")
# source_database = conn.execute(
#     "SELECT current_database()"
# ).fetchone()[0]

# snapshot_path = (
#     DB_PATH.parent / "marktstorm-snapshot.duckdb"
# ).as_posix().replace("'", "''")

# conn.execute(
#     f"ATTACH '{snapshot_path}' AS grafana_db "
#     "(STORAGE_VERSION 'v1.5.0')"
# )

# conn.execute(f"""
#     COPY FROM DATABASE "{source_database}"
#     TO grafana_db
# """)

# conn.execute("DETACH grafana_db")

# print("Grafana-compatible snapshot created!")
