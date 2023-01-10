import duckdb


conn = duckdb.connect("data_warehouse/memory_warehouse.duckdb")

cursor = conn.cursor()

print(cursor.execute("SHOW TABLES; ").fetchall())

