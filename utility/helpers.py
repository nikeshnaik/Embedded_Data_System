import sqlite3
import json
import duckdb


class SQLiteConn:

    def __init__(self, db_path):

        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.cursor.close()
        self.conn.commit()



def movie_review_generator(json_path):

    for each in json.load(open(json_path)):

        yield each



class DuckDBConn:

    def __init__(self, db_path):

        self.conn = duckdb.connect(database=db_path, read_only=False)
        self.cursor = self.conn.cursor()

    def __enter__(self):
        return self.cursor

    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.cursor.commit()
        self.conn.close()