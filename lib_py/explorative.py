import sqlite3
import os, sys
import pandas as pd

sys.path.insert(0, os.path.abspath(".."))

SQL_PATH = os.path.join("output", "dwh", "dwh.sqlite3")
print(SQL_PATH)

con = sqlite3.connect(SQL_PATH)

df = pd.read_sql_query(''' 
    SELECT 
        * 
    FROM count_games
''', con = con)

print(df)