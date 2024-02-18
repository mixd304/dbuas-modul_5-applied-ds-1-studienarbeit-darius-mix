#!/usr/bin/env python
# coding: utf-8

# Data Warehouse

import os
import sys
import sqlite3
import pandas as pd

SQL_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "output", "dwh", "dwh.sqlite3")

conn = sqlite3.connect(SQL_PATH)
c = conn.cursor()

def create_etl_tables():
    # Tabelle erstellen, falls sie nicht bereits existiert
    c.execute('''CREATE TABLE IF NOT EXISTS count_filter
                (date TEXT,
                paper_id TEXT,
                word TEXT,
                count INTEGER
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS count_games
                (date TEXT,
                paper_id TEXT,
                game TEXT,
                count INTEGER
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS count_word_pair
                (date TEXT,
                paper_id TEXT,
                filter_word TEXT,
                other_word TEXT,
                count INTEGER
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS word_with_prev_and_next_ten
                (date TEXT,
                paper_id TEXT,
                word TEXT,
                prev_words TEXT,
                next_words TEXT
                )''')

def create_dim_paper():
    c.execute('''CREATE TABLE IF NOT EXISTS DIM_Paper
                (id TEXT PRIMARY KEY,
                name TEXT,
                url TEXT,
                format TEXT,
                sprache TEXT
                )''')

def fill_dim_paper():
    papers = pd.read_csv(os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "input/web-sources.csv"))
    papers.to_sql("DIM_Paper", conn, index=False, if_exists="replace")

def create_dim_event():
    c.execute('''CREATE TABLE IF NOT EXISTS DIM_Event
                (event TEXT PRIMARY KEY,
                game TEXT,
                month TEXT,
                source_link TEXT
                )''')

def fill_dim_event():
    papers = pd.read_csv(os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "input/esports-events.csv"))
    papers.to_sql("DIM_Event", conn, index=False, if_exists="replace")

def main():
    create_dim_paper()
    create_etl_tables()
    fill_dim_paper()

if __name__ == "__main__":
    main()