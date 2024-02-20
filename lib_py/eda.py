#!/usr/bin/env python
# coding: utf-8

# Explorative Analytics

import os, sys
import pandas as pd

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from dwh import conn

c = conn.cursor()

def get_events():
    query = ''' 
        SELECT event AS Event
	        ,game AS Game
	        ,month AS Month
        FROM DIM_Event
    '''
    return pd.read_sql_query(query, con=conn)

def get_filterwords_count_total_by_day():
    query = ''' 
        SELECT date as Day
	        ,word AS Word
	        ,sum(count) AS Count
        FROM count_filter
        GROUP BY date, word
    '''
    return pd.read_sql_query(query, con=conn)

def get_filterwords_count_total_by_month():
    query = ''' 
        SELECT substr(date,0,8) as Month
	        ,word AS Word
	        ,sum(count) AS Count
        FROM count_filter
        GROUP BY substr(date,0,8), word
    '''
    return pd.read_sql_query(query, con=conn)

def get_filterwords_count_total_by_paper():
    query = ''' 
        SELECT count_filter.word AS Word
            ,DIM_Paper.name AS Paper
            ,sum(count_filter.count) AS Count
        FROM count_filter
		LEFT JOIN DIM_Paper on count_filter.paper_id = DIM_Paper.id
        GROUP BY count_filter.word, DIM_Paper.name
    '''
    return pd.read_sql_query(query, con=conn)

def get_filterwords_count_total_by_paper_group():
    query = ''' 
        SELECT count_filter.word AS Word
            ,DIM_Paper.Format AS Gruppe
            ,sum(count_filter.count) AS Count
        FROM count_filter
        LEFT JOIN DIM_Paper on count_filter.paper_id = DIM_Paper.id
        GROUP BY count_filter.word, DIM_Paper.Format
    '''
    return pd.read_sql_query(query, con=conn)

def get_filterwords_count_total_by_paper_group_and_month():
    query = ''' 
        SELECT substr(count_filter.date,0,8) AS Month
            ,count_filter.word AS Word
            ,DIM_Paper.Format AS Gruppe
            ,sum(count_filter.count) AS Count
        FROM count_filter
        LEFT JOIN DIM_Paper on count_filter.paper_id = DIM_Paper.id
        GROUP BY substr(count_filter.date,0,8), count_filter.word, DIM_Paper.Format
    '''
    return pd.read_sql_query(query, con=conn)

def get_filterwords_word_pair_count_total():
    query = ''' 
        SELECT filter_word
            ,other_word
            ,sum(count) AS Count
        FROM count_word_pair
        GROUP BY filter_word, other_word
        ORDER BY sum(count) DESC
    '''
    return pd.read_sql_query(query, con=conn)

def get_games_count_total():
    query = ''' 
        SELECT sum(count) as Count
            ,game as Game
        FROM count_games
        GROUP BY game
    '''
    return pd.read_sql_query(query, con=conn)

def get_games_count_total_by_month():
    query = ''' 
        SELECT substr(date,0,8) as Month
	        ,game as Game
	        ,sum(count) as Count
        FROM count_games
        GROUP BY substr(date,0,8), game
    '''
    return pd.read_sql_query(query, con=conn)

def get_games_count_by_paper():
    query = ''' 
        SELECT count_games.game AS Game
            ,DIM_Paper.name AS Paper
            ,sum(count_games.count) AS Count
        FROM count_games
        LEFT JOIN DIM_Paper on count_games.paper_id = DIM_Paper.id
        GROUP BY count_games.game, DIM_Paper.name
    '''
    return pd.read_sql_query(query, con=conn)

def get_games_count_by_paper_group():
    query = ''' 
        SELECT count_games.game AS Game
            ,DIM_Paper.Format AS Gruppe
            ,sum(count_games.count) AS Count
        FROM count_games
        LEFT JOIN DIM_Paper on count_games.paper_id = DIM_Paper.id
        GROUP BY count_games.game, DIM_Paper.Format
    '''
    return pd.read_sql_query(query, con=conn)

def get_games_count_by_paper_group_and_month():
    query = ''' 
        SELECT substr(count_games.date,0,8) as Month
            ,count_games.game AS Game
            ,DIM_Paper.Format AS Gruppe
            ,sum(count_games.count) AS Count
        FROM count_games
        LEFT JOIN DIM_Paper on count_games.paper_id = DIM_Paper.id
        GROUP BY substr(count_games.date,0,8), count_games.game, DIM_Paper.Format
    '''
    return pd.read_sql_query(query, con=conn)

def get_unique_filter_words():
    query = '''SELECT DISTINCT word FROM count_filter'''
    return pd.read_sql_query(query, con=conn)

def delete_filter_words(word_list):
    query = "DELETE FROM count_filter WHERE " + \
        " OR ".join(["word LIKE '%{}%'".format(word) for word in word_list])
    print(query)
    c.execute(query)

    query = "DELETE FROM count_word_pair WHERE " + \
        " OR ".join(["filter_word LIKE '%{}%'".format(word) for word in word_list])
    print(query)
    c.execute(query)

    query = "DELETE FROM word_with_prev_and_next_ten WHERE " + \
        " OR ".join(["word LIKE '%{}%'".format(word) for word in word_list])
    print(query)
    c.execute(query)

    conn.commit()

def update_filter_words(new_word, word_list):
    query = "UPDATE count_filter SET word = '{}' WHERE ".format(new_word) + \
        " OR ".join(["word LIKE '%{}%'".format(word) for word in word_list])
    print(query)
    c.execute(query)

    query = "UPDATE count_word_pair SET filter_word = '{}' WHERE ".format(new_word) + \
        " OR ".join(["filter_word LIKE '%{}%'".format(word) for word in word_list])
    print(query)
    c.execute(query)

    query = "UPDATE word_with_prev_and_next_ten SET word = '{}' WHERE ".format(new_word) + \
        " OR ".join(["word LIKE '%{}%'".format(word) for word in word_list])
    print(query)
    c.execute(query)

    conn.commit()

def update_games(new_word, games_list):
    query = "UPDATE count_games SET game = '{}' WHERE ".format(new_word) + \
        " OR ".join(["game LIKE '%{}%'".format(game) for game in games_list])
    print(query)
    c.execute(query)
    conn.commit()