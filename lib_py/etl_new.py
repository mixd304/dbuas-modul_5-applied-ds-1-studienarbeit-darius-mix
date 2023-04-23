#!/usr/bin/env python
# coding: utf-8

# Data Warehouse

import os
from datetime import datetime, timedelta
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import requests
import logging
from glob import glob

STORAGE_PATH = os.path.join("output", "data-lake")
SQL_PATH = os.path.join("output", "dwh", "dwh.sqlite3")
LOG_FOLDER = os.path.join("logs")

if not os.path.exists(LOG_FOLDER):
    os.makedirs(LOG_FOLDER)

log_file = os.path.join(LOG_FOLDER, "logs.log")
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

FILTER_WORDS = ['esports', 'e-sports', 'esport', 'e-sport', 'egaming', 'e-gaming', 'gaming', 'cs', 'csgo', 'cs:go', 'counter-strike', 'lol',
                'league', 'dota', 'dota2', 'valorant', 'fortnite', 'overwatch', 'hearthstone', 'pubg', 'playerunknown', 'starcraft', 'sc2', 'fifa']

stopwords_url = "https://raw.githubusercontent.com/solariz/german_stopwords/master/german_stopwords_full.txt"
stopwords_list = requests.get(stopwords_url, allow_redirects=True).text.split("\n")[9:]

def read_html_file(filename, encoding="utf-8"):
    with open(filename, "r", encoding=encoding) as f:
        text = f.read()
    return text

def process_html(text):
    words = text.replace("\n", " ").lower().split(" ")
    words = [i for i in words if len(i) > 1 and i not in stopwords_list]
    return words

def process_newspaper(newspaper):
    text = read_html_file(os.path.join("output", newspaper["file_name"]), newspaper["encoding"].lower())
    bstext = BeautifulSoup(text, "html.parser").text
    words = process_html(bstext)

    count_all = count_words(words)
    count_filter = count_filter_words(words)
    count_word_pair = count_filter_words_combinations(words)
    word_with_prev_and_next_ten = get_word_with_prev_and_next_ten(words)

    count_all["paper"] = newspaper["name"]
    count_all["date"] = newspaper["date"]
    count_filter["paper"] = newspaper["name"]
    count_filter["date"] = newspaper["date"]
    count_word_pair["paper"] = newspaper["name"]
    count_word_pair["date"] = newspaper["date"]
    word_with_prev_and_next_ten["paper"] = newspaper["name"]
    word_with_prev_and_next_ten["date"] = newspaper["date"]

    return count_all, count_filter, count_word_pair, word_with_prev_and_next_ten

def count_words(words):
    count = pd.Series(words).value_counts().to_frame()
    count.columns = ["count"]
    count["word"] = count.index
    return count

def count_filter_words(words):
    filtered_words = [word for word in words if word in FILTER_WORDS]
    count = pd.Series(filtered_words).value_counts().to_frame()
    count.columns = ["count"]
    count["word"] = count.index
    return count

def count_filter_words_combinations(words):
    word_counts = {}
    for i, filter_word in enumerate(words):
        if filter_word in FILTER_WORDS:
            for j in range(i-6, i+6):
                if j < len(words) and j >= 0 and i != j:
                    other_word = words[j]
                    if other_word != filter_word:
                        word_pair = tuple([filter_word, other_word])
                        if word_pair in word_counts:
                            word_counts[word_pair] += 1
                        else:
                            word_counts[word_pair] = 1

    word_pair_counts = []
    for pair, count in word_counts.items():
        word_pair_counts.append({
            "filter_word": pair[0],
            "other_word": pair[1],
            "count": count
        })

    word_pair_counts_df = pd.DataFrame(word_pair_counts)
    return word_pair_counts_df


def get_word_with_prev_and_next_ten(words):
    word_with_next_and_prev_ten = []
    for i, filter_word in enumerate(words):
        if filter_word in FILTER_WORDS:
            prev_words = []
            next_words = []
            for j in range(i+1, i+6):
                if j < len(words):
                    next_words.append(words[j])
            for j in range(i-6, i-1):
                if j >= 0:
                    prev_words.append(words[j])
            word_with_next_and_prev_ten.append({
                "word": filter_word,
                "prev_words": prev_words,
                "next_words": next_words
            })

    df = pd.DataFrame(word_with_next_and_prev_ten)
    return df


def process_wrapper(newspaper):
    name = newspaper["name"]
    try:
        result = process_newspaper(newspaper)
        logging.info('Processing %s', name)
        return result
    except Exception as e:
        logging.error('Failt to process %s', name)
        logging.error(e)
        print(f"[ERROR] Failt to process {name}")
        print(e)


def convert_to_string(list):
    return " ".join(list)


def process_one_logfile(log_file_name=None):
    
    collection_count_all = []
    collection_count_filter = []
    collection_count_word_pair = []
    collection_word_with_prev_and_next_ten = []
    
    if not log_file_name:
        now_str = datetime.now().strftime("%Y-%m-%d")
        log_file_name = os.path.join(
            STORAGE_PATH,
            f"{now_str}.csv",
        )

    print("[INFO] Processing ", log_file_name)
    logging.info('Processing %s', log_file_name)
    log_file = pd.read_csv(log_file_name)

    for i, newspaper in log_file.iterrows():
        result = process_wrapper(newspaper)
        if result:
            if type(result[0]).__name__ == "DataFrame":
                collection_count_all.append(result[0])
            if type(result[1]).__name__ == "DataFrame":
                collection_count_filter.append(result[1])
            if type(result[2]).__name__ == "DataFrame":
                collection_count_word_pair.append(result[2])
            if type(result[3]).__name__ == "DataFrame":
                collection_word_with_prev_and_next_ten.append(result[3])
    
    data_count_all = pd.concat(collection_count_all, axis=0)
    data_count_filter = pd.concat(collection_count_filter, axis=0)
    data_count_word_pair = pd.concat(collection_count_word_pair, axis=0)
    data_word_with_prev_and_next_ten = pd.concat(collection_word_with_prev_and_next_ten, axis=0)

    data_word_with_prev_and_next_ten['prev_words'] = data_word_with_prev_and_next_ten['prev_words'].apply(convert_to_string)
    data_word_with_prev_and_next_ten['next_words'] = data_word_with_prev_and_next_ten['next_words'].apply(convert_to_string)

    connection = sqlite3.connect(SQL_PATH)
    data_count_all.to_sql("count_all", connection, index=False, if_exists="append")
    data_count_filter.to_sql("count_filter", connection, index=False, if_exists="append")
    data_count_word_pair.to_sql("count_word_pair", connection, index=False, if_exists="append")
    data_word_with_prev_and_next_ten.to_sql("word_with_prev_and_next_ten", connection, index=False, if_exists="append")

    log_file.head()


def main():
    log_files = glob(os.path.join(STORAGE_PATH, "*.csv"))
    log_files = sorted(log_files)
    for log_file_name in log_files:
        if '2023' in log_file_name:
            process_one_logfile(log_file_name)
        #elif '2022' in log_file_name:
        #    process_one_logfile(log_file_name)


if __name__ == "__main__":
    main()