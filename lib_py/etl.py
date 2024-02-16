#!/usr/bin/env python
# coding: utf-8

# ETL

import os, sys
import pandas as pd
import requests, re
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from glob import glob

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from dwh import conn, main as create_dwh

STORAGE_PATH = os.path.join("output", "data-lake")
LOG_FOLDER = os.path.join("logs")
LOG_PATH = os.path.join(LOG_FOLDER, "logs.log")

if not os.path.exists(LOG_FOLDER):
    os.makedirs(LOG_FOLDER)

logging.basicConfig(filename=LOG_PATH, level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

FILTER_WORDS = re.compile(r'e[ -]?sport(s)?|e[ -]?gaming|gaming', re.IGNORECASE)
GAMES_LIST = ['Counter-Strike', 'CSGO', 'CS:GO', 'Counter-Strike-2', 'CS2', 'CS:2', 'League of Legends', 'Dota 2', 'Overwatch', 'Fortnite', 'Valorant', 'Hearthstone', 'PUBG', 'PlayerUnknown']

stopwords_url = "https://raw.githubusercontent.com/solariz/german_stopwords/master/german_stopwords_full.txt"
stopwords_list = requests.get(stopwords_url, allow_redirects=True).text.split("\n")[9:]

def convert_list_to_string(list):
    return " ".join(list)

def read_html_file(filename, encoding="utf-8"):
    with open(filename, "r", encoding=encoding) as f:
        text = f.read()
    return text

def process_html(text):
    words = text.replace("\n", " ").lower().split(" ")
    words = [i for i in words if len(i) > 1 and i not in stopwords_list]
    return words

def count_filter_words(words):
    filtered_words = [word for word in words if FILTER_WORDS.search(word)]
    count = pd.Series(filtered_words).value_counts().to_frame()
    count.columns = ["count"]
    count["word"] = count.index
    return count

def count_game_mentions(soup):
    game_mentions = {}

    text_nodes = soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "p"])
    for node in text_nodes:
        for game in GAMES_LIST:
            if game.lower() in node.text.lower():
                if game in game_mentions:
                    game_mentions[game] += 1
                else:
                    game_mentions[game] = 1

    games_count = []
    for game, count in game_mentions.items():
        games_count.append({
            "game": game,
            "count": count
        })

    df = pd.DataFrame(games_count)
    return df

def count_filter_word_pairs(words):
    word_counts = {}
    for i, filter_word in enumerate(words):
        if FILTER_WORDS.search(filter_word):
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
        if FILTER_WORDS.search(filter_word):
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

    df = pd.DataFrame(word_with_next_and_prev_ten, columns=["word", "prev_words", "next_words"])
    df["prev_words"] = df["prev_words"].apply(convert_list_to_string)
    df["next_words"] = df["next_words"].apply(convert_list_to_string)
    return df

def process_newspaper(newspaper):
    text = read_html_file(os.path.join(
        "output", newspaper["file_name"]), newspaper["encoding"].lower())
    soup = BeautifulSoup(text, "html.parser")
    bstext = soup.text
    words = process_html(bstext)

    result = {
        "count_filter": count_filter_words(words),
        "count_games": count_game_mentions(soup),
        "count_word_pair": count_filter_word_pairs(words),
        "word_with_prev_and_next_ten": get_word_with_prev_and_next_ten(words),
    }

    return result

def process_wrapper(newspaper):
    name = newspaper["name"]
    try:
        result = process_newspaper(newspaper)
        logging.info('Processing %s', name)
        return result
    except Exception as e:
        logging.error('Failt to process %s', name)
        logging.error(e)

def process_one_logfile(log_file_name=None):
    if not log_file_name:
        now_str = datetime.now().strftime("%Y-%m-%d")
        log_file_name = os.path.join(
            STORAGE_PATH,
            f"{now_str}.csv",
        )

    print("[INFO] Processing ", log_file_name)
    logging.info('Processing %s', log_file_name)

    log_file = pd.read_csv(log_file_name)

    for _, newspaper in log_file.iterrows():
        result = process_wrapper(newspaper)
        if result:
            for key in result:
                if type(result[key]).__name__ == "DataFrame":
                    result[key]["paper_id"] = newspaper["name"]
                    result[key]["date"] = newspaper["date"]
                    result[key].to_sql(key, conn, index=False, if_exists="append")

def process_all_logfiles():
    log_files = glob(os.path.join(STORAGE_PATH, "*.csv"))
    log_files = sorted(log_files)
    for log_file_name in log_files:
        process_one_logfile(log_file_name)

def main():
    create_dwh()
    process_all_logfiles()

if __name__ == "__main__":
    main()