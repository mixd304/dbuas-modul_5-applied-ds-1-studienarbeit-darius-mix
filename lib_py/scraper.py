#!/usr/bin/env python
# coding: utf-8

# Prepare scraping

import os
from datetime import datetime
import pandas as pd
import requests

SOURCES_PATH = os.path.join("input", "web-sources.csv")
STORAGE_PATH = os.path.join("output", "html")


def get_now_str():
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d")
    return now_str


def scrape_website(name, url, now_str=None):
    response = requests.get(url, allow_redirects=True)
    if not now_str:
        now_str = get_now_str()
    file_name = os.path.join(STORAGE_PATH, f"{now_str}-{name}.html")
    with open(file_name, "wb") as f:
        f.write(response.content)
    log_info = dict(
        name=name,
        date=now_str,
        file_name=file_name,
        status=response.status_code,
        original_url=url,
        final_url=response.url,
        encoding=response.encoding,
        error=False,
    )
    return log_info


def scrape_wrapper(newspaper, now_str=None):
    url = newspaper["url"]
    name = newspaper["id"]
    try:
        result = scrape_website(name, url, now_str)
        print(f"[INFO] Scraped {name} ({url})")
    except Exception as e:
        result = dict(
            original_url=url,
            name=name,
            error=True,
        )
        print(f"[ERROR] Failed to scrape: {name} ({url})")
        print(e)
    return pd.Series(result)


def main():
    web_sources = pd.read_csv(SOURCES_PATH)
    log_list = web_sources.apply(
        scrape_wrapper, axis=1, args=(get_now_str(), ))
    log_file_name = os.path.join(STORAGE_PATH, f"{get_now_str()}.csv")
    log_list.to_csv(log_file_name)


if __name__ == "__main__":
    main()