#!/usr/bin/env python
# By: Volker Strobel
from datetime import datetime
import multiprocessing as mp
import threading as thr
import os
import random
import re
import sys
import time
import urllib
import queue

from selenium import webdriver
import backoff
from bs4 import BeautifulSoup
import docopt
import pandas as pd
import progressbar
import requests
import requests.auth


__doc__ = """Extract Occurrences.

Usage:
  extract_occurrences.py (--term=<term> | --terms-filepath=<terms-filepath>)
                         (--start-year=<start-year>)
                         [--end-year=<end-year>]
                         [--output-filepath=<output-filepath>]
  extract_occurrences.py (-h | --help)

Options:
  -h --help                              Show this screen.
  --term=<term>                          Term to search for.
  --terms-filepath=<terms-filepath>      Filepath with list of terms to search for.
  --start-year=<start-year>              Start year.
  --end-year=<end-year>                  End year; if not specified, defaults to the current year.
  --output-filepath=<output-filepath>    Filepath to save results to [default: out.csv].
"""


@backoff.on_exception(backoff.expo, Exception, max_time=10)
def get_num_results(search_term, start_date, end_date, results_q):
    """
    Helper method, sends HTTP request and returns response payload.
    """

    auth = requests.auth.HTTPProxyAuth('4f65916665ca4f597997d004b2931e11', '590af9555d67a0291afbfff63501d85e')
    proxy = get_proxy()
    proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}

    # Open website and read html
    headers = {"User-Agent": 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
    query_params = { 'q' : search_term, 'as_ylo' : start_date, 'as_yhi' : end_date, "as_vis": 1, "hl": "en"}
    url = "https://scholar.google.com/scholar?as_sdt=1,5&"
    # url = "https://scholar.google.com/scholar?" + '&'.join(f"{key}={str(val)}" for key, val in query_params.items())
    # url = "http://zend2.com/bro.php?b=12&f=norefer&u=" + urllib.parse.quote(url)
    # driver = webdriver.Chrome()
    # driver.get(url)
    # resp = requests.get(url, params=query_params, headers=headers, cookies=cookies, proxies=proxies)
    resp = requests.get(url, params=query_params, headers=headers, proxies=proxies, auth=auth, timeout=5)

    # Create soup for parsing HTML and extracting the relevant information
    soup = BeautifulSoup(resp.text, 'html.parser')
    # soup = BeautifulSoup(driver.page_source, 'html.parser')
    div_results = soup.find("div", {"id": "gs_ab_md"}) # find line 'About x results (y sec)
    if not div_results:
        raise Exception(f"Couldn't extract number of results for {search_term} {start_date} {end_date} {resp.text}")
        # raise Exception(f"Couldn't extract number of results for {search_term} {start_date} {end_date} {driver.page_source}")

    res = re.findall(r'(\d+),?(\d+)?,?(\d+)?\s', div_results.text) # extract number of search results()
    num_results = 0 if not res else int(''.join(res[0])) # convert string to number

    return num_results


def get_proxy():
    url = 'http://falcon.proxyrotator.com:51337/'
    params = dict(
        apiKey='43XQbFJeWvhLpMqx5awTyY8RZu9PE2KV'
    )
    resp = requests.get(url=url, params=params)
    return resp.json()['proxy']


def get_range(search_terms, start_date, end_date, existing=None, output_filepath=None):

    results = existing if existing is not None and not existing.empty else pd.DataFrame(columns=["search_term", "year", "num_results"])
    existing_term_years = set((res[0], res[1]) for res in results.values.tolist())
    term_years = set((term, year) for term in search_terms
                     for year in range(start_date, end_date + 1)) - existing_term_years

    for term, year in progressbar.progressbar(term_years):
        num_results = get_num_results(term, year, year, None)
        results = results.append(pd.DataFrame(data=[{"search_term": term, "year": year, "num_results": num_results}]), sort=False)
        results.to_csv(output_filepath, index=False)


def main():
    args = docopt.docopt(__doc__)
    if args["--term"]:
        terms = [args["--term"]]
    elif args["--terms-filepath"]:
        terms = [l.strip() for l in open(args["--terms-filepath"]).readlines()]
    else:
        pass
    end_year = int(args["--end-year"]) if args["--end-year"] else datetime.now().year

    if os.path.isfile(args["--output-filepath"]):
        existing = pd.read_csv(args["--output-filepath"])
    else:
        existing = None

    get_range(terms, int(args["--start-year"]), end_year, existing=existing, output_filepath=args["--output-filepath"])



if __name__ == "__main__":
    exit(main())
