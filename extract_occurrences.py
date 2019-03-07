#!/usr/bin/env python
# By: Volker Strobel
from datetime import datetime
import os
import re
import sys
import time

import backoff
from bs4 import BeautifulSoup
import docopt
import pandas as pd
import progressbar
import requests


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
def get_num_results(search_term, start_date, end_date, cookies=None):
    """
    Helper method, sends HTTP request and returns response payload.
    """

    # Open website and read html
    headers = {"user_agent": 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
    query_params = { 'q' : search_term, 'as_ylo' : start_date, 'as_yhi' : end_date, "as_vis": 1, "hl": "en", "as_sdt": "1,5"}
    url = "https://scholar.google.com/scholar"
    resp = requests.get(url, params=query_params, headers=headers, cookies=cookies)

    # Create soup for parsing HTML and extracting the relevant information
    soup = BeautifulSoup(resp.text, 'html.parser')
    div_results = soup.find("div", {"id": "gs_ab_md"}) # find line 'About x results (y sec)
    if not div_results:
        raise Exception(f"Couldn't extract number of results for {search_term} {start_date} {end_date} {resp.text}")

    res = re.findall(r'(\d+),?(\d+)?,?(\d+)?\s', div_results.text) # extract number of search results
    num_results = 0 if not res else int(''.join(res[0])) # convert string to number

    return num_results, resp.cookies


def get_range(search_terms, start_date, end_date, existing=None, output_filepath=None):

    results = existing if not existing.empty else pd.DataFrame(columns=["search_term", "year", "num_results"])
    existing_term_years = set((res[0], res[1]) for res in results.values.tolist())
    term_years = set((term, year) for term in search_terms
                     for year in range(start_date, end_date + 1)) - existing_term_years

    cookies = None
    for term, year in progressbar.progressbar(term_years):
        num_results, cookies = get_num_results(term, year, year, cookies=cookies)
        current_results = {"search_term": term, "year": year, "num_results": num_results}
        results = results.append(pd.DataFrame(data=[current_results]), sort=False)
        results.to_csv(output_filepath, index=False)
        time.sleep(2)


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
