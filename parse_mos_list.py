import pandas as pd
import os
import requests
import sys
import time
from tqdm import tqdm
from bs4 import BeautifulSoup

BASE_URL = 'http://www.mosdb.com'

URLS = {
    'Army': 'http://www.mosdb.com/army/',
    'Air Force': 'http://www.mosdb.com/air-force/',
    'Navy': 'http://www.mosdb.com/navy/',
    'Marines': 'http://www.mosdb.com/marine-corps/',
    'Coast Guard': 'http://www.mosdb.com/coast-guard/'
}


def parse_table(branch):
    resp = requests.get(URLS[branch])
    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table')
    rows = table.find_all('tr')
    data = []
    for row in rows[1:]:
        cols = row.find_all('td')
        data.append({
            'branch': branch,
            'MOS': cols[0].text,
            'Name': cols[1].text,
            'Rank': cols[2].text,
            'URL': cols[0].find('a')['href'],
            'Description': ''
        })
    return pd.DataFrame(data)


def save_table_data(path, branch):
    data = parse_table(branch)

    if os.path.exists(path):
        mos_list = pd.read_json(path, lines=True)
        pd.concat([mos_list, data]).to_json(path, orient='records', lines=True)
    else:
        data.to_json(path, orient='records', lines=True)


def parse_description(url):
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, 'html.parser')
    desc = soup.find('div', {'class': 'desc'})
    return desc.text


def parse_descriptions(path, rescrape=False):
    mos_list = pd.read_json(path, lines=True)

    for _, item in tqdm(mos_list.iterrows()):
        if item['Description'] == '' or rescrape:
            url = BASE_URL + item['URL']
            item['Description'] = parse_description(url)

            mos_list.to_json(path, orient='records', lines=True)
            time.sleep(1)


if __name__ == '__main__':
    path = 'mos_list.jsonl'

    if sys.argv[1] == 'table':
        for branch in URLS:
            save_table_data(path, branch)

    if sys.argv[1] == 'desc':
        parse_descriptions(path)
