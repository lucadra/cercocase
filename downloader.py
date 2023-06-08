import json
import requests
import itertools
import pandas as pd
from pprint import pprint
from tqdm import tqdm, trange
from itertools import product
from concurrent.futures import ThreadPoolExecutor

# TODO: fix hardcoded values for contratto and categoria

CITY_ID = 8042


# Immobiliare.it does not show us all the listings in a given city just by searching for it: only ~2k listings are shown
# of the supposed ~37k the website reports. This is probably done to avoid scraping. For this reason we need to make
# a number of specific queries and join the results together. We could do it by number of rooms, bathrooms, etc..

# A simple way to do it is to call the API on the following URL:
# "https://www.immobiliare.it/search/macrozones?id=8042&type=3" | 8042 is Milan's ID, 'type' function is unknown
# The API answer with a *.JSON file containing all the 'macrozone' and 'neighbourhoods' in the city,
# together with their corresponding IDs.

def get_neighbourhoods_df(_city_id):
    print("Getting neighbourhoods list...")
    response = requests.get(f'https://www.immobiliare.it/search/macrozones?id={_city_id}&type=3')
    data = response.json()
    city_name = data['label']
    city_id = data['id']
    province_name = data['parents'][0]['label']
    province_id = data['parents'][0]['id']
    region_name = data['parents'][1]['label']
    region_id = data['parents'][1]['id']
    rows = []
    for macrozone in data['macrozones']:
        macrozone_name = macrozone['label']
        macrozone_id = macrozone['id']
        for neighbourhood in macrozone['children']:
            neighbourhood_name = neighbourhood['label']
            neighbourhood_id = neighbourhood['id']
            rows.append({'region_name': region_name,
                         'region_id': region_id,
                         'province_name': province_name,
                         'province_id': province_id,
                         'city_name': city_name,
                         'city_id': city_id,
                         'macrozone_name': macrozone_name,
                         'macrozone_id': macrozone_id,
                         'neighbourhood_name': neighbourhood_name,
                         'neighbourhood_id': neighbourhood_id})
    return pd.DataFrame(rows)


# This is the url to call to get the listings:
# https://www.immobiliare.it/api-next/search-list/real-estates/?fkRegione=lom&idProvincia=MI&idComune=8042&idMZona[0]=10061&idQuartiere[0]=12809&__lang=it&idContratto=2&idCategoria=1&pag=1&paramsCount=0&path=%2F

# Calling the API substituting the IDs is enough to get a json containing the first page of results (24 listings/page)
# ATTENTION: the API gives the same response for page 0 and page 1

# ========= LEGEND =========
# idContratto: 1 = SALES, 2 = RENTALS, 14 = AUCTIONS
# idCategoria: 1 = All Houses, ...(others still unmapped)

def call_API(_region_id, _province_id, _city_id, _macrozone_id, _neighbourhood_id, _contract_id, _category_id,
             _page_num):
    payload = {'fkRegione': _region_id,
               'idProvincia': _province_id,
               'idComune': _city_id,
               'idMZona[0]': _macrozone_id,
               'idQuartiere[0]': _neighbourhood_id,
               'idContratto': _contract_id,
               'idCategoria': _category_id,
               'pag': _page_num,
               'paramsCount': 1,
               'path': '%2F'}

    return requests.get('https://www.immobiliare.it/api-next/search-list/real-estates/', params=payload)


# I will call the API on every neighbourhood and parse the number of pages that neighbourhood gives us.
# I will then assemble an array with all the 'macrozone'-'neighbourhood'-'page' indexes and pass that to a
# thread pool to send requests in parallel. This speeds things up considerably.

def probe_neighbourhoods(df, _contract_id, _category_id):
    print('Probing neighbourhoods...')
    for idx, row in tqdm(df.iterrows(), total=df.shape[0]):
        response = call_API(row['region_id'], row['province_id'], row['city_id'], row['macrozone_id'],
                            row['neighbourhood_id'], _contract_id, _category_id, 0)
        pages_number = response.json()['maxPages']
        df.loc[idx, 'pages'] = pages_number
    return df


# Now I need to build the array I'll use to send the requests

def build_indexes(df):
    print('Building Indexes...')
    df_indexes = []
    for idx, row in df.iterrows():
        row_indexes = list(itertools.product([int(row['macrozone_id'])], [int(row['neighbourhood_id'])],
                                             range(1, int(row['pages'] + 1))))
        df_indexes.extend(iter(row_indexes))
    return df_indexes


# Now I can use multithreading to send the requests and save responses as *.JSON for further processing
# Region, province and city IDs are coded as to avoid passing further args to the function, will have to
# fix sometimes in the future.
# TODO: fix hardcoded values in downloader.multithread_api_call

def multithread_api_call(index):
    response = call_API('lom', 'MI', CITY_ID, index[0], index[1], 2, 1, index[2])
    with open(f'json_data_rentals/json_data_{index[0]}_{index[1]}_{index[2]}.json', 'w') as outfile:
        json.dump(response.json(), outfile)
    return True


def multithread_api_caller():
    neighbourhoods_data = get_neighbourhoods_df(CITY_ID)
    pages_data = probe_neighbourhoods(neighbourhoods_data, 2, 1)
    threading_indexes = build_indexes(pages_data)
    print('Downloading data...')
    with ThreadPoolExecutor() as t:
        t.map(multithread_api_call, threading_indexes)

multithread_api_caller()
