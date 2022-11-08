import json
import os
from pprint import pprint
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from downloader import get_neighbourhoods_df, CITY_ID


def json_to_csv(file_path, _neighbourhood_data):
    with open(file_path) as f:
        results = json.load(f)['results']
        macrozone_id = file_path.split('_')[-3]
        neighbourhood_id = file_path.split('_')[-2]
        macrozone_name = _neighbourhood_data.loc[_neighbourhood_data['macrozone_id'] == macrozone_id].iloc[0][
            'macrozone_name']
        neighbourhood_name = _neighbourhood_data.loc[_neighbourhood_data['neighbourhood_id'] == neighbourhood_id][
            'neighbourhood_name'].values[0]
        dfs = []
        for result in results:
            data = result['realEstate']
            title = data['title']
            listing_id = data['id']
            contract = data['contract']
            properties = pd.json_normalize(data['properties'])
            advertiser = pd.json_normalize(data['advertiser'])
            row = [pd.DataFrame({'id': listing_id, 'title': title, 'contract': contract,
                                 'macrozone': macrozone_name, 'neighbourhood': neighbourhood_name}, index=[0]),
                   properties, advertiser]
            df_row = pd.concat(row, axis=1)

            dfs.append(df_row)
    return pd.concat(dfs)


def batch_process_jsons(in_dir):
    print('Getting neighbourhood data...')
    neighbourhood_data = get_neighbourhoods_df(CITY_ID)
    dfs = []
    print('Joining JSON files...')
    for file_path in tqdm(os.listdir(in_dir)):
        input_path = os.path.join(in_dir, file_path)
        dfs.append(json_to_csv(input_path, neighbourhood_data))
    return pd.concat(dfs)


batch_process_jsons('json_data_sales').drop_duplicates('id').to_csv('data_sales.csv', index=False, encoding='utf-8')
