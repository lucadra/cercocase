#This file may not be useful to the reader. I was playing around with ML and needed a way to format the dataset.

import math
from scipy import stats
import pandas as pd
import numpy as np
from traveltime_api_caller import call_traveltime_api

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# Select Rows

df = pd.read_csv('data_sales.csv', low_memory=False)

rows_selection = df.loc[(df['category.id'] == 1) & (df['typology.id'] == 14)]

df_subset = rows_selection.loc[:, ['bathrooms', 'rooms', 'surface', 'floor.abbreviation',
                                   'location.latitude', 'location.longitude', 'price.value']]

df_subset = df_subset.dropna()

df_subset['surface'] = df_subset['surface'].replace({'m²': ''}, regex=True).fillna(0).map(
    lambda x: x.replace('.', '') if isinstance(x, str) else x).map(lambda x: int(x))
df_subset['floor.abbreviation'] = df_subset['floor.abbreviation'].replace({' - ': ',', 'S': '0', 'T': '0', 'R': '0'},
                                                                          regex=True).map(
    lambda x: max(int(i) for i in x.split(',')))
df_subset['bathrooms'] = df_subset['bathrooms'].map(lambda x: x.strip('+')).map(lambda x: int(x))
df_subset['rooms'] = df_subset['rooms'].map(lambda x: x.strip('+')).map(lambda x: int(x))
df_subset['price.value'] = df_subset['price.value'].map(lambda x: int(x))
df_subset = pd.DataFrame(df_subset)
df_subset = df_subset[(np.abs(stats.zscore(df_subset)) < 2.75).all(axis=1)]


def get_coordinates(df):
    return [{"id": str(index), "coords": {"lat": row['location.latitude'], "lng": row['location.longitude']}} for
            index, row in df.iterrows()]


duomo = {"id": 'arrival', "coords": {"lat": 45.464195, "lng": 9.189481}}
navigli = {"id": 'arrival', "coords": {"lat": 45.450717, "lng": 9.169698}}
garibaldi = {"id": 'arrival', "coords": {"lat": 45.483239, "lng": 9.187928}}

landmarks = [duomo, navigli, garibaldi]

departures = get_coordinates(df_subset)
for landmark in landmarks:
    for chunk in chunks(departures, 1999):
        coordinates = chunk
        coordinates.append(landmark)
        result = call_traveltime_api(coordinates)
        traveltime_array = result['results'][0]['locations']
        for i, traveltime in enumerate(traveltime_array):
            df_subset.loc[int(traveltime['id']), f'time_to_landmark{i}'] = traveltime['properties'][0]['travel_time']
        print(f'Done with {landmark["id"]}')
stats = df_subset.describe()

df_subset.to_csv('training_data.csv', index=False, encoding='utf-8')
print(df)
