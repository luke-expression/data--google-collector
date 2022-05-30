import pandas as pd
import numpy as np
import json
import os
from tqdm import tqdm
import glob
import time
import requests
import time
from pathlib import Path


class GeoCollector:
    def __init__(self):
        self._new_district = ['neighborhood', 'sublocality', 'locality']
        self._url = 'https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'
        self._api_key = os.environ['GEO_API_KEY']
        self._data = []


    def collect_geos(self, path, origin='SF', consider_old=True, id_col=False, name_col=False, street_col=False,
                     postcode_col=False):

        data = pd.read_csv(path)

        if origin == 'SF':
            # Read data and format for use.
            data = data[['Premises: Premises ID', 'Premises: Premises Name', 'Premises: Street', 'Postcode',
                         'Premises: City']].rename(
                columns={'Premises: Premises ID': 'id', 'Premises: Premises Name': 'name', 'Premises: Street': 'street',
                         'Postcode': 'postcode', 'Premises: City': 'town'}).dropna(subset=['id', 'street', 'postcode'])

            # HASH OUT TO DO FULL DATA
            #data = data.sample(50)

            data['street+pc'] = data[['street', 'postcode']].apply(lambda x: (x[0] + ', ' + x[1]).replace(' ', '+'),
                                                                   axis=1)

        else:  # data not from SalesForce

            # Read data and format for use.
            data = data[[id_col, name_col, street_col, postcode_col]].rename(
                columns={id_col: 'id', name_col: 'name', street_col: 'street',
                         postcode_col: 'postcode'}).dropna(subset=['id', 'street', 'postcode'])

            ### data = data.sample(5)  # For testing

            data['street+pc'] = data[['street', 'postcode']].apply(lambda x: (x[0] + ', ' + x[1]).replace(' ', '+'),
                                                                   axis=1)

        if consider_old:
            old = pd.read_csv('new_districts.csv')
            data = data[~data['id'].isin(list(old['PREM - ID']))]

        Path('places').mkdir(parents=True, exist_ok=True)
        print('Ready to collect geo data for {} places!'.format(len(data['street+pc'])))
        time.sleep(1)
        for i, add in tqdm(enumerate(data['street+pc'])):
            try:
                res = requests.get(self._url.format(add, self._api_key)).json()['results'][0]
                dict1 = {'id': list(data['id'])[i]}
                for item in res['address_components']:
                    index = 0
                    if item['types'][index] == 'political':
                        index += 1
                    dict1[item['types'][index]] = item['short_name']
                    #print(item)

                dict1['lat'] = res["geometry"]["location"]["lat"]
                dict1['long'] = res["geometry"]["location"]["lng"]
                dict1["place_id"] = res["place_id"]
            except IndexError:
                print('Index error for', add)
                continue

            # Save dictionary to JSON in case of a Crash.
            if 'postal_code' in dict1.keys():
                file_string = 'places/place_{}_{}.json'.format(dict1['id'], dict1['postal_code'])
            else:
                file_string = 'places/place_{}_no_pc.json'.format(dict1['id'])
            with open(file_string, 'w') as file:
                json.dump(dict1, file)

            # print('\n')
            # for item in dict1.items():
                # print(item)
            # print('\n')


    def find_new_district(self, out_path):
        print('\nFinding new districts:')

        in_file_path = 'places//*.json'
        districts = {}
        new = 0
        total = 0
        for file in tqdm(glob.glob(in_file_path)):
            dict1 = json.load(open(file))
            try:
                if dict1['country'] == 'GB':
                    found = 0
                    for val in self._new_district:
                        if val in dict1.keys():
                            districts[dict1['id']] = dict1[val]
                            found = 1
                            new += 1
                            total += 1
                            break
                    if found == 0:
                        if 'postal_town' in dict1.keys():
                            districts[dict1['id']] = dict1['postal_town']
                            total += 1
                    else:
                        continue
            except KeyError:
                print('Key error for\n', dict1)
                continue

        print(new, 'New disrticts found out of', total)
        print(str(round((new/total)*100, 2))+'%')

        # Save results to CSV.
        df = pd.DataFrame.from_dict(districts, orient='index')
        df = df.reset_index()
        df = df.rename(columns={'index': 'PREM - ID', 0: 'AREA'})
        df.to_csv(out_path, index=False)


    @staticmethod
    def return_coordinates(out_path):

        print('\nReturning new lon & lat:')

        in_file_path = 'places//*.json'
        coords = {}
        total = 0
        for file in tqdm(glob.glob(in_file_path)):
            dict1 = json.load(open(file))
            ID, lon, lat = dict1['id'], dict1['long'], dict1['lat']
            coords[ID] = [lon, lat]

        df = pd.DataFrame.from_dict(coords, orient='index')
        df = df.reset_index()
        df = df.rename(columns={'index': 'PREM - ID', 0: 'lon', 1: 'lat'})
        df.to_csv(out_path, index=False)
