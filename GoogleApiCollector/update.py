import glob
import json
import os
import requests
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from utils.utils import get_address_elements_from_google_json, get_time_element, check_you_want_to, \
    get_rating_from_fsa_request, get_keywords, flag


class GoogleDataUpdater:

    """

    A class used for de-duplicating new Google data against an existing database (SalesForce), and taking any old records
    that were not found in the new data to update.

    TO DO:
    - Test that Google pulls correct data and all fields needed.
    - Edit tabulate to include extra fields.

    """

    def __init__(self, chain_threshold=5):

        self.base_path = 'https://maps.googleapis.com/maps/api/place/details/json?'
        self.params = 'place_id={id}&fields={fields}&key={api_key}'

        self.fields = [
            'place_id',
            'name',
            'rating',
            'user_ratings_total',
            'reviews',
            'business_status',
            'address_component',
            'adr_address',
            'formatted_address',
            'geometry',
            'url',
            'vicinity',
            'review',
            'formatted_phone_number',
            'international_phone_number',
            'opening_hours',
            'website',
            'types'
        ]

        self.field_string = ','.join(self.fields)

        self.ids_to_query = []
        self.path_to_master = None
        self.path_to_old_data = None
        self.collected_id_list = None
        self.invalid_ids = None
        self.id_update = False


    def fit(self, path_to_new, path_to_old, old_data_id_col, path_to_master, cats):
        """
            Function to Deduplicate newly collected records against an old existing database. Will use Google IDs to
            deduplicate

            :param path_to_new: file path to the master dataset (CSV format) of businesses newly collected
            :param path_to_old: file path to an update-to-date existing data base download - SalesForce data.
            :param old_data_id_col: column name for column containing the IDs to deduplicate on.
            :param path_to_master: file_path to the master dataset to retrieve the Google categories for the existing records
            :param cats: a list of Google categories included in the data collection.
        """

        # self.path_to_master = path_to_master
        # self.path_to_old_data = path_to_old

        # open new data
        df1 = pd.read_csv(path_to_new).drop_duplicates(subset='gplace_id')

        # open old data
        df2 = pd.read_csv(path_to_old).drop_duplicates(subset=old_data_id_col)

        # open old master data - to get categories
        master = pd.read_csv(path_to_master).drop_duplicates(subset='gplace_id')
        cat_cols = [col for col in master if col.startswith('cat_')]
        master['cat_string'] = master[cat_cols].apply(lambda x: ' '.join([str(i) for i in x.values.tolist()]), axis=1)

        # Append Google categories onto old records.
        df2 = pd.merge(df2, master[['gplace_id', 'cat_string', 'name']], left_on=old_data_id_col, right_on='gplace_id',
                       how='left')

        # filter the old data for the categories retrieved.
        cat_string = '|'.join(cats)
        df2 = df2[df2['cat_string'].fillna('').str.contains(cat_string)]
        print(df2[['Premises: Premises Name', 'gplace_id', 'cat_string']])


        # find duplicates
        new_ids = df1.gplace_id.dropna()
        df = df2[~df2['gplace_id'].isin(new_ids)]

        print('{} still in data'.format(len(df2[df2['gplace_id'].isin(new_ids)])))
        print('{} to find updates for'.format(len(df)))

        # UNCOMMENT BELOW FOR TESTING.
        # df = df.sample(100)

        # and now gather based on those that are new and haven't been collected previously
        self.ids_to_query = df['gplace_id'].unique().tolist()
        print('Prepped to collect {} id(s)'.format(len(self.ids_to_query)))
        print('This will cost ${}'.format(round(len(self.ids_to_query)*0.0025), 2))


    def write_raw(self):
        if check_you_want_to():
            for _id in tqdm(self.ids_to_query):
                json_data = self._get_json(goog_id=_id)
                self._write_json_to_file(results=json_data, goog_id=_id)
            print('Collected {} locations'.format(len(self.ids_to_query)))


    def save_invalid_ids(self, out_path='invalid_ids.csv'):
        print('\nFinding Invalid IDs...\n')
        self.invalid_ids = self.find_invalid_ids()
        df = pd.DataFrame(self.invalid_ids, columns=['gplace_ids'])
        df.to_csv(out_path, index=False)


    def tabulate(self, out_file_path):
        print('\nTabulating...\n')
        df = self._convert_json_to_df(in_file_path='extra_raw_data/')
        self._write_to_file(df=df, out_file_path=out_file_path)


    def update_invalid_ids(self, invalid_id_path):
        df = pd.read_csv(invalid_id_path)
        self.ids_to_query = df.gplace_ids.to_list()
        self.fields = ['place_id']
        self.field_string = ','.join(self.fields)
        self.id_update = True


        print('Prepped to update {} id(s)'.format(len(self.ids_to_query)))


    def _get_json(self, goog_id):
        url = self.base_path + self.params.format(id=goog_id, fields=self.field_string, api_key=os.environ['API_KEY'])
        print(url)
        r = requests.get(
            url
        )
        status = r.json().get('status', {})

        if status == 'NOT_FOUND':
            result = {'gplace_id': goog_id, 'status': status}
            print(result)
            return result
        else:
            result = r.json().get('result', {})
            result['status'] = status
            print(result)
            return result


    def _write_json_to_file(self, results, goog_id):
        if self.id_update:
            path = 'id_update'
        else:
            path = 'extra_raw_data'
        Path(path).mkdir(parents=True, exist_ok=True)
        file_string = path+'/{gid}.json'

        with open(file_string.format(gid=goog_id), 'w') as file:
            json.dump(results, file)


    @staticmethod
    def merge_with_new(new_path, extra_path, out_path):
        new = pd.read_csv(new_path)
        extra = pd.read_csv(extra_path)

        df = pd.concat([new, extra])
        print(len(df))

        df.to_csv(out_path, index=False)




    @staticmethod
    def _write_to_file(df, out_file_path):
        df.to_csv(out_file_path, index=False)

    @staticmethod
    def _convert_json_to_df(in_file_path):
        all_data = []
        in_file_path += '/*.json'
        for file in glob.glob(in_file_path):
            file = file.replace('\\', '/')
            fpath, fileformat = file.rsplit('.', 1)
            folder, gid = fpath.rsplit('/', 1)
            data = json.load(open(file))
            status = data.get('business_status')

            if data.get('geometry') is not None:
                lat = data.get('geometry').get('location').get('lat')
                lon = data.get('geometry').get('location').get('lat')
            else:
                lat = None
                lon = None

            if status != 'NOT_FOUND':
                all_data.append(
                    {
                        'gplace_id': gid,
                        'lat': lat,
                        'lon': lon,
                        'name': data.get('name'),
                        'business_status': data.get('business_status'),
                        'rating': data.get('rating'),
                        'rating_count': data.get('user_ratings_total'),
                        'formatted_address': data.get('formatted_address'),
                        'international_phone_number': data.get('international_phone_number'),
                        'phone_number': data.get('formatted_phone_number'),
                        'street_number': get_address_elements_from_google_json(data, 'street_number'),
                        'road': get_address_elements_from_google_json(data, 'route'),
                        'postal_town': get_address_elements_from_google_json(data, 'postal_town'),
                        'admin_area_level_1': get_address_elements_from_google_json(data, 'administrative_area_level_1'),
                        'admin_area_level_2': get_address_elements_from_google_json(data, 'administrative_area_level_2'),
                        'country': get_address_elements_from_google_json(data, 'country'),
                        'monday_open_hour': get_time_element(data, 1, 'open'),
                        'monday_close_hour': get_time_element(data, 1, 'close'),
                        'tuesday_open_hour': get_time_element(data, 2, 'open'),
                        'tuesday_close_hour': get_time_element(data, 2, 'close'),
                        'wednesday_open_hour': get_time_element(data, 3, 'open'),
                        'wednesday_close_hour': get_time_element(data, 3, 'close'),
                        'thursday_open_hour': get_time_element(data, 4, 'open'),
                        'thursday_close_hour': get_time_element(data, 4, 'close'),
                        'friday_open_hour': get_time_element(data, 5, 'open'),
                        'friday_close_hour': get_time_element(data, 5, 'close'),
                        'saturday_open_hour': get_time_element(data, 6, 'open'),
                        'saturday_close_hour': get_time_element(data, 6, 'close'),
                        'sunday_open_hour': get_time_element(data, 7, 'open'),
                        'sunday_close_hour': get_time_element(data, 7, 'close'),
                        'post_code': get_address_elements_from_google_json(data, 'postal_code'),
                        'url': data.get('url'),
                        'website': data.get('website'),
                        'reviews': [i.get('text') for i in data.get('reviews', [])]
                    }
                )

        all_data = pd.DataFrame(all_data)
        max_reviews = all_data['reviews'].apply(lambda x: len(x) if x else 0).max()

        reviews = pd.DataFrame(all_data['reviews'].tolist(),
                               index=all_data.index,
                               columns=['review_{}'.format(i) for i in range(1, max_reviews + 1)])

        all_data = pd.concat([all_data.drop('reviews', axis=1), reviews], axis=1)

        return all_data

    @staticmethod
    def find_invalid_ids():
        in_file_path = 'extra_raw_data/'
        invalid_req = []
        in_file_path += '/*.json'
        for file in glob.glob(in_file_path):
            file = file.replace('\\', '/')
            fpath, fileformat = file.rsplit('.', 1)
            folder, gid = fpath.rsplit('/', 1)

            data = json.load(open(file))
            status = data.get('status')
            if status == 'NOT_FOUND':
                invalid_req.append(gid)
        return invalid_req


    """

    def _write_collected_ids_to_master(self):
        master = pd.read_csv(self.path_to_master)
        master['collected'] = master[master['gplace_id'].isin(self.ids_to_query)]['collected'] = 1
        master.to_csv(self.path_to_master, index=False)

    def _accept_or_reject_fsa(self, data_row):
        return get_rating_from_fsa_request(**data_row[0:2].to_dict()) if \
            bool(set(data_row[2].split(' ')) & set(self.fsa_categories)) else 'Not FSA'
    """
