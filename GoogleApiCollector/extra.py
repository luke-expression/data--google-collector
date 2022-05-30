import glob
import json
import os
import requests
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from utils.utils import get_address_elements_from_google_json, get_time_element, get_weekday_time_element, \
    check_you_want_to, get_rating_from_fsa_request, get_keywords, flag


class ContactDetailCollector:
    """
    Still to add:
    get final fsa acceptance
    cafe restaurant baker deli if FSA
    """

    def __init__(self, chain_threshold=5):

        self.base_path = 'https://maps.googleapis.com/maps/api/place/details/json?'
        self.params = 'place_id={id}&fields={fields}&key={api_key}'

        self.fields = [
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
            'website'
        ]

        self.field_string = ','.join(self.fields)

        self.ids_to_query = []
        self.ids_to_query_now = []
        self.chain_threshold = chain_threshold
        self.path_to_master = None
        self.collected_id_list = None
        self.acceptable_fsa_scores = ['4', '5', 'Pass', 'AwaitingInspection', 'Not FSA']
        self.fsa_categories = ['cafe', 'bakery', 'meal_takeaway', 'meal_delivery', 'restaurant']
        self.source = 'Normal'
        self.ids_collected = []


    def fit(self, path_to_master, filter_master_dataset=True, bad_cat_path=None, keyword_path=None, remove_chains=True,
                exclude_collected=True, log_filtered=False, test=False):
        """
            Function to fit the newly collected data from first Google data search to the second Google search to collect
            contact details. Aims to filter out businesses we are not interested in taking forward to the second search.
            Mainly consisting of large chain businesses & places currently out of scope for Expression to sell to.

            :param path_to_master: file path to the master dataset (CSV format) of businesses collected in first search of Google data
                pull and identifying which records have already been though the second search previously.
            :param filter_master_dataset: Boolean parameter to determine if the master dataset should go through filtering
                before the second Google data pull.
            :param bad_cat_path: file path to list of google categories that should be excluded from the second search
            :param keyword_path: file path to CSV of keywords that business names should be filtered for to remove the
                out of scope businesses
            :param remove_chains: Boolean parameter to determine if to filter for chains or not.
            :param exclude_collected: Boolean parameter to determine if to remove old + searched data.
            :param log_filtered: Boolean parameter to determine if to save a df to CSV of each stage were records are
                removed - will fo this for Keywords & Bad Categories.
            :param test: Boolean parameter to determine if it is a test run, and hence only a sample of the data
                should be taken.
        """

        self.path_to_master = path_to_master

        # just use relevant cols to keep memory usage down and remove duplicate gplace_ids
        df = pd.read_csv(self.path_to_master)
        cat_cols = [col for col in df if col.startswith('cat_')]
        df['cat_string'] = df[cat_cols].apply(lambda x: ' '.join([str(i) for i in x.values.tolist()]), axis=1)

        if test:
            print('THIS IS A TEST RUN - SAMPLE OF 50')
            df = df.sample(50)

        # we may have collected an id before that's marked collected in one row but not in another - lets merge them
        # and re-mark collected
        df['collected'] = df.groupby('gplace_id')['collected'].transform('sum').apply(lambda x: 1 if x >= 1 else 0)
        df = df.drop_duplicates(subset=['gplace_id', 'collected'], keep='last')
        print('\nRecords remaining after dropping duplicates:', len(df))


        # If we want to do the filtering
        if filter_master_dataset:

            # we now remove chains according to threshold
            if remove_chains:
                df['chain_freq'] = df.fillna(' ').groupby(['name', 'cat_1', 'cat_2'])['gplace_id'].transform('count')
                df = df[df['chain_freq'] <= self.chain_threshold]
                print('\nRecords remaining after removing obvious chains:', len(df))

            # Now only look at those that are new rows that have not been collected before:
            if exclude_collected:
                df = df[(df['new_data_flag'] == 1) & (df['collected'] == 0)]
                print('\nRecords remaining after filtering for new data:', len(df))

            # Now flag data for any signs we do not want to insure the place, using the keyword bank to identify problem
            # places.
            if keyword_path:
                kw = pd.read_csv(keyword_path)
                df['flag'] = df[['cat_string', 'name']].apply(lambda x: flag(*x, kw), axis=1)
                print(df['flag'].value_counts(dropna=False))

                # if log_filtered==True, then save CSV of businesses removed for being flagged.
                if log_filtered:
                    df[['gplace_id', 'name', 'cat_string', 'flag']].to_csv('log_keyword_removals.csv', index=False)

                df = df[df.flag.isna()]
                print('\nRecords remaining after removing flagged:', len(df))

            # Now filter out any that have problem categories, found in the file 'bad_cats.csv'
            if bad_cat_path:
                bad_cats = list(pd.read_csv(bad_cat_path).category)
                bad_cats_str = '|'.join(bad_cats)

                # if log_filtered==True, then save CSV of businesses removed for having bad categories.
                if log_filtered:
                    df['bad_cats'] = df['cat_string'].apply(lambda x: '|'.join([cat for cat in bad_cats if cat in x]))
                    df[df.cat_string.str.contains(bad_cats_str)][['gplace_id', 'name', 'cat_string', 'bad_cats']].to_csv(
                        'log_cat_removals.csv', index=False)

                df = df[~df.cat_string.str.contains(bad_cats_str)]
                print('\nRecords remaining after removing bad categories:', len(df))

        # and now gather based on those that are new and haven't been collected previously
        self.ids_to_query = df['gplace_id'].unique().tolist()
        print('Prepped to collect {} id(s)'.format(len(self.ids_to_query)))
        print('This will cost ${}'.format(round(len(self.ids_to_query)*0.0025), 2))


    def simple_fit(self, file_path):
        df = pd.read_csv(file_path)
        self.ids_to_query = df['gplace_id'].unique().tolist()
        print('Prepped to collect {} id(s)'.format(len(self.ids_to_query)))
        print('This will cost ${}'.format(round(len(self.ids_to_query) * 0.025), 2))
        self.source = 'Else'


    def write_raw(self):

        self._find_collected()

        if len(self.ids_collected) != 0:
            print('{} Ids already collected'.format(len(self.ids_collected)))
            for _id in self.ids_to_query:
                if _id not in self.ids_collected:
                    self.ids_to_query_now.append(_id)
        else:
            self.ids_to_query_now = self.ids_to_query


        if check_you_want_to():
            for _id in tqdm(self.ids_to_query_now):
                json_data = self._get_json(goog_id=_id)
                self._write_json_to_file(results=json_data, goog_id=_id)
            print('Collected {} locations'.format(len(self.ids_to_query)))

            if self.source == 'Normal':
                self._write_collected_ids_to_master()
                print('Logged ids to collected list')


    def tabulate(self, out_file_path):
        df = self._convert_json_to_df(in_file_path='extra_raw_data/')
        self._write_to_file(df=df, out_file_path=out_file_path)

    def _get_json(self, goog_id):
        r = requests.get(
            self.base_path + self.params.format(id=goog_id, fields=self.field_string, api_key=os.environ['API_KEY'])
        )
        return r.json().get('result', {})


    def _find_collected(self):

        in_file_path = 'extra_raw_data/'
        in_file_path += '/*.json'
        for file in glob.glob(in_file_path):
            file = file.replace('\\', '/')
            fpath, fileformat = file.rsplit('.', 1)
            folder, gid = fpath.rsplit('/', 1)
            self.ids_collected.append(gid)

    @staticmethod
    def _write_json_to_file(results, goog_id):
        Path('extra_raw_data').mkdir(parents=True, exist_ok=True)
        file_string = 'extra_raw_data/{gid}.json'

        with open(file_string.format(gid=goog_id), 'w') as file:
            json.dump(results, file)

    @staticmethod
    def _write_to_file(df, out_file_path):
        df.to_csv(out_file_path, index=False)

    @staticmethod
    def _convert_json_to_df(in_file_path):
        all_data = []
        in_file_path += '/*.json'
        print('Tabulating...')
        for file in tqdm(glob.glob(in_file_path)):
            file = file.replace('\\', '/')
            fpath, fileformat = file.rsplit('.', 1)
            folder, gid = fpath.rsplit('/', 1)

            data = json.load(open(file))
            all_data.append(
                {
                    'gplace_id': gid,
                    'formatted_address': data.get('formatted_address'),
                    'international_phone_number': data.get('international_phone_number'),
                    'phone_number': data.get('formatted_phone_number'),
                    'street_number': get_address_elements_from_google_json(data, 'street_number'),
                    'road': get_address_elements_from_google_json(data, 'route'),
                    'postal_town': get_address_elements_from_google_json(data, 'postal_town'),
                    'admin_area_level_1': get_address_elements_from_google_json(data, 'administrative_area_level_1'),
                    'admin_area_level_2': get_address_elements_from_google_json(data, 'administrative_area_level_2'),
                    'country': get_address_elements_from_google_json(data, 'country'),
                    'monday_open_hour': get_weekday_time_element(data, 'Monday', 'open'),
                    'monday_close_hour': get_weekday_time_element(data, 'Monday', 'close'),
                    'tuesday_open_hour': get_weekday_time_element(data, 'Tuesday', 'open'),
                    'tuesday_close_hour': get_weekday_time_element(data, 'Tuesday', 'close'),
                    'wednesday_open_hour': get_weekday_time_element(data, 'Wednesday', 'open'),
                    'wednesday_close_hour': get_weekday_time_element(data, 'Wednesday', 'close'),
                    'thursday_open_hour': get_weekday_time_element(data, 'Thursday', 'open'),
                    'thursday_close_hour': get_weekday_time_element(data, 'Thursday', 'close'),
                    'friday_open_hour': get_weekday_time_element(data, 'Friday', 'open'),
                    'friday_close_hour': get_weekday_time_element(data, 'Friday', 'close'),
                    'saturday_open_hour': get_weekday_time_element(data, 'Saturday', 'open'),
                    'saturday_close_hour': get_weekday_time_element(data, 'Saturday', 'close'),
                    'sunday_open_hour': get_weekday_time_element(data, 'Sunday', 'open'),
                    'sunday_close_hour': get_weekday_time_element(data, 'Sunday', 'close'),
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

    def _write_collected_ids_to_master(self):
        master = pd.read_csv(self.path_to_master)
        master['collected'] = master[master['gplace_id'].isin(self.ids_to_query)]['collected'] = 1
        master.to_csv(self.path_to_master, index=False)

    def _accept_or_reject_fsa(self, data_row):
        return get_rating_from_fsa_request(**data_row[0:2].to_dict()) if \
            bool(set(data_row[2].split(' ')) & set(self.fsa_categories)) else 'Not FSA'


