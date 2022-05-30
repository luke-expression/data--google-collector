import pandas as pd
import numpy as np
import json
import os
from tqdm import tqdm
import glob
import requests
import time
from fuzzywuzzy import fuzz
from math import floor, ceil
from pathlib import Path
from expression.utils.utils import check_you_want_to


class PlaceFinder:
    def __init__(self):
        self._url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query={}&key={}'
        self._api_key = os.environ['API_KEY']
        self._data = []


    def find_places(self, path, id_col, name_col, address_col, postcode_col, consider_old=False):
        """
                Takes a dataset that contains the ID number (any ID number that is unique for each record in the set),
                Name, Address, and Postcode for some real businesses to be searched Google to collect additional information
                :param path: file path to dataset
                :param id_col: name of the column containing the ID numbers
                :param name_col: name of the column containing the names of the businesses
                :param address_col: name of the column containing the addresses of the businesses (without Postcode)
                :param postcode_col: name of the column containing the postcode of the businesses
                :param consider_old: Boolean to denote if the class should check if any records have been collected
                already in this directory
        """
        data = pd.read_csv(path)  # .sample(50)  # REMOVE SAMPLE TO DO FULL SET

        data['query'] = data[[name_col, address_col, postcode_col]].apply(lambda x: (x[0] + ', ' + x[1] + ', ' +
                                                                                     x[2]).replace(' ', '+'), axis=1)

        if consider_old:
            # REMOVE FOR FUTURE.
            data = data.iloc[4000:, :]

            old = pd.read_csv(consider_old)
            old = old[id_col].to_list()
            data = data[~(data[id_col].isin(old))]


        Path('Places').mkdir(parents=True, exist_ok=True)
        print('Ready to collect place data (text search) for {} places!'.format(len(data['query'])))
        print('This will cost ${}'.format(round(len(data['query']) * 0.032, 2)))
        time.sleep(1)

        total = len(data)
        success = 0

        if check_you_want_to():
            for i, query in tqdm(enumerate(data['query'])):
                try:
                    res = requests.get(self._url.format(query, self._api_key)).json()['results']
                    #print('\n', query)
                    print('\n', res)

                    if 'establishment' in res[0]['types']:

                        old_name = data[name_col].to_list()[i]
                        google_name = res[0]["name"]

                        print(old_name, 'AND', res[0]["name"])
                        j1 = self.jaro_winkler(old_name.lower(), google_name.lower())
                        print('J.W name distance ', j1)

                        old_add = data[address_col].to_list()[i] + ' ' +  data[postcode_col].to_list()[i]
                        google_add = res[0]['formatted_address'].replace(', United Kingdom', '')

                        print(old_add, 'AND', google_add)
                        j2 = fuzz.ratio(old_add.lower(), google_add.lower())
                        print('Lev address distance ', j2)

                        # Thresholds for Name matching (j1) and address matching (j2)
                        if j1 >= 0.75 and j2 >= 50:


                            try:
                                dict1 = {'id': list(data[id_col])[i],
                                         "name": res[0]["name"],
                                         "formatted_address": res[0]["formatted_address"],
                                         "place_id": res[0]["place_id"],
                                         "rating": res[0]["rating"],
                                         "user_ratings_total": res[0]["user_ratings_total"],
                                         "cats": res[0]['types'],
                                         'business_status': res[0]['business_status']}
                                #print( '\n', dict1)

                            except KeyError:
                                dict1 = {'id': list(data[id_col])[i],
                                         "name": res[0]["name"],
                                         "formatted_address": res[0]["formatted_address"],
                                         "place_id": res[0]["place_id"],
                                         "rating": np.nan,
                                         "user_ratings_total": np.nan,
                                         "cats": res[0]['types'],
                                         'business_status': res[0]['business_status']}


                            file_string = 'places/{}_place_search.json'.format(dict1["place_id"])
                            with open(file_string, 'w') as file:
                                json.dump(dict1, file)

                            success += 1

                        else:
                            print('MATCH REJECTED\n')

                    else:
                        print('PLACE NOT FOUND\n')

                except IndexError:
                    print('Index error for', query)
                    continue

            print('\n\nTotal Successes = '+str(success))
            print('\nSuccess percentage = '+str((success/total)*100)+'%')

    # Jaro Winkler Similarity
    def jaro_winkler(self, s1, s2):

        jaro_dist = self.jaro_distance(s1, s2)

        # If the jaro Similarity is above a threshold
        if jaro_dist > 0.7:

            # Find the length of common prefix
            prefix = 0

            for i in range(min(len(s1), len(s2))):

                # If the characters match
                if s1[i] == s2[i]:
                    prefix += 1

                # Else break
                else:
                    break

            # Maximum of 4 characters are allowed in prefix
            prefix = min(4, prefix)

            # Calculate jaro winkler Similarity
            jaro_dist += 0.1 * prefix * (1 - jaro_dist)

        return jaro_dist

    @staticmethod
    def write_to_csv(out_path):
        in_file_path = 'Places//*.json'
        rows = {}
        new = 0
        max_cats = 0
        for file in tqdm(glob.glob(in_file_path)):
            dict1 = json.load(open(file))
            if len(dict1['cats']) > max_cats:
                max_cats = len(dict1['cats'])
            rows[dict1['id']] = [dict1["place_id"], dict1['name'], dict1["formatted_address"],
                                 dict1["rating"], dict1["user_ratings_total"]] + dict1['cats']

        cols = ['gplace_id', 'name', 'formatted_address', 'rating', 'rating_count'] + \
               ['cat_'+str(i) for i in range(1, max_cats+1, 1)]

        df = pd.DataFrame.from_dict(rows, orient='index', columns=cols)

        df = df.reset_index()
        df = df.rename(columns={'index': 'ID'})
        df.to_csv(out_path, index=False)




    @staticmethod
    def jaro_distance(s1, s2):

        # If the strings are equal
        if s1 == s2:
            return 1.0

        # Length of two strings
        len1 = len(s1)
        len2 = len(s2)

        if len1 == 0 or len2 == 0:
            return 0.0

        # Maximum distance upto which matching
        # is allowed
        max_dist = (max(len(s1), len(s2)) // 2) - 1

        # Count of matches
        match = 0

        # Hash for matches
        hash_s1 = [0] * len(s1)
        hash_s2 = [0] * len(s2)

        # Traverse through the first string
        for i in range(len1):

            # Check if there is any matches
            for j in range(max(0, i - max_dist),
                           min(len2, i + max_dist + 1)):

                # If there is a match
                if s1[i] == s2[j] and hash_s2[j] == 0:
                    hash_s1[i] = 1
                    hash_s2[j] = 1
                    match += 1
                    break

        # If there is no match
        if match == 0:
            return 0.0

        # Number of transpositions
        t = 0

        point = 0

        # Count number of occurrences
        # where two characters match but
        # there is a third matched character
        # in between the indices
        for i in range(len1):
            if hash_s1[i]:

                # Find the next matched character
                # in second string
                while hash_s2[point] == 0:
                    point += 1

                if s1[i] != s2[point]:
                    point += 1
                    t += 1
                else:
                    point += 1

            t /= 2

        # Return the Jaro Similarity
        return ((match / len1 + match / len2 +
                 (match - t) / match) / 3.0)

