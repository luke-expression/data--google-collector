# import ast
import glob
import json
import os
# import webbrowser
from pathlib import Path
import folium
import pandas as pd
import requests
# from shapely.geometry import Point
# import geopandas as gpd
from tqdm import tqdm
import time

from data.CategoryToRadiusMap import CATEGORY_RADIUS_MAP
from utils.utils import shift_n_meters, calc_required_lats, calc_required_lons, check_you_want_to, \
    flatten_list, find_distance


class GoogleMapCollector:
    def __init__(self, path=''):
        self._region_name = ''
        self.bottom_left = tuple()
        self.top_right = tuple()
        self._categories = []
        self._lat_centre = None
        self._lon_centre = None
        self.map = None
        self._zoom = 15
        self._circle_geometries = []
        self.cost_per_search = 0.04
        self.overlap_percentage = 0.01
        self.world_size = 110540
        self.flood_data = None
        self.flood_area_percentage_threshold = 25
        self.empty_region = []
        self.second_page = []
        self.third_page = []
        self.maxed_circles = []
        self.num_maxed_circles = 0
        self.exclude_circles = None
        self.file_path = path

        self._api_key = os.environ['API_KEY']

    """def fit(self, params, categories, postcode_data_path, flood_data_path, log_to_file_path=None, country='UK'):
        """"""
        #Fits the collector, creating a map of the search circles and prepares the relevant longitudes, latitudes and
        #radii to feed into 'collect()'
        #:param log_to_file_path: file path if desired
        #:param postcode_data_path:
        #:param flood_data_path:
        #:param categories: list of search categories of interest
        #:param params: dictionary of type {location: (bottom left long lat), (top right long lat), pop_density}
        #:param country: what country we are collecting data for

        #Notes: radii adjusting not working so set values to 1 in category map
        """"""

        if isinstance(categories, str):
            categories = [categories]

        self._categories = [cat for cat in flatten_list(categories)]
        self.flood_data = self._create_flood_data(postcode_data_path=postcode_data_path,
                                                  flood_data_path=flood_data_path)

        print('Fitting...')
        print(params)
        for region, geographic_params in params.items():
            print(geographic_params)
            if type(geographic_params['bottom_left']) == str:
                location_params = {
                    'region_name': region,
                    'bottom_left': ast.literal_eval(geographic_params['bottom_left'])[::-1],
                    'top_right': ast.literal_eval(geographic_params['top_right'])[::-1],
                    'categories': self._categories,
                    'shape_object': geographic_params['geometry']
                }
            else:
                location_params = {
                    'region_name': geographic_params,
                    'bottom_left': geographic_params['bottom_left'],
                    'top_right': geographic_params['top_right'],
                    'categories': self._categories,
                    'shape_object': geographic_params['geometry']
                }

            self._region_name = region
            radius = self._population_density_to_radius_function(region, geographic_params['pop_density'], self._categories)
            if country == 'UK':
                self._create_circles(rad=radius,
                                 bottom_left=location_params['bottom_left'],
                                 top_right=location_params['top_right'],
                                 geometry=location_params['shape_object'],
                                 log_to_file_path=log_to_file_path)
            else:
                self._create_circles_2(rad=radius,
                                 bottom_left=location_params['bottom_left'],
                                 top_right=location_params['top_right'],
                                 geometry=location_params['shape_object'],
                                 log_to_file_path=log_to_file_path)

        print('Prepared to collect {} circle(s) for {}'.format(
            len(self._circle_geometries), ', '.join(params.keys())))
        print('For {} categories, this will cost ${}'.format(
            len(self._categories), len(self._categories)*len(self._circle_geometries)*0.04))
        print('Categories = ', self._categories)"""

    def import_circles(self, circle_data_path, new_threshold, categories, params=False, country='UK'):
        """
        A function for accessing the scored circles saved from a previous fit to see how changing the overlap
        threshold looks

        Notes: Circle sizes fixed or dependent on import

        """

        if isinstance(categories, str):
            categories = [categories]
        self._categories = [cat for cat in flatten_list(categories)]

        all_circles = pd.read_csv(self.file_path+circle_data_path)
        if country == 'UK':
            all_circles = all_circles[all_circles['percentage_accept'] >= new_threshold]

        all_circles = all_circles[['lon', 'lat', 'rad']]
        # .sample(10, random_state=3) # use sampling to test before running all circles
        all_circles = all_circles.values.tolist()
        self._circle_geometries = all_circles

        cost = round(len(self._circle_geometries) * 0.04, 2)
        print('Prepared to spend ${} on {} circle(s)'.format(
            cost, len(self._circle_geometries)))
        print('For {} categories, this will cost ${}.'.format(len(self._categories), len(self._categories)*cost))
        print('Categories = ', self._categories)

        self.map = self._get_map(all_circles[0][0], all_circles[0][1])

        if params:
            for region, geographic_params in params.items():
                location_params = {
                    'shape_object': geographic_params['geometry']
                }

                geometry = location_params['shape_object']
                folium.GeoJson(geometry,
                               style_function=lambda feature: {
                                   'weight': 1,
                                   'fillOpacity': 0.25}
                               ).add_to(self.map)

        for lat, lon, rad in all_circles:
            folium.Circle((lat, lon), radius=rad,
                          line_opacity=0.2,
                          weight=0.5,
                          color='black',
                          fill=True,
                          fill_color='white',
                          fill_opacity=0.2
                          ).add_to(self.map)

        self._auto_open_folium_map(path=self.file_path+'map_new.html', folium_obj=self.map)


    def write_raw_2(self):
        """
        Use cautiously! This will hit Google's API with the fitted data and write them to .json locally
        UPDATED VERSION TO ACCOUNT FOR TIME DELAY IN SECOND AND THIRD PAGES FOR EACH CIRCLE.
        """


        self.exclude_used_circles()


        if check_you_want_to():
            print('Writing first pages of Circles...')
            for lat, long, rad in tqdm(self._circle_geometries):
                for category in self._categories:
                    #print(lat, long, rad, category)

                    self._write_circle_to_json_1(lat=lat,
                                                 long=long,
                                                 cat=category,
                                                 rad=rad)
            time.sleep(5)
            self._circle_geometries.clear()
            print('Writing second pages of Circles...')
            in_file_path = self.file_path+'page_1//*.json'
            for file in tqdm(glob.glob(in_file_path)):
                data = json.load(open(file))
                lat, long, rad, cat, status, next_page_token, total = data.values()
                if next_page_token:
                    self._write_circle_to_json_2(lat=lat,
                                                 long=long,
                                                 cat=cat,
                                                 rad=rad,
                                                 next_page_token=next_page_token)

            time.sleep(5)
            print('Writing third pages of Circles...')
            in_file_path = self.file_path+'page_2//*.json'
            for file in tqdm(glob.glob(in_file_path)):
                data = json.load(open(file))
                lat, long, rad, cat, status, next_page_token, total = data.values()
                if next_page_token:
                    self.num_maxed_circles += self._write_circle_to_json_3(lat=lat,
                                                                       long=long,
                                                                       cat=cat,
                                                                       rad=rad,
                                                                       second_next_page_token=next_page_token)
            print(self.num_maxed_circles, 'circle(s) maxed out\n')


    def exclude_used_circles(self):
        """
        Function to examine what circles have already been seacrhed, and to then remove them from the next search.
        To be used in the case of a program crash mid way.
        """

        all_data = []
        in_file_path = self.file_path+'./page_1'
        in_file_path += '/*.json'
        for file in glob.glob(in_file_path):
            data = json.load(open(file))
            all_data.append([
                data['lat'], data['long'], data['rad'], data['cat']
            ])

        # print(all_data)

        # find all completed circles
        grouped_data = {}
        for lat, lon, rad, cat in all_data:
            circle = str(lat)+'_AND_'+str(lon)+'_AND_'+str(rad)
            if circle in grouped_data.keys():
                grouped_data[circle] = grouped_data[circle] + [cat]
            else:
                grouped_data[circle] = [cat]
        del all_data

        circle_count_1 = len(self._circle_geometries)
        for circle, cats in grouped_data.items():
            if sorted(cats) == sorted(self._categories):
                lat, lon, rad = circle.split('_AND_')
                circle = [float(lat), float(lon), float(rad)]
                if circle in self._circle_geometries:
                    self._circle_geometries.remove(circle)
        circle_count_2 = len(self._circle_geometries)

        if circle_count_1-circle_count_2 != 0:

            print('\n\n{0} circles removed out of {1}\nCircles remaining: {2}\n'.format(
                circle_count_1-circle_count_2, circle_count_1, circle_count_2
            ))

            cost = round(len(self._circle_geometries) * 0.04, 2)
            print('Prepared to spend ${} on {} circle(s)'.format(
                cost, len(self._circle_geometries)))
            print('For {} categories, this will cost ${}.'.format(len(self._categories), len(self._categories) * cost))
            print('Categories = ', self._categories)


    def tabulate(self, out_file_path, dedupe=False):
        """
        Write the collected .json to a .csv
        """
        df = self._convert_json_to_df(in_file_path=self.file_path+'raw_data/', dedupe=dedupe)
        self._write_to_file(df=df, out_file_path=self.file_path+out_file_path)


    def write_to_master(self, master_file_path, clean_master=True):

        master = self._get_or_create_master(master_file_path)

        new_data = self._convert_json_to_df(in_file_path=self.file_path+'raw_data/')
        new_data['new_data_flag'] = 1
        new_data['collected'] = 0

        df = pd.concat([master, new_data], axis=0).reset_index(drop=True)

        if clean_master:
            df['collected'] = df.groupby('gplace_id')['collected'].transform('sum').apply(
                lambda x: 1 if x >= 1 else 0)
            df = df.drop_duplicates(subset=['gplace_id', 'collected'], keep='last')

        self._write_to_file(df=df, out_file_path=master_file_path)


    def log_temp_circles(self, out_path):
        final_circles = []
        for lat, long, rad in tqdm(self._circle_geometries):
            for category in self._categories:
                final_circles.append([lat, long, category, self._adjust_radius(rad, category)])

        pd.DataFrame(final_circles, columns=['lat', 'lon', 'cat', 'rad']).to_csv(out_path, index=False)


    def log_circles(self, circles_csv_path):
        new_circle_data = pd.DataFrame(self._circle_geometries, columns=['lat', 'lon', 'rad'])
        try:
            pd.concat(
                [pd.read_csv(circles_csv_path), new_circle_data], axis=0
            ).reset_index(drop=True).to_csv(circles_csv_path, index=False)

        except FileNotFoundError:
            new_circle_data.to_csv(circles_csv_path, index=False)


    def update_master_map(self, map_path):
        m = self._get_map(
            lat=self._circle_geometries[0][0],
            long=self._circle_geometries[0][1]
        )

        for lat, lon, rad in self._circle_geometries:
            folium.Circle((lat, lon), radius=rad,
                          line_opacity=0.2,
                          weight=0.5,
                          color='black',
                          fill=True,
                          fill_color='white',
                          fill_opacity=0.2
                          ).add_to(m)

        self._auto_open_folium_map(path=map_path, folium_obj=m)


    @staticmethod
    def _convert_json_to_df(in_file_path, dedupe=False):
        """
        Takes file path with json data and converts to .csv format as pandas dataframe
        :param in_file_path: json data file path
        :return: pandas dataframe
        """
        all_data = []
        in_file_path += '/*.json'
        for file in tqdm(glob.glob(in_file_path)):
            data = json.load(open(file))
            all_data.append({
                'lat': data['geometry']['location']['lat'],
                'lon': data['geometry']['location']['lng'],
                'name': data['name'],
                'gplace_id': data.get('place_id'),
                'price_level': data.get('price_level'),
                'rating': data.get('rating'),
                'rating_count': data.get('user_ratings_total'),
                'cats': data.get('types', []),
                'location': data.get('vicinity'),
                'loc_code': data.get('plus_code', {}).get('compound_code'),
                'business_status': data.get('business_status')
            })

        all_data = pd.DataFrame(all_data)
        max_cats = all_data['cats'].apply(lambda x: len(x) if x else 0).max()

        cats = pd.DataFrame(all_data['cats'].tolist(),
                            index=all_data.index,
                            columns=['cat_{}'.format(i) for i in range(1, max_cats + 1)])

        all_data = pd.concat([all_data.drop('cats', axis=1), cats], axis=1)

        if dedupe:
            all_data = all_data.drop_duplicates(subset=['gplace_id'])

        return all_data


    @staticmethod
    def _write_to_file(df, out_file_path):
        """
        Simple function to write dataframe to .csv for readability
        :param df: dataframe
        :param out_file_path: save to filepath
        :return: nothing but writes to file given
        """
        df.to_csv(out_file_path, index=False)


    @staticmethod
    def _auto_open_folium_map(path, folium_obj):
        """
        Quick/dirty function to write map to map.html to view circles on folium map
        :param path: path to write to
        :param folium_obj: the map object
        :return: nothing but writes to path. Open the html file and view in browser
        """
        html_page = f'{path}'
        folium_obj.save(html_page)
        # webbrowser.open(html_page, new=2)


    def _get_map(self, lat, long):
        """
        Creates a folium map object
        :param lat: latitude
        :param long: longitude
        :return: folium map object
        """
        return folium.Map(location=[lat, long], zoom_start=self._zoom)


    def _write_page_to_json(self, page_data, cat, lat, long):
        """
        Takes one of up to 3 pages returned by google's nearby search api and writes each dictionary and writes to json
        :param cat: category: cafe, restaurant etc.
        :param page_data: page of data returned by api
        :param lat: lat of circle used
        :param long: long of circle used
        :return: nothing but writes to folder raw_data/...
        """
        Path(self.file_path+'raw_data').mkdir(parents=True, exist_ok=True)
        for place in page_data['results']:
            name = place.get('reference')
            file_string = self.file_path+'raw_data/{cat}_{name}_{lat}_{long}.json'

            formatted_file_string = file_string.format(cat=cat,
                                                       name=name,
                                                       lat=lat,
                                                       long=long)

            with open(formatted_file_string, 'w') as file:
                json.dump(place, file)
        return len(page_data['results'])


    """
    def _create_circles(self, rad, bottom_left, top_right, geometry, log_to_file_path):
        """"""
        Takes various spacial params to return locations of grid of circles that can be used to hit google's api
        :param rad: radius of circles (meters)
        :return: list of lats and longs of required circles to cover given space
        Will also create a map.html file in path, open and click through to relevant browser to display
        """"""
        # Create map if not existing already
        if not self.map:
            self.map = self._get_map(
                lat=(bottom_left[0] + top_right[0]) / 2,
                long=(bottom_left[1] + top_right[1]) / 2
            )

        # Get grid circle centers
        lats = calc_required_lats(rad, bottom_left[1], top_right[1])
        lons = calc_required_lons(rad, bottom_left[0], top_right[0])

        # Adjust position appropriately
        all_lon_lats = [
            shift_n_meters(lon, lat, rad / (2 ** 0.5), rad / (2 ** 0.5))
            for lat in lats
            for lon in lons
        ]


        # Remove those that don't intersect with region (added while loop to fix
        cleaned_all_lon_lats = []
        scale = 0.5

        while len(cleaned_all_lon_lats) == 0:
            scale = scale*2
            cleaned_all_lon_lats = [(lat, lon) for lat, lon in all_lon_lats
                                    if Point(lon, lat).buffer(rad*scale / self.world_size).intersection(
                    geometry).area / geometry.area > self.overlap_percentage]
            if len(cleaned_all_lon_lats) == 0:
                print('no circles remain, retrying ')
                time.sleep(1)

        # Remove those that fall in flood risk etc. land
        scored_all_lon_lats = []
        for lon, lat in tqdm(cleaned_all_lon_lats):
            self.flood_data['int'] = self.flood_data.geometry.apply(
                lambda x: Point(lat, lon).buffer(rad / self.world_size).intersection(x).area)
            percentage_accept = ((self.flood_data['int'] / self.flood_data['int'].sum())
                                 * self.flood_data['accept']).sum() * 100
            scored_all_lon_lats.append([lon, lat, rad, percentage_accept])


        if log_to_file_path:
            try:
                scored_circles = pd.read_csv(self.file_path+log_to_file_path)
            except FileNotFoundError:
                scored_circles = pd.DataFrame(columns=['lat', 'lon', 'rad', 'percentage_accept'])

            new_circles = pd.DataFrame(scored_all_lon_lats,
                                       columns=['lon', 'lat', 'rad', 'percentage_accept'])
            all_circles = pd.concat([scored_circles, new_circles])
            if 'Unnamed: 0' in list(all_circles.columns):
                all_circles = all_circles.drop(columns=['Unnamed: 0'])
            all_circles.to_csv(self.file_path+log_to_file_path)


        del cleaned_all_lon_lats

        derisked_all_lon_lats = []
        for lon, lat, rad, percentage_accept in scored_all_lon_lats:
            if percentage_accept > self.flood_area_percentage_threshold:
                derisked_all_lon_lats.append((lon, lat))

        # And plot
        folium.GeoJson(geometry,
                       style_function=lambda feature: {
                           'weight': 1,
                           'fillOpacity': 0.25
                       }).add_to(self.map)

        for lat, lon in derisked_all_lon_lats:
            folium.Circle((lat, lon), radius=rad,
                          line_opacity=0.2,
                          weight=0.5,
                          color='black',
                          fill=True,
                          fill_color='white',
                          fill_opacity=0.2
                          ).add_to(self.map)

        if len(derisked_all_lon_lats) != 0:
            print('Cost for {} will be = {} * {} = ${}'.format(self._region_name,
                                                               len(derisked_all_lon_lats),
                                                               self.cost_per_search,
                                                               self.cost_per_search * len(derisked_all_lon_lats)))
        else:
            print(self._region_name, ' Has 0 circles.')
            self.empty_region.append(self._region_name)

        self._auto_open_folium_map(path=self.file_path+'map.html', folium_obj=self.map)

        for lat, lon in derisked_all_lon_lats:
            self._circle_geometries.append([lat, lon, rad])
            



    def _create_circles_2(self, rad, bottom_left, top_right, geometry, log_to_file_path):
        """"""
        Takes various spacial params to return locations of grid of circles that can be used to hit google's api
        :param rad: radius of circles (meters)
        :return: list of lats and longs of required circles to cover given space
        Will also create a map.html file in path, open and click through to relevant browser to display
        """"""
        # Create map if not existing already
        if not self.map:
            self.map = self._get_map(
                lat=(bottom_left[0] + top_right[0]) / 2,
                long=(bottom_left[1] + top_right[1]) / 2
            )

        # Get grid circle centers
        lats = calc_required_lats(rad, bottom_left[1], top_right[1])
        lons = calc_required_lons(rad, bottom_left[0], top_right[0])

        # Adjust position appropriately
        all_lon_lats = [
            shift_n_meters(lon, lat, rad / (2 ** 0.5), rad / (2 ** 0.5))
            for lat in lats
            for lon in lons
        ]

        # Remove those that don't intersect with region (added while loop to fix
        cleaned_all_lon_lats = []
        scale = 0.5

        while len(cleaned_all_lon_lats) == 0:
            scale = scale * 2
            cleaned_all_lon_lats = [(lat, lon) for lat, lon in all_lon_lats
                                    if Point(lon, lat).buffer(rad * scale / self.world_size).intersection(
                    geometry).area / geometry.area > self.overlap_percentage]
            cleaned_all_lon_lats = [(lat, lon, rad) for lat, lon in cleaned_all_lon_lats]

            if len(cleaned_all_lon_lats) == 0:
                print('no circles remain, retrying ')
                time.sleep(1)

        # Remove those that fall in flood risk etc. land
        #scored_all_lon_lats = []
        #for lon, lat in tqdm(cleaned_all_lon_lats):
        #    self.flood_data['int'] = self.flood_data.geometry.apply(
        #        lambda x: Point(lat, lon).buffer(rad / self.world_size).intersection(x).area)
        #    percentage_accept = ((self.flood_data['int'] / self.flood_data['int'].sum())
        #                         * self.flood_data['accept']).sum() * 100
        #    scored_all_lon_lats.append([lon, lat, rad, percentage_accept])


        if log_to_file_path:
            try:
                scored_circles = pd.read_csv(self.file_path+log_to_file_path)
            except FileNotFoundError:
                scored_circles = pd.DataFrame(columns=['lat', 'lon', 'rad'])

            new_circles = pd.DataFrame(cleaned_all_lon_lats,
                                       columns=['lon', 'lat', 'rad'])
            all_circles = pd.concat([scored_circles, new_circles])
            if 'Unnamed: 0' in list(all_circles.columns):
                all_circles = all_circles.drop(columns=['Unnamed: 0'])
            all_circles.to_csv(self.file_path+log_to_file_path)



        derisked_all_lon_lats = []
        for lon, lat, rad in cleaned_all_lon_lats:
            derisked_all_lon_lats.append((lon, lat))

        # And plot
        folium.GeoJson(geometry,
                       style_function=lambda feature: {
                           'weight': 1,
                           'fillOpacity': 0.25
                       }).add_to(self.map)

        for lat, lon in derisked_all_lon_lats:
            folium.Circle((lat, lon), radius=rad,
                          line_opacity=0.2,
                          weight=0.5,
                          color='black',
                          fill=True,
                          fill_color='white',
                          fill_opacity=0.2
                          ).add_to(self.map)

        if len(derisked_all_lon_lats) != 0:
            print('Cost for {} will be = {} * {} = ${}'.format(self._region_name,
                                                               len(derisked_all_lon_lats),
                                                               self.cost_per_search,
                                                               self.cost_per_search * len(derisked_all_lon_lats)))
        else:
            print(self._region_name, ' Has 0 circles.')
            self.empty_region.append(self._region_name)

        self._auto_open_folium_map(path=self.file_path+'map.html', folium_obj=self.map)

        for lat, lon in derisked_all_lon_lats:
            self._circle_geometries.append([lat, lon, rad])

    """

    def _write_circle_to_json_1(self, lat, long, cat, rad):
        """
                Take a specific circle and write the first page to path using write_page_to_json, a new method of
                writing circles due to the time delay in second and thrid pages.
                :param lat: lat of circle used
                :param long: long of circle used
                :param cat: category: cafe, restaurant etc.
                :param rad: radius of circle (meters)
                :return: nothing but writes to folder {region_name}_data/...
        """
        # set up path and parameters
        base_path = 'https://maps.googleapis.com/maps/api/place/nearbysearch/'
        url_param_string = 'json?location={lat},{long}&radius={rad_m}&type={cat}&language=en-GB&key={key}'
        search_params = {'lat': lat, 'long': long, 'rad_m': rad, 'cat': cat, 'key': os.environ['API_KEY']}

        # request page 1
        page1 = requests.get(base_path + url_param_string.format(**search_params))
        page1_json = page1.json()
        total = self._write_page_to_json(cat=cat, page_data=page1_json, lat=lat, long=long)
        next_page_token = page1_json.get('next_page_token')
        status = page1_json.get('status')
        Path(self.file_path+'page_1').mkdir(parents=True, exist_ok=True)
        circle_dict = {'lat': lat, 'long': long, 'rad': rad, 'cat': cat, 'status': status,
                       'next_page_token': next_page_token, 'results': total}
        file_string = self.file_path+'page_1/first_page_{lat}_{long}_{rad}_{cat}.json'.format(**circle_dict)
        with open(file_string, 'w') as file:
            json.dump(circle_dict, file)




        # If there is a page 2, save circle with next page token and return to it later.
        #if next_page_token:
        #    self.second_page.append([lat, long, cat, rad, next_page_token])


    def _write_circle_to_json_2(self, lat, long, cat, rad, next_page_token):
        """
                After first pages have been wrote to files, come back to those that have a second page and access those.
                :param lat: lat of circle used
                :param long: long of circle used
                :param cat: category: cafe, restaurant etc.
                :param rad: radius of circle (meters)
                :param next_page_token: token to access second page of a circle.
                :return: nothing but writes to folder {region_name}_data/...
        """
        base_path = 'https://maps.googleapis.com/maps/api/place/nearbysearch/'
        url_next_page = 'json?pagetoken={}&key={}'

        page_2_path = base_path + url_next_page.format(next_page_token, os.environ['API_KEY'])
        page2 = requests.get(page_2_path)
        page2_json = page2.json()
        total = self._write_page_to_json(cat=cat, page_data=page2_json, lat=lat, long=long)
        second_next_page_token = page2_json.get('next_page_token')
        status = page2_json.get('status')
        Path(self.file_path+'page_2').mkdir(parents=True, exist_ok=True)
        circle_dict = {'lat': lat, 'long': long, 'rad': rad, 'cat': cat, 'status': status,
                       'next_page_token': second_next_page_token, 'results': total}
        file_string = self.file_path+'page_2/second_page_{lat}_{long}_{rad}_{cat}.json'.format(**circle_dict)
        with open(file_string, 'w') as file:
            json.dump(circle_dict, file)



    def _write_circle_to_json_3(self, lat, long, cat, rad, second_next_page_token):
        """
                After second pages have been wrote to files, come back to those that have a third page and access those.
                :param lat: lat of circle used
                :param long: long of circle used
                :param cat: category: cafe, restaurant etc.
                :param rad: radius of circle (meters)
                :param second_next_page_token: token to access final page of a circle.
                :return: nothing but writes to folder {region_name}_data/...
        """
        base_path = 'https://maps.googleapis.com/maps/api/place/nearbysearch/'
        url_next_page = 'json?pagetoken={}&key={}'

        page_3_path = base_path + url_next_page.format(second_next_page_token, os.environ['API_KEY'])
        page3 = requests.get(page_3_path)
        page3_json = page3.json()
        total = self._write_page_to_json(cat=cat, page_data=page3_json, lat=lat, long=long)
        status = page3_json.get('status')
        Path(self.file_path+'page_3').mkdir(parents=True, exist_ok=True)
        circle_dict = {'lat': lat, 'long': long, 'rad': rad, 'cat': cat, 'status': status, 'results': total}
        file_string = self.file_path+'page_3/third_page_{lat}_{long}_{rad}_{cat}.json'.format(**circle_dict)
        with open(file_string, 'w') as file:
            json.dump(circle_dict, file)
        if total == 20:
            return 1
        else:
            return 0



    def save_maxed_circles_to_csv(self, out_path=None, make_map=False):
        """
                After second pages have been wrote to files, come back to those that have a third page and access those.
                :param out_path: What to call the CSV/ where to save it.
                :param make_map: option to make a map of the circles that maxed out.
                :return: nothing but writes to files.0
        """
        in_file_path = self.file_path+'page_3//*.json'
        maxed_circles_data = []
        print('\nFinding circles which hit the maximum result count....')
        for file in tqdm(glob.glob(in_file_path)):
            data = json.load(open(file))
            lat, long, rad, cat, status, total = data.values()
            if total == 20:
                maxed_circles_data.append([lat, long, rad, cat])

        if len(maxed_circles_data) != 0:

            print('{} circles maxed out!'.format(len(maxed_circles_data)))
            # Save details of maxed out circles to csv
            if out_path:
                df = pd.DataFrame(maxed_circles_data, columns=['lat', 'lon', 'rad', 'cat'])
                df['used'] = 0
                df.to_csv(self.file_path+out_path, index=False)

            # Optional: make a map of the circles that have maxed out
            if make_map:
                max_circle_map = folium.Map(location=[maxed_circles_data[0][0], maxed_circles_data[0][1]],
                                            zoom_start=self._zoom)
                for lat, lon, rad, cat in maxed_circles_data:
                    folium.Circle((lat, lon), radius=rad,
                                  line_opacity=0.2,
                                  weight=0.5,
                                  color='black',
                                  fill=True,
                                  fill_color='white',
                                  fill_opacity=0.2
                                  ).add_to(max_circle_map)
                path = self.file_path+'maxed_circle_map.html'
                html_page = f'{path}'
                max_circle_map.save(html_page)
        else:
            print('No circles maxed out!')


    def import_maxed_circles(self, in_path, make_map=True):

        # Read file to find what circles hit the maximum threshold.
        df = pd.read_csv(in_path)
        df = df[df.used == 0]

        # Some lines for testing
        # df = df[df.cat == 'restaurant']  # test on one category to check circle deduping is working.
        # df = df.sample(1)  # REMOVE WHEN USING AGAIN

        # For each circle which hit the max threshold, return 9 smaller circles which cover the old one.
        new_circles = []
        for lat, lon, rad, cat, used in df.values.tolist():
            n = rad * (2 / 3)
            directions = [-1, 0, 1]
            rad_new = (1 / 3) * ((2 * (rad ** 2)) ** 0.5)
            # n = rad_new/2
            for shift in directions:
                for shift2 in directions:
                    de = shift * n
                    dn = shift2 * n
                    lat_new, lon_new = shift_n_meters(lat=lat, lon=lon, dn=dn, de=de)
                    new_circles.append([lat_new, lon_new, rad_new, cat])

        # Remove duplicate circles - where 2 maxed circles touch, and hence 2 of the new smaller circles are almost identical
        # As these are not exact duplicates, they need to be removed and replaced with 1 slightly larger circle
        new_circles_copy = new_circles
        df_new_circles = pd.DataFrame(new_circles, columns=['lat', 'lon', 'rad', 'cat'])
        unique_cat = df_new_circles.cat.unique()
        for cat1 in unique_cat:
            df_new_circles_filtered = df_new_circles[df_new_circles.cat == cat1]
            unique_rad = df_new_circles_filtered.rad.unique()
            # print(len(unique_rad), 'Unique_rad')
            time.sleep(0.5)
            for radius1 in tqdm(unique_rad):
                # print('\nRadius = {}\n'.format(radius1))
                df_new_circles_filtered_2 = df_new_circles_filtered[df_new_circles_filtered.rad == radius1]
                for i, [lat, lon, rad, cat] in enumerate(df_new_circles_filtered_2.values.tolist()):
                    for j, [lat1, lon1, rad1, cat1] in enumerate(df_new_circles_filtered_2.values.tolist()):
                        if i < j:
                            d = find_distance(c1=[lat, lon], c2=[lat1, lon1])
                            # print(d)
                            if d < rad:
                                # print(d)
                                removes = 0
                                if [lat, lon, rad, cat] in new_circles_copy:
                                    new_circles_copy.remove([lat, lon, rad, cat])
                                    removes += 1
                                if [lat1, lon1, rad1, cat1] in new_circles_copy:
                                    new_circles_copy.remove([lat1, lon1, rad1, cat1])
                                    removes += 1
                                if removes == 2:
                                    new_circles_copy.append([lat, lon, rad, cat])

        if make_map:
            map2 = folium.Map(location=[new_circles_copy[0][0], new_circles_copy[0][1]], zoom_start=5)
            for lat, lon, rad, cat, used in df.values.tolist():
                folium.Circle((lat, lon), radius=rad,
                              line_opacity=1,
                              weight=0.5,
                              color='black',
                              fill=True,
                              fill_color='black',
                              fill_opacity=0.2
                              ).add_to(map2)
            for lat, lon, rad, cat in new_circles_copy:
                folium.Circle((lat, lon), radius=rad,
                              line_opacity=1,
                              weight=0.5,
                              color='red',
                              fill=True,
                              fill_color='white',
                              fill_opacity=0.2
                              ).add_to(map2)
            path = self.file_path+'maxed_circle_covered_map.html'
            html_page = f'{path}'
            map2.save(html_page)

        self._circle_geometries = new_circles_copy

        print('Ready to collect {0} new circles for ${1}'.format(str(len(new_circles_copy)),
                                                                 str(len(new_circles_copy) * 0.04)))
        self.exclude_used_circles()

        if check_you_want_to():
            print('Writing first pages of Circles...')
            for lat, long, rad, cat in tqdm(self._circle_geometries):
                self._write_circle_to_json_1(lat=lat,
                                             long=long,
                                             cat=cat,
                                             rad=rad)
            time.sleep(5)
            self._circle_geometries.clear()
            print('Writing second pages of Circles...')
            in_file_path = self.file_path+'page_1//*.json'
            for file in tqdm(glob.glob(in_file_path)):
                data = json.load(open(file))
                lat, long, rad, cat, status, next_page_token, total = data.values()
                if next_page_token:
                    self._write_circle_to_json_2(lat=lat,
                                                 long=long,
                                                 cat=cat,
                                                 rad=rad,
                                                 next_page_token=next_page_token)


            time.sleep(5)
            print('Writing third pages of Circles...')
            in_file_path = self.file_path+'page_2//*.json'
            for file in tqdm(glob.glob(in_file_path)):
                data = json.load(open(file))
                lat, long, rad, cat, status, next_page_token, total = data.values()
                if next_page_token:
                    maxed = self._write_circle_to_json_3(lat=lat,
                                                         long=long,
                                                         cat=cat,
                                                         rad=rad,
                                                         second_next_page_token=next_page_token)
                    if maxed == 1:
                        self.maxed_circles.append([lat, long, rad, cat])
            print(len(self.maxed_circles), 'Still Maxed out\nUpdating maxed circle CSV...')
            df1 = pd.DataFrame(self.maxed_circles, columns=['lat', 'lon', 'rad', 'cat'])
            df1['used'] = 0
            df['used'] = 1
            df = pd.concat([df, df1])
            df.to_csv(in_path, index=False)


    @staticmethod
    def _create_master():
        return pd.DataFrame(
            columns=[
                'lat', 'lon', 'name', 'gplace_id', 'price_level', 'rating', 'rating_count',
                'location', 'loc_code', 'business_status', 'cat_1', 'cat_2', 'cat_3', 'cat_4',
                'cat_5', 'cat_6', 'cat_7', 'cat_8', 'cat_9', 'cat_10', 'cat_11', 'cat_12',
                'cat_13', 'cat_14', 'cat_15', 'new_data_flag', 'collected'
            ]
        )

    @staticmethod
    def _population_density_to_radius_function(region, density, cats):
        """
        Equation to take asymptotic format to manage radii. Generated to rougly fit estimations of
        apt circle size
        """
        # Some regions that cause problems when calculating radius.
        if 'store' in cats:
            cat_scale = 0.75
        else:
            cat_scale = 0.5

        problem_regions_1 = ['Moray', 'Carmarthenshire', 'Gwynedd', 'Cornwall', 'East Riding of Yorkshire',
                             'Kingston upon Hull, City of', 'Boston', 'Shropshire', 'Wiltshire', 'Belfast',
                             'Derry City and Strabane', 'County Durham']
        problem_regions_2 = ['Northumberland', 'Belfast', 'Aberdeenshire', 'Argyll and Bute', 'Dumfries and Galloway',
                             'Na h-Eileanan Siar', 'Highland', 'Perth and Kinross', 'Scottish Borders', 'Powys']

        if region in problem_regions_1:
            scaler = 1
        elif region in problem_regions_2:
            scaler = 0.75
        #elif region == 'Brighton and Hove':
        #    scaler = 0.3
        else:
            scaler = 1.5  # normally 1.5
        return ((-4200 + (10500 / (1 + (density / 15750) ** 0.4))) / scaler) * cat_scale

    @staticmethod
    def _adjust_radius(rad, category):
        """
        adjust radius based on category
        """
        return int(rad / CATEGORY_RADIUS_MAP[category])

    def _get_or_create_master(self, path):
        response = None
        while response != 'n' and response != 'y':
            response = input('Did you write the previous batch to master? y/n')
            if response == 'y':
                try:
                    master = pd.read_csv(path)
                    master['new_data_flag'] = 0
                except FileNotFoundError:
                    print('File not found, creating new master...')
                    master = self._create_master()
            elif response == 'n':
                try:
                    master = pd.read_csv(path)
                except FileNotFoundError:
                    print('File not found, creating new master...')
                    master = self._create_master()
            else:
                print('Invalid response, try again...')

        return master
    """
    
    @staticmethod
    def _create_flood_data(postcode_data_path, flood_data_path):
        geog_data = gpd.read_file(postcode_data_path)
        flood_risk_data = pd.read_csv(flood_data_path)
        df = pd.merge(geog_data, flood_risk_data, how='left', left_on='name', right_on='postal_sector')
        df['crime_accept'] = df['crime_rating'].apply(lambda x: 1 if x in ['A', 'B', 'C', 'D'] else 0)
        df['flood_accept'] = df['flood_score'].apply(lambda x: 1 if x >= 0.9 else 0)
        df['accept'] = df['flood_accept'] + df['crime_accept']
        df['accept'] = df['accept'].apply(lambda x: 1 if x == 2 else 0)
        return df[['name', 'accept', 'geometry']]
    """
