import math
from collections.abc import Iterable
from urllib.parse import urlparse
import folium
import numpy as np
import pandas as pd
import requests


def plot_points(in_path, lon_col, lat_col, name_col, map_path, postcodes=None, postcode_col='Postcode'):

    df = pd.read_csv(in_path)

    if postcodes:
        df['short_pc'] = df[postcode_col].apply(lambda x: x[0:2] if type(x) == str else x)
        df = df[df['short_pc'].isin(postcodes)]
        print('There are {} places in these postcodes'.format(len(df)))
        print(df['short_pc'].value_counts())


    df = df[[lon_col, lat_col, name_col]].dropna().values.tolist()
    lon1, lat1, name1 = df[1]

    map1 = folium.Map(location=[lat1, lon1], zoom_start=10)

    for lon, lat, name in df:
        folium.Marker([lat, lon], popup=name).add_to(map1)

    html_page = f'{map_path}'
    map1.save(html_page)


def flatten_list(irregular_list):
    for element in irregular_list:
        if isinstance(element, Iterable) and not isinstance(element, (str, bytes)):
            yield from flatten_list(element)
        else:
            yield element


def get_clean_list(list_to_clean):
    return [i for i in flatten_list(list_to_clean) if i]


def clone_csv_and_sample(in_path, out_path, sample_size):
    data = pd.read_csv(in_path).sample(sample_size)
    data.to_csv(out_path)


def get_address_elements_from_google_json(data, addr_element):
    address_comps = data.get('address_components', [])
    for component in address_comps:
        if addr_element in component.get('types'):
            if len(component.get('long_name', '')) >= 2:
                return component['long_name']
            else:
                return component['short_name']


# Old Function Left for Reference.
def get_time_element_old(data, day, desired_status):
    periods = data.get('opening_hours', {}).get('periods', [])
    for period in periods:
        status = period.get(desired_status)
        if status:
            if status.get('day') == day:
                return status.get('time')


def get_time_element(data, day, desired_status):
    periods = data.get('opening_hours', {}).get('periods', [])
    # have to check it is the correct day by checking open day incase they close the following morning
    for period in periods:
        weekday = period.get('open').get('day')
        if weekday == day:
            return period.get(desired_status).get('time')


def get_weekday_time_element(data, weekday, desired_status):
    days = data.get('opening_hours', {}).get('weekday_text', [])
    if len(days) != 0:
        string = [day for day in days if weekday in day][0]
        if string == '{}: Open 24 hours'.format(weekday):
            if desired_status == 'open':
                return '0000'
            elif desired_status == 'close':
                return '2400'
        elif string == '{}: Closed'.format(weekday):
            return np.nan
        else:
            convert = {'Sunday': 0,
                      'Monday': 1,
                      'Tuesday': 2,
                      'Wednesday': 3,
                      'Thursday': 4,
                      'Friday': 5,
                      'Saturday': 6}
            return get_time_element(data, convert[weekday], desired_status)
    else:
        return np.nan



def shift_n_meters(lat, lon, dn, de):
    """
    Takes location and amount of desired displacement to return a new long and lat
    :param lat: latitude
    :param lon: longitude
    :param dn: displacement north
    :param de: displacement east
    :return: new lat, new long
    """
    lat = float(lat)
    lon = float(lon)
    # Earth’s radius, sphere
    R = 6378137
    # Coordinate offsets in radians
    dLat = dn / R
    dLon = de / (R * math.cos(math.pi * lat / 180))
    # OffsetPosition, decimal degrees
    lat_new = lat + dLat * 180 / math.pi
    lon_new = lon + dLon * 180 / math.pi

    return lat_new, lon_new


def find_distance(c1, c2):
    lat1, lon1 = c1
    lat2, lon2 = c2
    R = 6378137
    o1 = lat1 * math.pi/180
    o2 = lat2 * math.pi/180
    d1 = (lat2-lat1) * math.pi/180
    d2 = (lon2-lon1) * math.pi/180

    a = math.sin(d1/2) * math.sin(d1/2) + math.cos(o1) * math.cos(o2) * math.sin(d2/2) * math.sin(d2/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    d = R * c
    return d


def calc_required_lons(rad, lon_init, lon_final):
    lons = []
    while lon_init < lon_final:
        lons.append(lon_init)
        _, lon_init = shift_n_meters(0, lon_init, 0, (rad * 2) / (2 ** 0.5))
    return lons


def calc_required_lats(rad, lat_init, lat_final):
    lats = []
    while lat_init < lat_final:
        lats.append(lat_init)
        lat_init, _ = shift_n_meters(lat_init, 0, (rad * math.pi) / (2 ** 0.5), 0)
    return lats


def check_you_want_to():
    decision = input('Are you sure you want to go ahead? This may set you back a lot of cash!\ny / n ?')
    if decision == 'y':
        return True


def get_domain_data(path):
    return pd.read_csv(path, usecols=['location_website']).dropna()['location_website'].values.tolist()


def clean_domain_data(domain_string):
    stripped_domain = urlparse(domain_string).netloc
    if stripped_domain[0:4] == 'www.':
        stripped_domain = stripped_domain[4:]
    return stripped_domain


def request_fsa(name, location):
    url_string = 'http://api.ratings.food.gov.uk/Establishments?name={name}&address={address}'
    fsa_headers = {'accept': 'application/json', 'x-api-version': '2'}

    return requests.get(url_string.format(name=name, address=location), headers=fsa_headers)


def get_rating_from_fsa_request(name, location):
    locations = [location] + location.split(',')
    for location in locations:
        r = request_fsa(name, location)
        if r:
            json_response = r.json()
            establishments = json_response.get('establishments')
            if establishments:
                return establishments[0].get('RatingValue')
    return 'No Score'


def get_keywords(file_path):
    return pd.read_csv(file_path)


def flag(cat_string, name, kw):
    if type(cat_string) == str and type(name) == str:
        name = name.lower()
        flags = []

        # Check for generic flag/ chain words.
        for word in list(kw['chain'].dropna()):
            if word.lower() in name:
                flags.append('chain')
        for word in list(kw['flags'].dropna()):
            if word.lower() in name:
                flags.append('flag')
        for word in list(kw.exact.dropna()):
            if word.lower() in name + '.':
                flags.append('flag2')

        # If it is a restaurant, check for more keywords
        if 'restaurant' in cat_string:
            for word in list(kw['asian'].dropna()):
                if word.lower() in name:
                    flags.append('asian_restaurant')
            for word in list(kw['pub'].dropna()):
                if word.lower() in name:
                    flags.append('pub')
            for word in list(kw['takeaway'].dropna()):
                if word.lower() in name:
                    flags.append('takeaway')

        # If it is a clothing store, check for more keywords
        if 'clothing' in cat_string:
            for word in list(kw['designer'].dropna()):
                if word.lower() in name:
                    flags.append('designer')

        # If a store, check for more.
        if 'store' in cat_string:
            for word in list(kw.stores.dropna()):
                if word in name:
                    flags.append('bad_store')

        # Return NAN if no flags found, otherwise return a string of the flags found.
        flags = np.unique(flags)
        if len(flags) == 0:
            return np.nan
        else:
            flag_string = ''
            for flag1 in flags:
                flag_string += ' ' + flag1
            return flag_string.strip()
    else:
        return np.nan
