import dotenv
# import geopandas as gpd
import sys
import pandas as pd
from GoogleApiCollector.core import GoogleMapCollector
from pathlib import Path
from GoogleApiCollector.extra import ContactDetailCollector
from data.ExpressionGoogleCategoryMapping import CORE_CATEGORY_MAP
from utils.utils import get_clean_list


if __name__ == '__main__':

    """
    version of the run file to be run on an EC2 Instance
    - needed to run on EC2 as you have to run from top level. 
    """


    # Load dotenv to enable api from os.environ
    dotenv.load_dotenv()

    # bring in params for search query, we want them in specific format described in core.py
    # here we select 5 random cities. If you wanted to use normally, just remove sample(5)
    # the set_index and to_dict allows us to place in correct format
    #data_path = 'data/population_densities/population_densities.gpkg'
    #params = gpd.read_file(data_path, layer='regions')
    #params['pop_density'] = params['pop_density'].apply(lambda x: 200 if x < 200 else x)

    #params = params[params.name.isin(['Brighton and Hove'])]  # Use for testing  # Manchester

    #params = params.set_index('name').to_dict('index')



    # we reference a master .csv file to handle removal of chains and dupes
    # ensure you always write any new data here using collector.write_to_master
    # otherwise, we won't keep learning about trades
    path_to_master = 'data/master_dataset_2022.csv'  # '../../data/master_final_2021.csv'
    #postcode_data_path = 'data/postcodes/Sectors.shp'
    #flood_data_path = 'data/postal_sector_full_ratings.csv'
    # finally, we get a list of categories, this can take any format, as long as they match googles format
    # and as long as in a list or set
    chosen_cats = ['store']
    categories = set(get_clean_list(list(CORE_CATEGORY_MAP.values())))
    categories = set(cat for cat in categories if cat in chosen_cats)  # 'book_store'


    # we now instantiate our circle based map collector
    directory_path = 'collection/store/'
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    """
    collector = GoogleMapCollector(path=directory_path)


    # then we fit it with the params from above and categories - this will create a map and will store
    # in our collector class the longs, lats and radii that we want
    #log_to_file_path = 'all_scored_circles.csv'
    #collector.fit(params=params, categories=categories, postcode_data_path=postcode_data_path,
    #              flood_data_path=flood_data_path, log_to_file_path=log_to_file_path)


    # import saved circles to adjust overlap threshold.
    circle_data_path = 'all_scored_circles.csv'
    collector.import_circles(circle_data_path=circle_data_path, new_threshold=0, categories=categories)

    # write_raw collects and persists data from google's api to a raw directory in this file's directory - use with
    # caution!
    collector.write_raw_2()

    # save any circles that hit the maximum threshold.
    max_circle_out_path = 'maxed_circles.csv'
    collector.save_maxed_circles_to_csv(out_path=max_circle_out_path, make_map=True)
    

    # write_to_master is an important final step to then persist all of our data to a master csv that we
    # can use for deduping
    collector.write_to_master(master_file_path=path_to_master)


    # tabulate will take the persisted .json data and tabulate it if we want to
    out_file_path = 'all_data_collected.csv'
    collector.tabulate(out_file_path=out_file_path, dedupe=True)
    
    """






    #### USE FOR RUNNING MAX CIRCLES  ##### RUN NUMBER 2

    dotenv.load_dotenv()

    maxed_circle_run_number = 7  # which iteration of the maxed circles is this run?
    print('Maxed Circle Run {}'.format(maxed_circle_run_number))

    # Where to store the files from this run
    directory_path_2 = directory_path+'maxed_circles_{}/'.format(maxed_circle_run_number)
    Path(directory_path_2).mkdir(parents=True, exist_ok=True)
    collector = GoogleMapCollector(path=directory_path_2)

    path_to_maxed_circles = directory_path+'maxed_circles.csv'

    collector.import_maxed_circles(in_path=path_to_maxed_circles, make_map=True)

    collector.write_to_master(master_file_path=path_to_master)

    out_file_path = 'all_data_collected_mc.csv'
    collector.tabulate(out_file_path=out_file_path)

    max_circle_out_path = 'maxed_circles.csv'
    collector.save_maxed_circles_to_csv(out_path=max_circle_out_path, make_map=True)
