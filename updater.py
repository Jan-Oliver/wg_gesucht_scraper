import requests
import time
import random
import os
import sys

import pandas as pd
import numpy as np

from bs4 import BeautifulSoup
from datetime import datetime
from util import database_table

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

class Updater():
  """Updates "is_active" column of dataframe.

  By iterating over all currently published ads, a list of currently acitve ads is created.
  This list is then used to update the "is_active" column of the dataframe.
  For the correspondance between the published ads on the website and the entries 
  in the dataframe, the "ad_id" column is utilized. A base url consists of two parts,
  where the beginning is everything to the part in the url that is used to change the page number,
  and the end is everything afterwards.

    Typical usage example:
    base_url_start = <Start of url of city to compare df with>
    base_url_end = <End of url of city to compare df with>
    city_name = <Name of city in dataframe column "city_name" to update>
    database_tablename = <Name of table in database> 
    df = <call to database, only fetch active data from city that corresponds to base_url>
    
    updater = Updater(database_tablename,city_name)
    df_updated = updater.update(df,base_url_start,base_url_end)
  """
  def __init__(self, database_tablename: str, city_name: str):
    """ Init database writer """
    self._city_name = city_name
    self._writer = database_table.DatabaseTable(database_tablename)

  def update(self, base_url_start: str, base_url_end: str):
    """Checks if ads are still active.

    Checks if each row in the dataframe is still active on the website. 
    If not the column "is_active" is set to False.

    Args:
        base_url_start: Beginning of base url which is used to iterate over each page.
        base_url_end: End of base url which is used to iterate over each page.
    """
    print("Starting update!")
    # Get all ads from database!
    df = self._writer.get_dataframe()
    # Only use rows with correct city 
    df_city = df[df["city_name"] is self._city_name]

    # Get all ids of ads on the website.
    page_id = 0 # Pages start from zero.
    active_ad_id_list = list()
    while True:
      print("Currently on page: ", page_id)


      # Request html and init bs4 instance.
      base_url = base_url_start + str(page_id) + base_url_end
      page_id += 1
      r = requests.get(base_url)
      html = r.text
      soup = BeautifulSoup(html)


      # Parse data to get ad id of each element on page.
      ads_on_page = soup.find_all("tr",class_="offer_list_item")
      if not ads_on_page:
        print("Page does not contain any active entries!")
        break
      
      print("Found %d ads on page!" % len(ads_on_page))
      for ad in ads_on_page:
        # Validate if inactive ads are reached
        star_column = ad.find("td") 
        flatmates_column = star_column.find_next("td") 
        put_online_column = flatmates_column.find_next("td")
        put_online_content = put_online_column.find("span").text.strip()
        if "inaktiv" in put_online_content:
          print("Page contains first inactive entries!")
          break
        ad_id = int(ad["data-id"]) # Has to be int to compare to existing scraped ad ids
        active_ad_id_list.append(ad_id)
      time.sleep(5 + 30 * random.random()) # Process has to be slow to not get detected as bot!
    
    temp = df_city[~df_city['ad_id'].isin(active_ad_id_list)]
    print("Found a total of %d entries to be inactive!" % temp.shape[0])

    # Update dataframe is_active column
    # This means that ads can both be deactivated and alos reactivated
    df_city.loc[~df_city['ad_id'].isin(active_ad_id_list), 'is_active'] = False
    df_city.loc[~df_city['ad_id'].isin(active_ad_id_list), 'ts_deactivated'] = datetime.now()
    df_city.loc[df_city['ad_id'].isin(active_ad_id_list), 'is_active'] = True
    df_city.loc[df_city['ad_id'].isin(active_ad_id_list), 'ts_deactivated'] = np.Nan


    # Update rows in df that correspond to city
    df = df.set_index('ad_id')
    df.update(df_city.set_index('ad_id'))
    df = df.reset_index()

    # Push to database
    self._writer.upload_df_to_database(df,if_exists="replace")