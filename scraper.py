import requests

import pandas as pd
import numpy as np

from flats import flats_main_page_parser
from flats import flats_ad_page_parser
from util import database_table
from util import google_maps_api

class Scraper():
  """Scrapes all new ads present on first page.

  All present ads on the first overview page are crawled and their ad_ids are compared
  to the already crawled entries from the database. If a new entry is detected it is added
  to the database.

    Typical usage example:
    base_url = <Url of overview page to scrape>
    city_name = <Name of city in dataframe column "city_name" to update>
    database_tablename = <Name of table in database> 
    google_maps_api_key = <API Key from google maps>

    scraper = Scraper(database_tablename,google_maps_api_key)
    df_updated = scraper.scrape(base_url,city_name)
  """
  def __init__(self,database_tablename: str, google_maps_api_key: str):
    """ Init with name of table in database and api key. """
    self._overview_page_scraper = flats_main_page_parser.FlatsMainPageParser()
    self._ad_scraper = flats_ad_page_parser.FlatsAdPageParser()
    self._writer = database_table.DatabaseTable(database_tablename)
    self._maps = google_maps_api.GoogleMapsAPI(google_maps_api_key)

  def scrape(self, base_url: str, city_name: str):
    """Scrape ads on first page and append to dataframe if not yet present.

    Args:
        base_url (str): Url of overview page to scrape
        city_name (str): Name of city. Set in column "city_name"
    """

    # Load existing ad ids as list
    # Returns empty list if table does not exist
    df_id_list = self._writer.get_existing_ad_ids() 

    # Request and parse overview page
    response = requests.get(base_url)
    html = response.text
    if html is None:
      print("Error: Get-request to base url %s does not yield any response!" % base_url)
      return None
    
    parsed_df = self._overview_page_scraper.parse(html)
    print("Found a total of %d ads on overview page." % (parsed_df.shape[0]))

    parsed_df["address_street"] = " "
    parsed_df["address_district"] = " "
    parsed_df["html"] = " "
    parsed_df["city_name"] = city_name
    parsed_df["address_string"] = " "
    parsed_df["lon"] = np.NaN
    parsed_df["lat"] = np.NaN

    # Validate which ads were not scraped yet by comparing the ad ids
    new_rows = list()
    counter = 0
    for index, row in parsed_df.iterrows():
      parsed_df_ad_id = int(row["ad_id"])
      parsed_df_ad_url = row["url"]
      if parsed_df_ad_id not in df_id_list:
        print("New ad found: Id is %d -  url is: %s" % (parsed_df_ad_id,parsed_df_ad_url))
        # Request html
        response = requests.get(parsed_df_ad_url)
        html = response.text
        # Parse html
        address_street, address_district = self._ad_scraper.parse(html)
        row["address_street"] = address_street
        row["address_district"] = address_district
        row["html"] = html
        # Get combined string, lon and lat
        bundled_address = address_street + " " + address_district
        address_string, lon,lat = self._maps.get_address_lon_lat(bundled_address)
        print("Converted address via maps API: %s - %f lon - %f lat" % (address_string,lon,lat))
        row["address_string"] = address_string
        row["lon"] = lon
        row["lat"] = lat
        new_rows.append(row)
        counter = counter+1

    print("New ads present: %d " % counter)
    if(counter > 0):
      new_rows_df = pd.DataFrame(new_rows)
      new_rows_df.reset_index()
      self._writer.upload_df_to_database(new_rows_df) # Append new rows!
