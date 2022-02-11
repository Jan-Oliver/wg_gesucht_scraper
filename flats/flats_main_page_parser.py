import os
import sys
import warnings
import re
import pandas as pd
import numpy as np

from bs4 import BeautifulSoup
from datetime import datetime

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

class FlatsMainPageParser():
  """Parses overview page information.

  Converts the content of the flat-html overview page such as 
  "https://www.wg-gesucht.de/wg-zimmer-in-Munchen.90.0.0.1.html?noDeact=1"
  into a pandas Dataframe

    Typical usage example:
    html = <html of overview page using f.ex. requests>

    overview_page_parser = FlatsMainPageParser()
    df_parsed = overview_page_parser.parse(html)
  """
  def __init__(self):
    """ Default init """
    print("Overview Page Parser initialized!")
    pass

  def parse(self, html: str) -> pd.DataFrame:
    """Parses overview page content provided by html as string.

    Converts html to bs4 soup and extracts useful information.

    Args:
        html: HTML from overview page website.
    
    Returns:
        Pandas Dataframe with parsed information
    """
    soup = BeautifulSoup(html,features="html.parser")
    ads_on_page = soup.find_all("tr",class_="offer_list_item")
    df_list = list()
    for ad in ads_on_page:
      row_list = list()

      url = ad["adid"]
      url = "https://www.wg-gesucht.de/" + url
      ad_id = ad["data-id"]
      
      star_column = ad.find("td")   # Not important
      flatmates_column = star_column.find_next("td") 
      put_online_column = flatmates_column.find_next("td") # Not important -> A timestamp is used
      rent_column = put_online_column.find_next("td")
      room_size_column = rent_column.find_next("td")
      district_column = room_size_column.find_next("td") # Not important -> Scraped lateron
      free_from_column = district_column.find_next("td")
      free_until_column = free_from_column.find_next("td")
      # Parse flatmates column: -> How many people live in flat, which gender, what are they looking for
      flat_description = flatmates_column.find("span")["title"]
      flat_size, female_flatmates, male_flatmates, diverse_flatmates = re.match("(\d+)er WG \((\d+)w,(\d+)m,(\d+)d\)",flat_description).groups()
      looking_for_female = "Mitbewohnerin" in flatmates_column.find_all("img")[-1]["alt"].split(" ")
      looking_for_male = "Mitbwohner" in flatmates_column.find_all("img")[-1]["alt"].split(" ")  
      # Parse rent_column
      rent = rent_column.find("span").text.strip().replace("€","")
      try:
        rent = float(rent)
      except ValueError: # Not stated or "K.A."
        rent = None
      # Parse room_size
      room_size = room_size_column.find("span").text.strip().replace("m²","")
      try:
        room_size = float(room_size)
      except ValueError: # Not stated or "K.A."
        room_size = None
      # Parse free_from_column
      free_from_string = free_from_column.find("span").text.strip()
      # Parse free_until_column
      free_until_string = free_until_column.find("span")
      if(free_until_string is not None):
        free_until_string = free_until_string.text.strip()
      else:
        free_until_string = None

      # Other parameters
      is_active = True # Data was scraped and therefore ad is active
      ts_scraped = datetime.now()
      ts_deactivated = np.NAN

      row_list.append(ad_id)
      row_list.append(url)
      row_list.append(is_active)
      row_list.append(ts_scraped)
      row_list.append(ts_deactivated)
      row_list.append(flat_size)
      row_list.append(female_flatmates)
      row_list.append(male_flatmates)
      row_list.append(diverse_flatmates)
      row_list.append(looking_for_female)
      row_list.append(looking_for_male)
      row_list.append(rent)
      row_list.append(room_size)
      row_list.append(free_from_string)
      row_list.append(free_until_string)
      df_list.append(row_list)
    column_names = ["ad_id","url","is_active","ts_scraped","ts_deactivated","flat_size","female_flatmates","male_flatmates","diverse_flatmates",
                  "looking_for_female","looking_for_male","rent","room_size","free_from_string","free_until_string"]
      
    return pd.DataFrame(df_list,columns=column_names)