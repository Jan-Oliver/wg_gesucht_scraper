import sys
import warnings
import os
from bs4 import BeautifulSoup

class FlatsAdPageParser():
  """Parses individual ad page information.

  Converts the content of the flat-html ad page into a pandas Dataframe

    Typical usage example:
    html = <html of page using requests>

    ad_parser = FlatsAdPageParser()
    df_parsed = ad_parser.parse(html)
  """
  def __init__(self):
    """ Default init """
    pass

  def parse(self, html: str) -> "tuple[str, str]":
    """Parses ad page content provided by html as string.

    Converts html to bs4 soup and extracts useful information.

    Args:
        html: HTML from ad page.
    
    Returns:
        Tuple with street and district information
    """
    soup = BeautifulSoup(html,features="html.parser")
    content = soup.find("div", class_="panel-body")
    address_details = content.find("div",class_="col-sm-4 mb10")
    address = address_details.find("a")
    
    list_address = address.text.split("\n")
    list_address = [i.strip() for i in list_address if i.strip() is not ""] # Only leave elements with data
    street_information = list_address[0]
    district_information = list_address[1]
    return (street_information,district_information)