"""Main entry point for all wg_gesucht scrapers

Handles the timing of different components. The actual data scraper is
executed every ~2 Minutes. Randomness is introduced to make the bot detection harder.
Every day at 00:00 am all ad pages are iterated over and the "is_active" column is updated. 
The user has to specify which city to scrape by changing the base_urls and names. Custom cities are
also useable. 
"""

import scraper
import updater
import time
import random
import schedule
import time

DATATABLE_DATABASE = "wg_gesucht_wg"
MAPS_API_KEY = "TODO"

base_url_munich = "https://www.wg-gesucht.de/wg-zimmer-in-Munchen.90.0.0.0.html?noDeact=1"
base_url_munich_start = "https://www.wg-gesucht.de/wg-zimmer-in-Munchen.90.0.0."
base_url_munich_end = ".html?noDeact=1"
city_name_munich = "munich"
base_url_berlin = "https://www.wg-gesucht.de/wg-zimmer-in-Berlin.8.0.0.0.html?noDeact=1"
base_url_berlin_start = "https://www.wg-gesucht.de/wg-zimmer-in-Berlin.8.0.0."
base_url_berlin_end = ".html?noDeact=1"
city_name_berlin = "berlin"
base_url_frankfurt = "https://www.wg-gesucht.de/wg-zimmer-in-Frankfurt-am-Main.41.0.0.0.html?noDeact=1"
base_url_frankfurt_start = "https://www.wg-gesucht.de/wg-zimmer-in-Frankfurt-am-Main.41.0.0."
base_url_frankfurt_end = ".html?noDeact=1"
city_name_frankfurt = "frankfurt"
base_url_dusseldorf = "https://www.wg-gesucht.de/wg-zimmer-in-Dusseldorf.30.0.0.0.html?noDeact=1"
base_url_dusseldorf_start = "https://www.wg-gesucht.de/wg-zimmer-in-Dusseldorf.30.0.0."
base_url_dusseldorf_end = ".html?noDeact=1"
city_name_dusseldorf = "duesseldorf"

base_url_to_scrape = base_url_munich
base_url_start_to_scrape = base_url_munich_start
base_url_end_to_scrape = base_url_munich_end
city_name_to_scrape = city_name_munich

def main():
  scraper_instance = scraper.Scraper(DATATABLE_DATABASE,MAPS_API_KEY)
  schedule.every().day.at("00:00").do(update,'Starting update!')
  while True:
    schedule.run_pending()
    scraper_instance.scrape(base_url_to_scrape, city_name_to_scrape)
    time.sleep(60 + 60 * random.random())

def update(t):
  updater_instance = updater.Updater(DATATABLE_DATABASE,city_name_to_scrape)
  updater_instance.update(base_url_start_to_scrape,base_url_end_to_scrape)
  return

if __name__ == "__main__":
  main()
