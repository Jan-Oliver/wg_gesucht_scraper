# WG-Gesucht Data Scraper

Scrapes und updates data for a specific city from the [Wg Gesucht Website](https://www.wg-gesucht.de/) and automatically uploads it to database.

At the moment only flats are supported. Other ad types such as Apartments will be suported in the future.

## Installation
- Create a virtual environemnt
- Activate environment
- Install `requirements.txt`

```sh
python3 -m venv wg_gesucht_scraper_env
source wg_gesucht_scraper_env/bin/activate
pip install -r requirements.txt
```

- Fill out `database_config.json` to establish connection to your database.

```json
{
  "dialect_driver": "...",
  "user": "...",
  "password": "...",
  "hostname": "...",
  "dbname": "..."
}
```

## Usage

The 'main.py' file encapsulates all information the user has to change.

### Name of datatable
Change the `DATATABLE_DATABASE` constant to whatever you want. 

### Specify which city to scrape

Example for Munich to clarify the usage. Other examples are also provided in the `main.py` to give a head start.

```python
base_url_to_scrape = base_url_munich
base_url_start_to_scrape = base_url_munich_start
base_url_end_to_scrape = base_url_munich_end
city_name_to_scrape = city_name_frankfurt
```
