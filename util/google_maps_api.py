import googlemaps

class GoogleMapsAPI():
  """ Encapsulates googlemaps Python API Client """
  
  def __init__(self, api_key: str):
    """ Init using API key to connect to service. """
    self._gmaps = googlemaps.Client(key=api_key)

  def get_address_lon_lat(self,address:str) -> "tuple[str,float,float]":
    """ Convert address string to formatted address string and lon, lat value

    Args:
        address (str): Address to convert

    Returns:
        [tuple]: Tuple of formatted address string, longitude, latitude
    """
    # Geocoding an address
    geocode_result = self._gmaps.geocode(address=address)
    geometry_information = geocode_result[0]['geometry']
    address_formatted = geocode_result[0]['formatted_address']
    location = geometry_information['location']
    lat = location['lat']
    lon = location['lng']
    return (address_formatted,lon,lat)

