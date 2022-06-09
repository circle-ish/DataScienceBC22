from pandas import DataFrame

### CITY 
def scrape_wiki_cities() -> DataFrame:
    from bs4 import BeautifulSoup
    import requests

    #get html code
    doc_url = 'https://en.wikipedia.org/wiki/List_of_cities_in_the_European_Union_by_population_within_city_limits'
    response = requests.get(doc_url)
    if response.status_code != 200:
        raise Exception(f'wikipedia returned code {response.status_code} for url = {doc_url}')
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.select('table.wikitable > tbody > tr')

    # prettify the names and take only selected ones
    header = [h.text.strip().replace(' ', '_').lower() for h in table[0].select('th')][1:-2]
    cities = [[cell.text.strip() for cell in city.select('td')[1:-2]] for city in table[1:]]

    import pandas as pd 
    return pd.DataFrame(data=cities, columns=header)

def cleanup_cities(df : DataFrame):
    import pandas as pd
    df.loc[:, 'officialpopulation'] = df['officialpopulation'].str.replace(',', '').astype(int)
    df.loc[:, 'date'] = pd.to_datetime(df['date'])
    
    
### WEATHER 
def scrape_weather(city_lst : list, openweather_key : str) -> DataFrame:
    weather_arguments = {
        'q' : '', #city
        'appid' : openweather_key, #api key
       # 'cnt' : '3', # number of results
        'units' : 'metric'
    }
    
    weather_lst = []
    for c in city_lst: 
        weather_arguments['q'] = c
        weather_json = get_weather(weather_arguments)

        keep_cols = ['city', 'dt_txt', 'main:temp', 'main:feels_like', 'main:humidity', 
                     'weather:0:description', 'clouds:all', 'wind:speed', 'wind:deg', 
                     'wind:gust', 'pop', 'rain:3h', 'snow:3h', 'sys:pod']
        weather_df = weather_json_to_df(weather_json, c, keep_cols)


        new_cols = ['city', 'date', 'temp_celcius', 'temp_feels_like_celcius', 'humidity_percent', 
                     'weather_description', 'clouds_percent', 'wind_speed_meter_sec', 'wind_direction_degree', 
                     'wind_gust_meter_sec', 'pop_percent', 'rain_3h_mm', 'snow_3h_mm', 'pod']
        weather_df = weather_df.rename(columns=dict(zip(keep_cols, new_cols)))
        cleanup_weather(weather_df)
        weather_lst.append(weather_df)
    df = None
    if weather_lst:
        import pandas as pd
        df = pd.concat(weather_lst, ignore_index=True)
    return df

def get_weather(weather_arguments : dict) -> dict:
    import requests
    import json
    #check that only string arguments are present
    if not all(isinstance(val, str) for val in weather_arguments.values()):
        raise Exception('all arguments must be string')
        
    #preparing the request url
    weather_api = "http://api.openweathermap.org/data/2.5/forecast?"
    api_arguments = repr(weather_arguments).replace("': '", '=').replace("', '", '&')[2:-2]
    weather_request = weather_api + api_arguments
    
    response = requests.get(weather_request)
    if response.status_code != 200:
        raise Exception(f'openweather returned code {response.status_code} for url = {weather_request}')
    return response.json()

def weather_json_to_df(weather_json : dict, city : str, keep_cols : list=None) -> DataFrame:
    # install flatdict; needed for weather_json_to_df()
    import sys, os
    from proj3_utils import install_pip_pkg

    #!pip3 install flatdict
    install_pip_pkg({'flatdict'})
    
    from flatdict import FlatterDict as flatten
    import pandas as pd
    
    #take weather data and city name
    weather_df = pd.json_normalize([dict(flatten(i)) for i in weather_json['list']])
    weather_df = weather_df.assign(city = [city]*weather_df.shape[0])
    
    # return only selection of columns
    if not keep_cols:
        return weather_df
    keep_cols = [c for c in keep_cols if c in weather_df.columns]
    return weather_df[keep_cols]

def cleanup_weather(df : DataFrame):
    import pandas as pd
    if 'rain_3h_mm' in df:
        df.loc[:, 'rain_3h_mm'] = df['rain_3h_mm'].fillna(0)
    if 'snow_3h_mm' in df:
        df.loc[:, 'snow_3h_mm'] = df['snow_3h_mm'].fillna(0)
    df.loc[:, 'date'] = pd.to_datetime(df['date'])
    

### AIRPORTS
def get_icao_args() -> list:
    querystring = {"withFlightInfoOnly":"true"}
    headers = {
      "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
      "X-RapidAPI-Key": "----needs key-----"
    }
    return [querystring, headers]

from pandas import DataFrame
def icao_airport_codes(
    latitudes : list, 
    longitudes : list, 
    aerodatabox_key : str,
    args : list = get_icao_args()) -> DataFrame:
    
    assert len(latitudes) == len(longitudes)
    querystring, headers = args
    headers['X-RapidAPI-Key'] = aerodatabox_key
    list_for_df = []

    for i in range(len(latitudes)):
        url = f"https://aerodatabox.p.rapidapi.com/airports/search/location/{latitudes[i]}/{longitudes[i]}/km/100/16"
        response = requests.request("GET", url, headers=headers, params=querystring)

        if response.status_code != 200:
            raise Exception(f'aerodatabox returned code {response.status_code} for url = {url}')
        list_for_df.append(pd.json_normalize(response.json()['items']))
        df = None
        if list_for_df:
            df = pd.concat(list_for_df, ignore_index=True)

    return df