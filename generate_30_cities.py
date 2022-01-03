import asyncio
import time

import pandas as pd
from aiohttp import ClientSession, TCPConnector


async def load_url(url):
    async with ClientSession(connector=TCPConnector(ssl=False)) as session:
        async with session.get(url) as resp:
            print(resp.status)
            results = await resp.json()
            return results

coros = []
cities = [

    'London', 'Paris', 'Berlin', 'copenhagen', 'Lisbon', 'Budapest', 'Amsterdam', 'Madrid', 'Athens', 'Vienna',
    'Nairobi', 'Lagos', 'Accra', 'Cairo', 'Luanda', 'Durban', 'Lusaka', 'Maputo', 'Kigali', 'Dakar',
    'Bangkok', 'Tokyo', 'Jakarta', 'Mumbai', 'Shanghai', 'Manila', 'Hanoi', 'Osaka', 'Dhaka', 'Istanbul'

          ]
for city in cities:
    print(city)
    url = 'https://api.openweathermap.org/data/2.5/weather?q=%s&appid=71ba4ecb2c7b84dc75574ca6091a5952'%city
    coros.append(load_url(url))

extraction_columns = {
    'main.temp_min': 'min_temp',
    'main.temp_max': 'max_temp',
    'main.feels_like': 'Temperature Feel',
    'main.pressure': 'pressure',
    'main.humidity': 'humidity',
    'weather.main': 'clouds',
    'sys.sunrise': 'sunrise',
    'sys.sunset': 'sunset'
}


def extract(row, key):
    return row[key]


async def load(coros):
    results = pd.DataFrame(await asyncio.gather(*coros))
    exploded = results.explode("weather")

    for extraction_column_key in extraction_columns:
        key = extraction_column_key.split(".")
        column_name = key[0]
        key_name_in_column = key[1]
        result_column = extraction_columns[extraction_column_key]
        exploded[result_column] = exploded[column_name].apply(extract, key=key_name_in_column)

    exploded.to_csv("E:/task_di/Python Generate 30 Cities Weather through API/result.csv")

loop = asyncio.get_event_loop()
loop.run_until_complete(load(coros))

