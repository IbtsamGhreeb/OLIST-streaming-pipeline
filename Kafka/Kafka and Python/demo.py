import json
import requests
from quixstreams import Application
response = requests.get("https://api.open-meteo.com/v1/forecast", params = {
    "latitude": 30.0626,
    "longitude": 31.2497,
    "current": "temperature_2m"
})
weather = response.json()

app = Application(broker_address = "localhost:9092",
                  loglevel = "DEBUG")


with app.get_producer() as producer:
    producer.produce(
        topic = "weather_data_demo",
        key = "Cairo",
        value = json.dumps(weather)
    )