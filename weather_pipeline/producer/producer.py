# producer/producer.py

from kafka import KafkaProducer
import requests, json

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 0.5143,
        "longitude": 35.2698,
        "hourly": "temperature_2m,relative_humidity_2m"
    }

    response = requests.get(url, params=params)
    data = response.json()["hourly"]

    for i in range(len(data["time"])):
        message = {
            "time": data["time"][i],
            "temperature": data["temperature_2m"][i],
            "humidity": data["relative_humidity_2m"][i]
        }

        producer.send("weather_topic", value=message)

    producer.flush()