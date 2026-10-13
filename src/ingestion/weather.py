from datetime import date, timedelta

from src.ingestion.common import get_logger, http_get, save_json

log = get_logger("weather")
URL = "https://archive-api.open-meteo.com/v1/archive"
CITIES = {
    "casablanca": (33.57, -7.59),
    "rabat": (34.02, -6.84),
    "marrakech": (31.63, -8.01),
    "agadir": (30.43, -9.60),
    "fes": (34.03, -5.00),
}


def fetch_city(city, start, end):
    lat, lon = CITIES[city]
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "daily": "temperature_2m_mean,precipitation_sum",
        "timezone": "Africa/Casablanca",
    }
    return http_get(URL, params).json()


def run(start="2015-01-01", end=None):
    end = end or (date.today() - timedelta(days=7)).isoformat()
    for city in CITIES:
        payload = fetch_city(city, start, end)
        payload["city"] = city
        save_json("weather", f"{city}_{start}_{end}.json", payload)


if __name__ == "__main__":
    run()
