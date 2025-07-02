import os
import requests
import pandas as pd
from datetime import datetime
import logging


def extract_meteo(city: str, date: str) -> bool:
    try:
        # . RECUPERATION DES LATITUDES ET LONGITUDES DE LA VILLE
        ulr_to_fetch_city_lat_lon = "https://geocoding-api.open-meteo.com/v1/search"
        lat_lon_params = {
            "name": city,
            "count": 10,
            "language": "end",
            "format": "json",
        }
        city_geocoding = requests.get(
            ulr_to_fetch_city_lat_lon, params=lat_lon_params, timeout=10
        )
        city_geocoding.raise_for_status()
        lat_lon_response = city_geocoding.json()  # * reponse pour lat et lon
        city_coordinates = {
            "name": lat_lon_response["results"][0]["name"],
            "lat": lat_lon_response["results"][0]["latitude"],
            "lon": lat_lon_response["results"][0]["longitude"],
        }
        logging.info(city_coordinates)

        # . RECUPERATION DES DONNEES METEO HISTORIQUE DE LA VILLE VIA LATITUDE ET LONGITUDE
        url_to_fetch_city_meteo_archive_per_dates = (
            "https://archive-api.open-meteo.com/v1/archive"
        )
        archive_params = {
            "longitude": city_coordinates["lon"],
            "latitude": city_coordinates["lat"],
            "start_date": "2023-01-01",
            "end_date": datetime.now().strftime("%Y-%m-%d"),
            "daily": "temperature_2m_max,temperature_2m_min,wind_speed_10m_max,sunrise,sunset,rain_sum,snowfall_sum,cloud_cover_min,cloud_cover_max",
            "timezone": "Europe/Moscow",
        }
        archive_response = requests.get(
            url_to_fetch_city_meteo_archive_per_dates,
            params=archive_params,
            timeout=10,
        )
        archive_response.raise_for_status()
        archive_data = archive_response.json()
        city_meteo_archive_data = {  # TODO A VERIFIER DANS LA RESPONSE
            "ville": city,
            "date_extraction": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "daily_time": archive_data["daily"]["time"],
            "daily_temperature_2m_max": archive_data["daily"]["temperature_2m_max"],
            "daily_temperature_2m_min": archive_data["daily"]["temperature_2m_min"],
            "sunrise": archive_data["daily"]["sunrise"],
            "sunset": archive_data["daily"]["sunset"],
            "rain_sum": archive_data["daily"]["rain_sum"],
            "snowfall_sum": archive_data["daily"]["snowfall_sum"],
            "cloud_cover_min": archive_data["daily"]["cloud_cover_min"],
            "cloud_cover_max": archive_data["daily"]["cloud_cover_max"],
        }

        # . RECUPERATION DES DONNEES CURRENT FORECAST DE LA VILLE
        url_to_fetch_city_meteo_forecast = "https://api.open-meteo.com/v1/forecast"
        city_meteo_forecast_params = {
            "latitude": city_coordinates["lat"],
            "longitude": city_coordinates["lon"],
            "daily": "sunrise,sunset",
            "hourly": "rain,showers,snowfall,temperature_2m,relative_humidity_2m,cloud_cover",
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,rain,precipitation,snowfall,showers,cloud_cover",
            "timezone": "Europe/Moscow",
        }
        city_meteo_forecast_response = requests.get(
            url_to_fetch_city_meteo_forecast, city_meteo_forecast_params, timeout=10
        )
        city_meteo_forecast_response.raise_for_status()
        city_meteo_forecast_json = city_meteo_forecast_response.json()
        city_meteo_forecast_data = {
            "expasions_date": 7,
            "current_time": city_meteo_forecast_json["current"]["time"],
            "current_temperature_2m": city_meteo_forecast_json["current"][
                "temperature_2m"
            ],
            "current_relative_humidity_2m": city_meteo_forecast_json["current"][
                "relative_humidity_2m"
            ],
            "current_wind_speed_10m": city_meteo_forecast_json["current"][
                "wind_speed_10m"
            ],
            "current_rain": city_meteo_forecast_json["current"]["rain"],
            "current_precipitation": city_meteo_forecast_json["current"][
                "precipitation"
            ],
            "current_snowfall": city_meteo_forecast_json["current"]["snowfall"],
            "current_showers": city_meteo_forecast_json["current"]["showers"],
            "current_cloud_cover": city_meteo_forecast_json["current"]["cloud_cover"],
            "hourly_time": city_meteo_forecast_json["hourly"]["time"],
            "hourly_rain": city_meteo_forecast_json["hourly"]["rain"],
            "hourly_showers": city_meteo_forecast_json["hourly"]["showers"],
            "hourly_snowfall": city_meteo_forecast_json["hourly"]["snowfall"],
            "hourly_temperature_2m": city_meteo_forecast_json["hourly"][
                "temperature_2m"
            ],
            "hourly_relative_humidity_2m": city_meteo_forecast_json["hourly"][
                "relative_humidity_2m"
            ],
            "hourly_cloud_cover": city_meteo_forecast_json["hourly"]["cloud_cover"],
            "daily_time": city_meteo_forecast_json["daily"]["time"],
            "daily_sunrise": city_meteo_forecast_json["daily"]["sunrise"],
            "daily_sunset": city_meteo_forecast_json["daily"]["sunset"],
        }
        logging.info(city_meteo_forecast_data)

        # . CREATON DES DOSSIERS DE DESTINATION
        os.makedirs(f"data/raw/forecast/{date}", exist_ok=True)
        os.makedirs(f"data/raw/archive/{date}", exist_ok=True)

        # . SAUVEGARDE DES DONNEES
        pd.DataFrame([city_meteo_archive_data]).to_csv(
            f"data/raw/archive/{date}/archive_meteo_{city}.csv",
            index=False,  # Pas d'enregistrement de l'index
        )
        pd.DataFrame([city_meteo_forecast_data]).to_csv(
            f"data/raw/forecast/{date}/forecast_meteo_{city}.csv",
            index=False,  # Pas d'enregistrement de l'index
        )
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau/API pour {city}: {str(e)}")
    except KeyError as e:
        logging.error(f"Champ manquant dans la réponse pour {city}: {str(e)}")
    except Exception as e:
        logging.error(f"Erreur inattendue pour {city}: {str(e)}")

    return False
