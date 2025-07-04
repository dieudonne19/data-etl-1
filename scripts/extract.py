import os
import requests
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path


def extract_meteo(city: str, date: str) -> bool:
    try:
        home_dir = Path.home()
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
        # logging.info(city_coordinates)

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
            # "ville": city,
            # "date_extraction": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "daily": archive_data["daily"],
        }

        # . RECUPERATION DES DONNEES CURRENT forecast DE LA VILLE
        url_to_fetch_city_meteo_forecast = "https://api.open-meteo.com/v1/forecast"
        city_meteo_forecast_params = {
            "latitude": city_coordinates["lat"],
            "longitude": city_coordinates["lon"],
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,rain,precipitation,snowfall,showers,cloud_cover,weather_code",
            "hourly": "rain,showers,snowfall,temperature_2m,relative_humidity_2m,cloud_cover,weather_code",
            "timezone": "Europe/Moscow",
            "forecast_days": 5,
        }
        # "daily": "sunrise,sunset",
        city_meteo_forecast_response = requests.get(
            url_to_fetch_city_meteo_forecast, city_meteo_forecast_params, timeout=10
        )
        city_meteo_forecast_response.raise_for_status()
        city_meteo_forecast_json = city_meteo_forecast_response.json()
        city_meteo_forecast_data = {
            "current": city_meteo_forecast_json["current"],
            "hourly": city_meteo_forecast_json["hourly"],
            # "daily": city_meteo_forecast_json["daily"],
        }
        # logging.info(city_meteo_forecast_data["hourly"])

        # . CREATON DES DOSSIERS DE DESTINATION
        forecast_dir = (
            home_dir
            / "airflow"
            / "dags"
            / "tourisme"
            / "data"
            / "raw"
            / "forecast"
            / date
        )
        archive_dir = (
            home_dir
            / "airflow"
            / "dags"
            / "tourisme"
            / "data"
            / "raw"
            / "archive"
            / date
        )

        forecast_dir.mkdir(parents=True, exist_ok=True)
        archive_dir.mkdir(parents=True, exist_ok=True)

        forecast_csv_path = forecast_dir / f"forecast_meteo_{city}.csv"
        current_csv_path = forecast_dir / f"current_meteo_{city}.csv"
        archive_csv_path = archive_dir / f"archive_meteo_{city}.csv"

        # . SAUVEGARDE DES DONNEES
        pd.DataFrame(
            {
                "city": city,
                "latitude": city_coordinates["lat"],
                "longitude": city_coordinates["lon"],
                "date": city_meteo_archive_data["daily"]["time"],
                "sunrise": city_meteo_archive_data["daily"]["sunrise"],
                "sunset": city_meteo_archive_data["daily"]["sunset"],
                "rain_sum": city_meteo_archive_data["daily"]["rain_sum"],
                "snowfall_sum": city_meteo_archive_data["daily"]["snowfall_sum"],
                "temperature_2m_min": city_meteo_archive_data["daily"][
                    "temperature_2m_min"
                ],
                "temperature_2m_max": city_meteo_archive_data["daily"][
                    "temperature_2m_max"
                ],
            }
        ).to_csv(
            archive_csv_path,
            index=False,  # Pas d'enregistrement de l'index
        )
        pd.DataFrame(
            {
                "city": city,
                # "date": city_meteo_forecast_data["current"]["time"],
                "latitude": city_coordinates["lat"],
                "longitude": city_coordinates["lon"],
                "time": city_meteo_forecast_data["hourly"]["time"],
                "weather_code": city_meteo_forecast_data["hourly"]["weather_code"],
                "rain": city_meteo_forecast_data["hourly"]["rain"],
                "temperature_2m": city_meteo_forecast_data["hourly"]["temperature_2m"],
                "cloud_cover": city_meteo_forecast_data["hourly"]["cloud_cover"],
            }
        ).to_csv(
            forecast_csv_path,
            index=False,  # Pas d'enregistrement de l'index
        )
        pd.DataFrame(
            {
                "city": city,
                "latitude": city_coordinates["lat"],
                "longitude": city_coordinates["lon"],
                "weather_code": city_meteo_forecast_data["current"]["weather_code"],
                "temperature_2m": [
                    city_meteo_forecast_data["current"]["temperature_2m"]
                ],
                "rain": [city_meteo_forecast_data["current"]["rain"]],
                "snowfall": [city_meteo_forecast_data["current"]["snowfall"]],
                "cloud_cover": [city_meteo_forecast_data["current"]["cloud_cover"]],
            }
        ).to_csv(
            current_csv_path,
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
