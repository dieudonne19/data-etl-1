import pandas as pd
import os
from pathlib import Path
import logging


def merge_files(date: str) -> str:
    home_dir = Path.home()
    common_dir = home_dir / "airflow" / "dags" / "tourisme" / "data"
    processed_dir = common_dir / "processed"
    # . Ajoute les nouvelles données au fichier global existant
    input_dir_archive = common_dir / "raw" / "archive" / date
    input_dir_forecast = common_dir / "raw" / "forecast" / date

    output_file_archive = processed_dir / "archive_meteo_global.csv"
    output_file_forecast = processed_dir / "forecast_meteo_global.csv"
    output_file_current = processed_dir / "current_meteo_global.csv"

    # . Créer le dossier processed si inexistant
    processed_dir.mkdir(parents=True, exist_ok=True)

    # . Vérifier que le dossier d'entrée existe
    if not Path.exists(input_dir_archive) or not Path.exists(input_dir_forecast):
        raise FileNotFoundError(
            f"Le dossier d'entrée pour le fichier archivé {input_dir_archive} ou {input_dir_forecast} n'existe pas. "
            "Vérifiez que les tâches d'extraction ont bien fonctionné."
        )

    # . Charger les données existantes (si fichier existe)
    if (
        Path.exists(output_file_archive)
        and Path.exists(output_file_forecast)
        and Path.exists(output_file_current)
    ):
        archive_global_df = pd.read_csv(output_file_archive)
        forecast_global_df = pd.read_csv(output_file_forecast)
        current_global_df = pd.read_csv(output_file_current)
    else:
        archive_global_df = pd.DataFrame()
        forecast_global_df = pd.DataFrame()
        current_global_df = pd.DataFrame()

    # . Lire les nouveaux fichiers
    archive_new_data = []
    forecast_new_data = []
    current_new_data = []
    for file in Path.iterdir(input_dir_archive):
        if file.name.startswith("archive_meteo_") and file.name.endswith(".csv"):
            archive_new_data.append(
                pd.read_csv(
                    f"/home/peace/airflow/dags/tourisme/data/raw/archive/{date}/{file.name}"
                )
            )
    for file in Path.iterdir(input_dir_forecast):
        if file.name.startswith("forecast_meteo_") and file.name.endswith(".csv"):
            forecast_new_data.append(
                pd.read_csv(
                    f"/home/peace/airflow/dags/tourisme/data/raw/forecast/{date}/{file.name}"
                )
            )
        elif file.name.startswith("current_meteo_") and file.name.endswith(".csv"):
            current_new_data.append(
                pd.read_csv(
                    f"/home/peace/airflow/dags/tourisme/data/raw/forecast/{date}/{file.name}"
                )
            )

    if not archive_new_data or not forecast_new_data or not current_new_data:
        raise ValueError(f"Aucune nouvelle donnée à fusionner pour {date}")

    # . Concaténation et déduplication
    archive_updated_df = pd.concat(
        [archive_global_df] + archive_new_data, ignore_index=True
    )
    archive_updated_df = archive_updated_df.drop_duplicates(
        subset=[
            "city",
            "date",
            "latitude",
            "longitude",
            "sunrise",
            "sunset",
            "rain_sum",
            "snowfall_sum",
            "temperature_2m_min",
            "temperature_2m_max",
        ],  # Clé unique
        keep="last",  # Garde la dernière version
    )
    forecast_updated_df = pd.concat(
        [forecast_global_df] + forecast_new_data, ignore_index=True
    )
    forecast_updated_df = forecast_updated_df.drop_duplicates(
        subset=[
            "city",
            "latitude",
            "longitude",
            "weather_code",
            "rain",
            "temperature_2m",
            "cloud_cover",
        ],  # Clé unique
        keep="last",  # Garde la dernière version
    )
    current_updated_df = pd.concat(
        [current_global_df] + current_new_data, ignore_index=True
    )
    current_updated_df = current_updated_df.drop_duplicates(
        subset=[
            "city",
            "latitude",
            "longitude",
            "weather_code",
            "temperature_2m",
            "rain",
            "snowfall",
            "cloud_cover",
        ],  # Clé unique
        keep="last",  # Garde la dernière version
    )

    # .Sauvegarde
    archive_updated_df.to_csv(output_file_archive, index=False)
    forecast_updated_df.to_csv(output_file_forecast, index=False)
    current_updated_df.to_csv(output_file_current, index=False)
    return str(output_file_archive), str(output_file_forecast), str(output_file_current)
