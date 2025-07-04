import pandas as pd
import os
from pathlib import Path


def transform_to_star() -> str:
    # . 1. Configuration des chemins
    archive_input_file = "/home/peace/airflow/dags/tourisme/data/processed/archive_meteo_global.csv"  # Fichier source archive
    forecast_input_file = "/home/peace/airflow/dags/tourisme/data/processed/forecast_meteo_global.csv"  # Fichier source forecast
    current_input_file = "/home/peace/airflow/dags/tourisme/data/processed/current_meteo_global.csv"  # Fichier source current
    output_dir = (
        "/home/peace/airflow/dags/tourisme/data/star_schema"  # Dossier de sortie
    )
    os.makedirs(output_dir, exist_ok=True)  # Crée le dossier si besoin

    # . 2. Chargement des données brutes
    archive_meteo_data = pd.read_csv(archive_input_file)
    forecast_meteo_data = pd.read_csv(forecast_input_file)
    current_meteo_data = pd.read_csv(current_input_file)

    # . 3. Gestion de la dimension Ville
    dim_ville_path = f"{output_dir}/dim_ville.csv"

    # . Charger ou initialiser la dimension
    if os.path.exists(dim_ville_path):
        dim_ville = pd.read_csv(dim_ville_path)
    else:
        dim_ville = pd.DataFrame(columns=["city_id", "city"])

    # . Identifier les nouvelles villes
    villes_existantes = set(dim_ville["city"])
    archive_nouvelles_villes = set(archive_meteo_data["city"]) - villes_existantes

    # . Ajouter les nouvelles villes avec des IDs incrémentaux
    if archive_nouvelles_villes:
        nouveau_id = dim_ville["city_id"].max() + 1 if not dim_ville.empty else 1
        archive_nouvelles_lignes = pd.DataFrame(
            {
                "city_id": range(
                    nouveau_id, nouveau_id + len(archive_nouvelles_villes)
                ),
                "city": list(archive_nouvelles_villes),
            }
        )
        dim_ville = pd.concat([dim_ville, archive_nouvelles_lignes], ignore_index=True)

        dim_ville.to_csv(dim_ville_path, index=False)  # . Sauvegarde

    # . 4. Création de la table de faits
    archive_faits_meteo = archive_meteo_data.merge(
        dim_ville, on="city", how="left"
    ).drop(columns=["city"])
    forecast_faits_meteo = forecast_meteo_data.merge(
        dim_ville, on="city", how="left"
    ).drop(columns=["city"])
    current_faits_meteo = current_meteo_data.merge(
        dim_ville, on="city", how="left"
    ).drop(columns=["city"])

    # . 5. Sauvegarde des faits
    archive_faits_path = f"{output_dir}/archive_fact_weather.csv"
    archive_faits_meteo.to_csv(archive_faits_path, index=False)

    forecast_faits_path = f"{output_dir}/forecast_fact_weather.csv"
    forecast_faits_meteo.to_csv(forecast_faits_path, index=False)

    # TRANSFORM forecast to have THE MEDIAN for the temperature, weather_code and cloud_cover per CITY
    tourisme_score_column = forecast_faits_meteo[
        ["time", "temperature_2m", "city_id", "weather_code", "rain"]
    ]
    # Calculate medians per city_id and create DataFrame
    tourism_faits_df = (
        tourisme_score_column.groupby("city_id")
        .agg({"temperature_2m": "median", "rain": "median", "weather_code": "median"})
        .reset_index()
    )
    tourism_faits_df.rename(
        columns={
            "temperature_2m": "median_temperature_per_city_id",
            "rain": "median_rain_per_city_id",
            "weather_code": "median_weather_code_per_city_id",
        },
        inplace=True,
    )

    # Example: Add more columns (e.g., mean temperature)
    # tourism_faits_df["mean_temperature_per_city_id"] = tourisme_score_column.groupby("city_id")["temperature_2m"].mean().values

    tourism_faits_path = f"{output_dir}/tourism_fact.csv"
    tourism_faits_df.to_csv(tourism_faits_path, index=False)

    current_faits_path = f"{output_dir}/current_fact_weather.csv"
    current_faits_meteo.to_csv(current_faits_path, index=False)

    return (
        archive_faits_path,
        forecast_faits_path,
        current_faits_path,
        tourism_faits_path,
    )
