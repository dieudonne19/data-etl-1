import pandas as pd
import os


def merge_files(date: str) -> str:
    # . Ajoute les nouvelles données au fichier global existant
    input_dir_archive = f"data/raw/archive/{date}"
    input_dir_forecast = f"data/raw/forecast/{date}"
    output_file_archive = "data/processed/archive_meteo_global.csv"
    output_file_forecast = "data/processed/forecast_meteo_global.csv"

    # . Créer le dossier processed si inexistant
    os.makedirs(os.path.dirname(output_file_archive), exist_ok=True)
    os.makedirs(os.path.dirname(output_file_forecast), exist_ok=True)

    # . Vérifier que le dossier d'entrée existe
    if not os.path.exists(input_dir_archive) or not os.path.exists(input_dir_forecast):
        raise FileNotFoundError(
            f"Le dossier d'entrée pour le fichier archivé {input_dir_archive} ou {input_dir_forecast} n'existe pas. "
            "Vérifiez que les tâches d'extraction ont bien fonctionné."
        )

    # . Charger les données existantes (si fichier existe)
    if os.path.exists(output_file_archive) and os.path.exists(output_file_forecast):
        archive_global_df = pd.read_csv(output_file_archive)
        forecast_global_df = pd.read_csv(output_file_forecast)
    else:
        archive_global_df = pd.DataFrame()
        forecast_global_df = pd.DataFrame()

    # . Lire les nouveaux fichiers
    archive_new_data = []
    forecast_new_data = []
    for file in os.listdir(input_dir_archive):
        if file.startswith("archive_meteo_") and file.endswith(".csv"):
            archive_new_data.append(pd.read_csv(f"{input_dir_archive}/{file}"))
    for file in os.listdir(input_dir_forecast):
        if file.startswith("forecast_meteo_") and file.endswith(".csv"):
            forecast_new_data.append(pd.read_csv(f"{input_dir_forecast}/{file}"))

    if not archive_new_data or not forecast_new_data:
        raise ValueError(f"Aucune nouvelle donnée à fusionner pour {date}")

    # . Concaténation et déduplication
    archive_updated_df = pd.concat(
        [archive_global_df] + archive_new_data, ignore_index=True
    )
    archive_updated_df = archive_updated_df.drop_duplicates(
        subset=["ville", "date_extraction"],  # Clé unique
        keep="last",  # Garde la dernière version
    )
    forecast_updated_df = pd.concat(
        [forecast_global_df] + forecast_new_data, ignore_index=True
    )
    forecast_updated_df = archive_updated_df.drop_duplicates(
        subset=["ville", "date_extraction"],  # Clé unique
        keep="last",  # Garde la dernière version
    )

    # .Sauvegarde
    archive_updated_df.to_csv(output_file_archive, index=False)
    forecast_updated_df.to_csv(output_file_forecast, index=False)
    return output_file_archive, output_file_forecast
