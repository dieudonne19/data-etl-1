import pandas as pd
import os


def transform_to_star() -> str:
    """
    Transforme les données météo en schéma en étoile simplifié avec :
    - Une table de faits (mesures météo)
    - Une dimension ville (référence commune)

    Returns:
        str: Chemin vers le fichier des faits généré
    """

    # . 1. Configuration des chemins
    archive_input_file = (
        "data/processed/archive_meteo_global.csv"  # Fichier source archive
    )
    forecast_input_file = (
        "data/processed/forecast_meteo_global.csv"  # Fichier source forecast
    )
    output_dir = "data/star_schema"  # Dossier de sortie
    os.makedirs(output_dir, exist_ok=True)  # Crée le dossier si besoin

    # . 2. Chargement des données brutes
    archive_meteo_data = pd.read_csv(archive_input_file)
    forecast_meteo_data = pd.read_csv(forecast_input_file)

    # . 3. Gestion de la dimension Ville
    dim_ville_path = f"{output_dir}/dim_ville.csv"

    # . Charger ou initialiser la dimension
    if os.path.exists(dim_ville_path):
        dim_ville = pd.read_csv(dim_ville_path)
    else:
        dim_ville = pd.DataFrame(columns=["ville_id", "ville"])

    # . Identifier les nouvelles villes
    villes_existantes = set(dim_ville["ville"])
    archive_nouvelles_villes = set(archive_meteo_data["ville"]) - villes_existantes
    forecast_nouvelles_villes = set(forecast_meteo_data["ville"]) - villes_existantes

    # . Ajouter les nouvelles villes avec des IDs incrémentaux
    if archive_nouvelles_villes and forecast_nouvelles_villes:
        nouveau_id = dim_ville["ville_id"].max() + 1 if not dim_ville.empty else 1
        archive_nouvelles_lignes = pd.DataFrame(
            {
                "ville_id": range(
                    nouveau_id, nouveau_id + len(archive_nouvelles_villes)
                ),
                "ville": list(archive_nouvelles_villes),
            }
        )
        dim_ville = pd.concat([dim_ville, archive_nouvelles_lignes], ignore_index=True)

        forecast_nouvelles_lignes = pd.DataFrame(
            {
                "ville_id": range(
                    nouveau_id, nouveau_id + len(forecast_nouvelles_villes)
                ),
                "ville": list(forecast_nouvelles_villes),
            }
        )
        dim_ville = pd.concat([dim_ville, forecast_nouvelles_lignes], ignore_index=True)

        dim_ville.to_csv(dim_ville_path, index=False)  # . Sauvegarde

    # . 4. Création de la table de faits
    archive_faits_meteo = archive_meteo_data.merge(
        dim_ville, on="ville", how="left"
    ).drop(columns=["ville"])
    forecast_faits_meteo = forecast_meteo_data.merge(
        dim_ville, on="ville", how="left"
    ).drop(columns=["ville"])

    # . 5. Sauvegarde des faits
    archive_faits_path = f"{output_dir}/archive_fact_weather.csv"
    archive_faits_meteo.to_csv(archive_faits_path, index=False)

    forecast_faits_path = f"{output_dir}/forecast_fact_weather.csv"
    forecast_faits_meteo.to_csv(forecast_faits_path, index=False)

    return archive_faits_path, forecast_faits_meteo
