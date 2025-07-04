# Data ETL pour Tableau de Bord Touristique

Ce projet propose un pipeline ETL (Extract, Transform, Load) qui récupère des données météo depuis l’API [Open-Meteo](https://open-meteo.com/) afin d’alimenter un tableau de bord permettant aux utilisateurs de choisir les meilleures destinations touristiques selon la météo passée et prévue.

## Objectifs du projet

- **Extraction** : Collecte des données météo historiques et prévisionnelles pour plusieurs villes via Open-Meteo (coordonnées GPS).
- **Transformation** : Nettoyage, fusion et structuration des données selon un schéma en étoile pour l’analyse.
- **Chargement** : Génération de fichiers CSV traités, prêts pour la visualisation et l’analyse.
- **Orchestration** : Automatisation quotidienne du pipeline via un DAG Apache Airflow.

## Fonctionnalités

- Récupération des données météo pour plusieurs villes (ex : Paris, Ottawa, Tokyo, New York, San Francisco).
- Extraction des données historiques et prévisionnelles.
- Fusion quotidienne et déduplication des données globales.
- Transformation en schéma en étoile pour faciliter l’analyse touristique.
- Prêt à être connecté à un tableau de bord pour aider les utilisateurs à faire les meilleurs choix touristiques selon la météo.

## Structure du projet

```
dags/
  etl.py               # DAG Airflow orchestrant le pipeline ETL
scripts/
  extract.py           # Extraction des données depuis Open-Meteo
  merge.py             # Fusion et déduplication des données
  transform.py         # Transformation en schéma en étoile (dimension ville, tables de faits)
data/
  raw/                 # Données brutes extraites par date et ville
    archive/
    forecast/
  processed/           # Jeux de données fusionnés
  star_schema/         # Tables finales prêtes pour l’analyse (dimension, faits)
```

## Fonctionnement

### 1. Extraction (`scripts/extract.py`)
- Utilise l’API de géocodage Open-Meteo pour obtenir les coordonnées des villes.
- Récupère les données météo historiques et de prévision pour chaque ville.
- Sauvegarde les données brutes au format CSV dans `data/raw/archive/<date>/` et `data/raw/forecast/<date>/`.

### 2. Fusion (`scripts/merge.py`)
- Fusionne les nouvelles données extraites avec les archives globales (`archive_meteo_global.csv` et `forecast_meteo_global.csv`).
- Déduplication par ville et date d’extraction.
- Résultat dans `data/processed/`.

### 3. Transformation (`scripts/transform.py`)
- Conversion des données fusionnées en schéma en étoile :
  - `dim_ville.csv` : table de dimension sur les villes
  - `archive_fact_weather.csv` et `forecast_fact_weather.csv` : tables de faits
- Sauvegarde dans `data/star_schema/`.

### 4. Orchestration (`dags/etl.py`)
- Un DAG Airflow exécute quotidiennement le pipeline :
  - Extraction parallèle pour chaque ville
  - Fusion après extraction
  - Transformation après fusion

## Prise en main

### Prérequis

- Python 3.x
- Apache Airflow
- pandas, requests

### Installation

1. **Cloner le dépôt :**
   ```bash
   git clone https://github.com/dieudonne19/data-etl-1.git
   cd data-etl-1
   ```

2. **Installer les dépendances :**
   ```bash
   pip install pandas requests apache-airflow
   ```

3. **Configurer Airflow :**
   - Placez `dags/etl.py` dans le dossier de vos DAGs Airflow.
   - Initialisez Airflow et lancez le scheduler.

4. **Lancer le pipeline :**
   - Le pipeline s’exécute automatiquement chaque jour selon la planification Airflow.

## Sources de données

- [API météo Open-Meteo](https://open-meteo.com/)
- [API de géocodage Open-Meteo](https://open-meteo.com/en/docs/geocoding-api)

## Intégration Tableau de Bord

Les tables finales (schéma en étoile) peuvent être utilisées dans des outils BI (ex : Tableau, Power BI) ou intégrées dans des dashboards personnalisés pour recommander les meilleures destinations touristiques en fonction de la météo.

## Contribuer

N’hésitez pas à ouvrir des issues ou des pull requests pour améliorer l’extraction, la transformation ou les fonctionnalités du tableau de bord !

## Licence

Projet distribué sous licence MIT. Voir `LICENSE` pour plus d’informations.

---

**Auteur** : dieudonne19
