import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
from extract import extract_meteo
from merge import merge_files
from transform import transform_to_star


# Configuration par défaut du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 30),
}

# Liste des villes à traiter
CITIES = ["Paris", "Ottawa", "Tokyo", "New York", "San Francisco"]

with DAG(
    "tourisme",
    default_args=default_args,
    schedule="@daily",  # Exécution quotidienne
    catchup=False,  # Ne pas rattraper les exécutions passées
    max_active_runs=1,  # Pour éviter les conflits
) as dag:

    # ========== Tâches d'Extraction ==========
    extract_tasks = [
        PythonOperator(
            task_id=f'extract_{city.lower().replace(" ", "_")}',
            python_callable=extract_meteo,
            op_args=[city, "{{ ds }}"],
            # ds = date de l'exécution au format YYYY-MM-DD
        )
        for city in CITIES
    ]

    # ========== Tâche de Fusion ==========
    merge_task = PythonOperator(
        task_id="merge_files",
        python_callable=merge_files,
        op_args=["{{ ds }}"],
    )

    # ========== Tâche de Transformation ==========
    transform_task = PythonOperator(
        task_id="transform_to_star", python_callable=transform_to_star
    )

    # ========== Orchestration ==========
    # Les tâches d'extraction s'exécutent en parallèle
    # puis la fusion s'exécute, suivie de la transformation
    extract_tasks >> merge_task >> transform_task
