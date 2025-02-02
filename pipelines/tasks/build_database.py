"""
Consolidate data into the database.
"""

import logging
import os
from typing import Dict
from zipfile import ZipFile

import duckdb
import requests

from ._common import CACHE_FOLDER, DUCKDB_FILE, clear_cache

logger = logging.getLogger(__name__)

def get_yearly_dataset_infos(year:str) -> Dict[str,str]:
    """
    Returns information for yearly dataset extract of the SISE Eaux datasets.
    The data comes from https://www.data.gouv.fr/fr/datasets/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/
    For each year a dataset is downloadable on a URL like this (ex. 2024):
        https://www.data.gouv.fr/fr/datasets/r/84a67a3b-08a7-4001-98e6-231c74a98139
    The id of the dataset is the last part of this URL
    The name of the dataset is dis-YEAR.zip (but the format could potentially change).
    :param year: The year from which we want to get the dataset information.
    :return: A dict with the id and name of the dataset.
    """
    dis_files_info_by_year = {
        "2024": {"id": "84a67a3b-08a7-4001-98e6-231c74a98139", "name" : "dis-2024.zip"},
        "2023": {"id":"c89dec4a-d985-447c-a102-75ba814c398e", "name" : "dis-2023.zip"},
        "2022": {"id":"a97b6074-c4dd-4ef2-8922-b0cf04dbff9a", "name" : "dis-2022.zip"},
        "2021": {"id":"d2b432cc-3761-44d3-8e66-48bc15300bb5", "name" : "dis-2021.zip"},
        "2020": {"id":"a6cb4fea-ef8c-47a5-acb3-14e49ccad801", "name" : "dis-2020.zip"},
        "2019": {"id":"861f2a7d-024c-4bf0-968b-9e3069d9de07", "name" : "dis-2019.zip"},
        "2018": {"id":"0513b3c0-dc18-468d-a969-b3508f079792", "name" : "dis-2018.zip"},
        "2017": {"id":"5785427b-3167-49fa-a581-aef835f0fb04", "name" : "dis-2017.zip"},
        "2016": {"id":"483c84dd-7912-483b-b96f-4fa5e1d8651f", "name" : "dis-2016.zip"}
    }
    return dis_files_info_by_year[year]


def process_sise_eaux_dataset_2024():
    """Process SISE-Eaux dataset for 2024."""

    # Dataset specific constants
    DATA_URL = (
        "https://www.data.gouv.fr/fr/datasets/r/84a67a3b-08a7-4001-98e6-231c74a98139"
    )
    ZIP_FILE = os.path.join(CACHE_FOLDER, "dis-2024.zip")
    EXTRACT_FOLDER = os.path.join(CACHE_FOLDER, "raw_data_2024")

    FILES = {
        "communes": {"filename": "DIS_COM_UDI_2024.txt", "table": "sise_communes"},
        "prelevements": {"filename": "DIS_PLV_2024.txt", "table": "sise_prelevements"},
        "resultats": {"filename": "DIS_RESULT_2024.txt", "table": "sise_resultats"},
    }

    logger.info("Downloading and extracting SISE-Eaux dataset for 2024...")
    response = requests.get(DATA_URL, stream=True)
    with open(ZIP_FILE, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info("Extracting files...")
    with ZipFile(ZIP_FILE, "r") as zip_ref:
        zip_ref.extractall(EXTRACT_FOLDER)

    logger.info("Creating tables in the database...")
    conn = duckdb.connect(DUCKDB_FILE)
    for file_info in FILES.values():
        filepath = os.path.join(EXTRACT_FOLDER, file_info["filename"])
        query = f"""
            CREATE OR REPLACE TABLE {file_info["table"]} AS 
            SELECT * FROM read_csv('{filepath}', header=true, delim=',');
        """
        conn.execute(query)
    conn.close()

    logger.info("Cleaning up...")
    clear_cache()

    return True


def execute():
    process_sise_eaux_dataset_2024()
