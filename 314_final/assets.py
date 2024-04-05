import kaggle
import dagster
import pandas as pd
import os
import zipfile
from dagster import MetadataValue, asset, MaterializeResult, AssetExecutionContext

import os
import zipfile
from kaggle import api
from kaggle.api.kaggle_api_extended import KaggleApi


@asset
def download_kaggle_data_5(context: AssetExecutionContext) -> MaterializeResult:

    api = KaggleApi()
    api.authenticate()

    # Define the competition name
    competition_name = "nfl-big-data-bowl-2024"

    # Specify the directory where you want to download the data
    data_dir = "./data"

    # Create the directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)

    # Download the data using the Kaggle API
    kaggle.api.competition_download_files(competition_name, path=data_dir, force=True)

    # Unzip the file
    zip_file_path = os.path.join(data_dir, f"{competition_name}.zip")
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(data_dir)

    return MaterializeResult

@asset
def create_dfs(context: AssetExecutionContext) -> MaterializeResult:
    return ()
