import kaggle
import dagster
import pandas as pd
import os
import zipfile
import csv
from dagster import MetadataValue, asset, MaterializeResult, AssetExecutionContext

from kaggle import api
from kaggle.api.kaggle_api_extended import KaggleApi

import boxsdk
from boxsdk import OAuth2, Client 


@asset
def download_kaggle_data(context: AssetExecutionContext) -> MaterializeResult:
    """
    Reads the data in using kaggle API and saves it to the data folder (it makes a new one
    if the folder doesn't already exist)
    """

    api = KaggleApi()
    api.authenticate()
    os.makedirs("./data", exist_ok=True)

    kaggle.api.competition_download_files("nfl-big-data-bowl-2024", path="./data", force=True)

    # Unzip the file
    zip_file_path = os.path.join("./data", "nfl-big-data-bowl-2024.zip")
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall("./data")

    return MaterializeResult

@asset
def download_box_data(context: AssetExecutionContext) -> MaterializeResult:
   os.makedirs("./data", exist_ok=True)

   auth = OAuth2(client_id='YOUR_CLIENT_ID',
                 client_secret='YOUR_CLIENT_SECRET',
                 access_token='YOUR_ACCESS_TOKEN')
   client = Client(auth)

   folder = client.folder('nfl-big-data-bowl-2024').get()

   for csv in folder.get_items():
    if isinstance(csv, boxsdk.object.file.File) and csv.name.endswith('.csv'):
        # Download the file to the ./data folder
        csv.download_to_file(f'./data/{csv.name}')
        print(f"Downloaded {csv.name} successfully.")

@asset
def add_total_tackles_column(download_kaggle_data) -> MaterializeResult:
   """
   Adds a new column called 'total_tackles' that calculates the total number of tackles in each game
   """
   df = pd.read_csv("./data/tackles.csv")
   total_tackles_per_game = df.groupby('gameId')['tackle'].sum()
   total_tackles_df = pd.DataFrame({'gameId': total_tackles_per_game.index, 'total_tackles': total_tackles_per_game.values})
   df = pd.merge(df, total_tackles_df, on='gameId')
   df.to_csv("./data/tackles.csv", index=False) 
   return df

@asset
def calculate_home_advantage(download_kaggle_data) -> MaterializeResult:
   """
   Uses the final scores of the home and visiting teams to calculate if each team 
   has a home team advantage.
   """

   games_df = pd.read_csv("./data/games.csv")
   
   home_games_played = games_df['homeTeamAbbr'].value_counts()
   home_wins = games_df[games_df['homeFinalScore'] > games_df['visitorFinalScore']]['homeTeamAbbr'].value_counts()
   home_win_rate = (home_wins / home_games_played).fillna(0) 
   home_win_rate_df = pd.DataFrame({'homeTeamAbbr': home_win_rate.index, 'HomeWinPercentage': home_win_rate.values})
   home_win_rate_df['HomeFieldAdvantage'] = home_win_rate_df['HomeWinPercentage'] > 0.5

   games_df = pd.merge(games_df, home_win_rate_df, on='homeTeamAbbr', how='left')
   games_df.to_csv("./data/tackles.csv", index=False)

   return games_df


# Function to convert height from feet-inches format to inches
def height_to_inches(height):
   feet, inches = map(int, height.split("-"))
   return feet * 12 + inches

@asset
def weight_density(download_kaggle_data) -> MaterializeResult:
    """
    Calculates the weight density of players
    """
    players_df = pd.read_csv("./data/players.csv")

    players_df["height_inches"] = players_df["height"].apply(height_to_inches)
    players_df["weight_density"] = players_df["weight"] / players_df["height_inches"]
    players_df.to_csv("./data/players.csv", index=False)

    return players_df

@asset
def calculate_avg_yards_by_formation():
   plays_df = pd.read_csv('./data/plays.csv')
   
   avg_yards_by_formation_df = plays_df.groupby('offenseFormation')['playResult'].mean().reset_index()
   avg_yards_by_formation_df.to_csv("./data/plays.csv")
   
   return avg_yards_by_formation_df
