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
import matplotlib.pyplot as plt
import datetime
import re

#model imports
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.ensemble import RandomForestClassifier
import json


@asset(group_name="extract")
def download_kaggle_data(context: AssetExecutionContext):
   """
   Reads the data in using kaggle API and saves it to the data folder (it makes a new one
   if the folder doesn't already exist)
   """

   download_dir = "./data"

   # if the data folder is empty, download use the kaggle api to download everything
   if len(os.listdir("./data")) <= 1:
      os.environ["KAGGLE_USERNAME"] = "pranavkirti"
      os.environ["KAGGLE_KEY"] = "fb32031c050e2952ac1455094d8c1655"
      api = KaggleApi()
      api.authenticate()

      competition_name = "nfl-big-data-bowl-2024"
      os.makedirs("./data", exist_ok=True)

      api.competition_download_files(competition_name, path=download_dir)
      with zipfile.ZipFile(f"{download_dir}/{competition_name}.zip", "r") as zip_ref:
         zip_ref.extractall(download_dir)
   
   # once downloaded, read everything into a dictionary of dataframes
   dataframes = {}
   for filename in os.listdir(download_dir):
      if filename.endswith(".csv"):
         df_name = os.path.splitext(filename)[0]  # Extract DataFrame name from filename
         df = pd.read_csv(os.path.join(download_dir, filename))
         dataframes[df_name] = df
   
   return dataframes

@asset(group_name="transform")
def tracking_drop_plays(download_kaggle_data):
   """
   From all of the tracking CSVs, drop any rows that have a playID that is not in 
   tackles.

   Returns all new tracking dataframes in a list in their week order
   """
   return_list = []
   tackles = download_kaggle_data['tackles']
   valid_ids = tackles['playId'].tolist()
   for i in range(1, 10):
      df = download_kaggle_data[f'tracking_week_{i}']
      return_list.append(df[df['playId'].isin(valid_ids)])

   return return_list  

@asset(group_name="transform")
def players_add_age(download_kaggle_data):
   """
   Convert all the values in birthDate to the correct format. Then,
   add a new column called "age" to players which is calculated from birthDate
   """
   players_df = download_kaggle_data['players']

   pattern = r'^\d{4}-\d{2}-\d{2}$'
   for i, date in enumerate(players_df['birthDate']):
      if not pd.isna(date):
         if not re.match(pattern, str(date)[0:10]):
               players_df.loc[i, 'birthDate'] = re.sub(r'(\d{2})/(\d{2})/(\d{4})', r'\3-\1-\2', date)
               print(players_df.loc[i, 'birthDate'])

   players_df['birthDate'] = pd.to_datetime(players_df['birthDate'])
   players_df['age'] = (pd.Timestamp.now().normalize() - players_df['birthDate']).dt.days // 365

   return players_df

@asset(group_name="transform")
def players_convert_height(players_add_age):
   """
   Make a new column called "height_inches" that converts "height" to inches
   """
   players_add_age['height_inches'] = players_add_age['height'].apply(lambda x: int(x.split('-')[0]) * 12 + int(x.split('-')[1]))

   return players_add_age

@asset(group_name="transform")
def players_drop_cols(players_convert_height):
   """
   Drop the columns collegeName, displayName, position, and birthDate
   """
   cols = ['collegeName', 'displayName', 'position', 'birthDate']
   players_convert_height.drop(columns = cols, inplace = True)
   return players_convert_height

@asset(group_name="transform")
def tracking_concat_all(tracking_drop_plays):
   """
   Concatenate all of the tracking dataframes into one long dataframe
   """
   dfs_with_week = []
   for i, df in enumerate(tracking_drop_plays, start=1):
      df.loc[:, 'Week'] = [i]*len(df['playId'])
      dfs_with_week.append(df)
   
   concatenated_df = pd.concat(dfs_with_week, ignore_index=True)

   return concatenated_df

@asset(group_name="transform")
def tracking_drop_cols(tracking_concat_all):
   """
   Drop columns displayName, frameId, time, jerseyNumber, club, and play from tracking
   """
   cols = ['displayName', 'frameId', 'time', 'jerseyNumber', 'club', 'playDirection', 'event']
   tracking_concat_all = tracking_concat_all.drop(columns = cols)
   return tracking_concat_all

@asset(group_name="transform")
def tracking_players_add_force(tracking_drop_cols, players_drop_cols):
   """
   Add a new column "force" that is the force used on the tackle to tracking.
   Also, we merge tracking and players first. 
   """
   merged_df = pd.merge(tracking_drop_cols, players_drop_cols[['nflId', 'weight', 'age', 'height_inches']], on='nflId', how='left')
   merged_df['weight_kg'] = merged_df['weight'] * 0.453592
   merged_df['force'] = merged_df['weight_kg'] * merged_df['a']

   return merged_df

@asset(group_name="transform")
def merge_all_dfs(download_kaggle_data, tracking_players_add_force):
   """
   tracking_add_force merged tracking with players. Merge this with tackles
   """
   tackles_df = download_kaggle_data['tackles']
   all_merged = pd.merge(tracking_players_add_force, tackles_df, on=['gameId', 'playId', 'nflId'], how='left')

   return all_merged

@asset(group_name="transform")
def downsample(merge_all_dfs):
   """
   Drop any rows that contain an "na" or missing value. Then, downsample
   the dataframe so all the rows have the same amount of columns
   """

   merge_all_dfs.dropna(inplace=True)

   y_zero_rows = merge_all_dfs[merge_all_dfs['forcedFumble'] == 0]
   y_one_rows = merge_all_dfs[merge_all_dfs['forcedFumble'] == 1]

   y_zero_rows_downsampled = y_zero_rows.sample(n=len(y_one_rows))
   balanced_df = pd.concat([y_zero_rows_downsampled, y_one_rows])
   balanced_df = balanced_df.sample(frac=1, random_state=42).reset_index(drop=True)
   
   return balanced_df

@asset(group_name="load")
def df_to_csv(downsample):
   """
   Write the dataframe into a csv
   """
   downsample.to_csv('data/final_nfl_dataframe.csv', index=True)

   return MaterializeResult

@asset(group_name='machine_learning')
def random_forest_model(downsample):
   """
   Build a random forest classifier to build a model and predict forced fumbles.
   Write the accuracy and confusion matrix into a json.
   """

   X = downsample[['x', 'y', 's', 'a', 'dis', 'o', 'dir', 'force', 'age', 
                   'height_inches', 'weight_kg', 'force']]
   y = downsample['forcedFumble']
   X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

   random_forest = RandomForestClassifier(n_estimators=100)
   random_forest.fit(X_train, y_train)
   y_pred = random_forest.predict(X_test)
   
   accuracy = accuracy_score(y_test, y_pred)
   conf_matrix = confusion_matrix(y_test, y_pred)
   results = {'accuracy': accuracy,
              'conf matrix': conf_matrix.tolist()}
   
   os.makedirs('./model_results', exist_ok=True)
   results_path = './model_results/randomforest_results.json'
   with open(results_path, "w") as json_file:
      json.dump(results, json_file)