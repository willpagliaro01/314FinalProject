import pandas as pd

def test_height_column_type_conversion():
   # Load final NFL dataframe
   final_nfl_dataframe = pd.read_csv("./data/final_nfl_dataframe.csv")  # Replace with actual path
   
   # Check if the "height" column is of type numeric
   assert final_nfl_dataframe['height_inches'].dtype == float or final_nfl_dataframe['height_inches'].dtype == int

def test_final_dataframe_processing():
   # Load final NFL dataframe
   final_df = pd.read_csv("./data/final_nfl_dataframe.csv")  # Replace with actual path
   
   # Check if any rows with NA values are present in the final dataframe
   assert final_df.isnull().sum().sum() == 0
   
def test_dropped_columns_from_players():
   # Load players data
   players_df = pd.read_csv("./data/players.csv")  # Replace with actual path
   
   # Store the original columns for comparison
   original_columns = players_df.columns.tolist()
   
   # Drop the specified columns
   columns_to_drop = ['CollegeName', 'displayName', 'position', 'birthDate']
   players_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
   
   # Check if the dropped columns are not present in the dataframe
   for col in columns_to_drop:
       assert col not in players_df.columns
   
   # Check if the other columns are still present
   assert set(players_df.columns) == set(original_columns) - set(columns_to_drop)

