import pytest
import os
import pandas as pd
from pathlib import Path
import 314_final
from unittest.mock import patch


def test_players_cleaned_df():
    """
    Test that players.csv has been read in correctly.
    """
    file_path = "./data/players.csv"
    df = pd.read_csv(file_path)

    # Check that the players_df is created and has the expected columns
    assert isinstance(df, pd.DataFrame)
    expected_columns = ["nflId", "weight", "birthDate", "collegeName", "position", "displayName",
                        "height_feet", "height_inches", "height_total_inches"]

    assert set(df.columns) == set(expected_columns)

    assert df["nflId"].dtype == int
    assert df["weight"].dtype == int
    assert df["collegeName"].dtype == object
    assert df["position"].dtype == object
    assert df["displayName"].dtype == object
    assert df["height_feet"].dtype == int
    assert df["height_inches"].dtype == int
    assert df["height_total_inches"].dtype == int

    # Check the values of the first row
    first_row = df.iloc[0]
    assert first_row["nflId"] == 25511
    assert first_row["weight"] == 225
    assert first_row["collegeName"] == "Michigan"
    assert first_row["position"] == "QB"
    assert first_row["displayName"] == "Tom Brady"
    assert first_row["height_feet"] == 6
    assert first_row["height_inches"] == 4
    assert first_row["height_total_inches"] == 76

def total_tackles_test():
    df = pd.read_csv("./data/tackles.csv")
    input = df[df['gameId'] == 2022090800]
    cleaned = 314_final.add_total_tackles_column()
    cleaned = cleaned[cleaned['gameId'] == 2022090800]
    assert input == cleaned



def test_players_data_processing():
    test_data_dir = "tests/data"
    os.makedirs(test_data_dir, exist_ok=True)
    test_players_csv_path = os.path.join(test_data_dir, "players.csv")
    mock_players_df = pd.DataFrame({
        "height": ["6-4", "5-11", "6-0"],
        "weight": [225, 190, 210]})


    mock_players_df["height_inches"] = mock_players_df["height"].apply(height_to_inches)
    assert mock_players_df["height_inches"][0] == 76
    assert mock_players_df["height_inches"][1] == 71
    assert mock_players_df["height_inches"][2] == 72


    # Test weight density calculation
    mock_players_df["weight_density"] = mock_players_df["weight"] / mock_players_df["height_inches"]
    assert mock_players_df["weight_density"][0] == 225 / 76
    assert mock_players_df["weight_density"][1] == 190 / 71
    assert mock_players_df["weight_density"][2] == 210 / 72


    # Test writing and reading the updated CSV file
    mock_players_df.to_csv(test_players_csv_path, index=False)
    with patch("data_extraction.pd.read_csv", return_value=mock_players_df):
        updated_players_df = pd.read_csv(test_players_csv_path)

    assert set(updated_players_df.columns) == {"height", "weight", "height_inches", "weight_density"}
    assert updated_players_df["height_inches"][0] == 76
    assert updated_players_df["height_inches"][1] == 71
    assert updated_players_df["height_inches"][2] == 72
    assert updated_players_df["weight_density"][0] == 225 / 76
    assert updated_players_df["weight_density"][1] == 190 / 71
    assert updated_players_df["weight_density"][2] == 210 / 72
