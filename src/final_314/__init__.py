from dagster import Definitions, load_asset_checks_from_package_name

from final_314 import assets

defs = Definitions(
    assets=[
        assets.download_kaggle_data,
        assets.tracking_drop_plays,
        assets.players_add_age,
        assets.players_convert_height,
        assets.players_drop_cols,
        assets.tracking_concat_all,
        assets.tracking_drop_cols,
        assets.tracking_players_add_force,
        assets.merge_all_dfs,
        assets.downsample,
        assets.df_to_csv,
        assets.random_forest_model
    ],
)