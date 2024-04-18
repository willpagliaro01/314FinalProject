from dagster import Definitions, load_asset_checks_from_package_name

from final_314 import assets

defs = Definitions(
    assets=[
        assets.add_total_tackles_column,
        assets.calculate_avg_yards_by_formation,
        assets.download_kaggle_data,
        assets.download_box_data,
        assets.calculate_home_advantage,
        assets.height_to_inches,
        assets.weight_density
    ],
)