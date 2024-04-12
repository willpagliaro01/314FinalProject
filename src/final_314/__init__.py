from dagster import Definitions, load_asset_checks_from_package_name

all_assets = load_asset_checks_from_package_name("final_314")

defs = Definitions(
    assets=all_assets,
)