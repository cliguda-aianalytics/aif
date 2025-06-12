"""This modul contains the asset for the OHLC exploration notebook. If run, the updated notebook and the results
are available within Dagster."""

import dagstermill as dgm
import dagster as dg

from aif.common.dagster.util import run_jobs_for_assets
from aif.ai_analytics.dwh_finance.core_fin_data_yf import DWH_NAME, SCHEMA_NAME

# pylint: disable=assignment-from-no-return
asset_nb_ohlc_explore = dgm.define_dagstermill_asset(
    key_prefix=[DWH_NAME, SCHEMA_NAME],
    name="NB_OHLC_EXPLORE",
    description="Explore OHLC data",
    group_name=f"{DWH_NAME}_{SCHEMA_NAME}",
    notebook_path="aif/ai_analytics/dwh_finance/core_fin_data_yf/notebooks/ohlc_explore.ipynb",
    deps=[dg.SourceAsset(key=dg.AssetKey([DWH_NAME, SCHEMA_NAME, "OHLC_DAILY_REFRESH"]))],
)


@dg.asset(
    key_prefix=[DWH_NAME, SCHEMA_NAME],
    name="NB_OHLC_EXPLORE_UPLOAD",
    description="Sharing the refreshed notebook",
    group_name=f"{DWH_NAME}_{SCHEMA_NAME}",
    ins={"nb_ohlc_explore": dg.AssetIn(key=dg.AssetKey([DWH_NAME, SCHEMA_NAME, "NB_OHLC_EXPLORE"]))},
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def asset_nb_ohlc_explore_upload(context, nb_ohlc_explore):
    """Share the updated notebook. The notebook is of type bytes and can be written to any filesystem (e.g. S3)."""
    context.log.info(f"The notebook to share has the size of {len(nb_ohlc_explore)} bytes")


if __name__ == "__main__":
    run_jobs_for_assets([asset_nb_ohlc_explore])
