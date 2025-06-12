"""Workflow module for placeholder_schema schema assets."""

from aif.ai_analytics.dwh_use_cases.placeholder_schema.wf.asset_kaggle_train import asset_kaggle_train
from aif.ai_analytics.dwh_use_cases.placeholder_schema.wf.asset_kaggle_train_etl import asset_kaggle_train_etl
from aif.ai_analytics.dwh_use_cases.placeholder_schema.wf.asset_schema import asset_schema

__all__ = ["asset_schema", "asset_kaggle_train", "asset_kaggle_train_etl"]