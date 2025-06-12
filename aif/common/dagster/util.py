"""Utility functions and classes for Dagster setup in AIF.

This module provides utility functions and classes for setting up and managing Dagster
components within the AIF framework. It includes tools for defining and merging schema
definitions, creating main definitions, and running jobs for assets.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Optional, Iterable, Union, TypeVar

import dagstermill as dgm
import dagster as dg
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition

import aif.common.aif.src.aif_logging as logging

T = TypeVar("T")


@dataclass
class DagsterSchemaDefinitions:
    """Container for Dagster schema definitions.

    Each schema defines its assets, schedules, sensors, jobs, resources, loggers, and asset checks.
    These definitions can be merged together to create a unified code location for Dagster.

    Attributes:
        assets: Collection of asset definitions.
        schedules: Collection of schedule definitions.
        sensors: Collection of sensor definitions.
        jobs: Collection of job definitions.
        resources: Mapping of resource names to resource definitions.
        loggers: Mapping of logger names to logger definitions.
        asset_checks: Collection of asset check definitions.
    """

    assets: Optional[Iterable[Union[dg.AssetsDefinition, dg.SourceAsset, dg.CacheableAssetsDefinition]]] = None
    schedules: Optional[Iterable[Union[dg.ScheduleDefinition, dg.UnresolvedPartitionedAssetScheduleDefinition]]] = None
    sensors: Optional[Iterable[dg.SensorDefinition]] = None
    jobs: Optional[Iterable[Union[dg.JobDefinition, dg.UnresolvedAssetJobDefinition]]] = None
    resources: Optional[dg.MutableMapping[str, dg.Any]] = None
    loggers: Optional[dg.MutableMapping[str, dg.LoggerDefinition]] = None
    asset_checks: Optional[Iterable[dg.AssetChecksDefinition]] = None

    def merge(self, defs: DagsterSchemaDefinitions) -> None:
        """Merge another schema definition into this one.

        This method combines all components from the provided schema definition
        with the current schema definition.

        Parameters:
            defs: The schema definition to merge into this one.

        Raises:
            ValueError: If there are conflicting resource or logger definitions.
        """
        self.assets = self._merge_iters(self.assets, defs.assets)
        self.schedules = self._merge_iters(self.schedules, defs.schedules)
        self.sensors = self._merge_iters(self.sensors, defs.sensors)
        self.jobs = self._merge_iters(self.jobs, defs.jobs)
        self.resources = self._merge_dict(self.resources, defs.resources)
        self.loggers = self._merge_dict(self.loggers, defs.loggers)
        self.asset_checks = self._merge_iters(self.asset_checks, defs.asset_checks)

    @staticmethod
    def _merge_iters(i1: Optional[Iterable[T]], i2: Optional[Iterable[T]]) -> Optional[Iterable[T]]:
        """Merge two iterables into a single iterable.

        Parameters:
            i1: First iterable.
            i2: Second iterable.

        Returns:
            Optional[Iterable[T]]: A combined iterable, or None if both inputs are None.
        """
        if i1 is None:
            return i2
        if i2 is None:
            return i1
        return itertools.chain(i1, i2)

    @staticmethod
    def _merge_dict(d1: Optional[dg.Mapping[str, T]], d2: Optional[dg.Mapping[str, T]]) -> Optional[dg.Mapping[str, T]]:
        """Merge two dictionaries into a single dictionary.

        This method raises an exception if both dictionaries contain the same key
        with different values.

        Parameters:
            d1: First dictionary.
            d2: Second dictionary.

        Returns:
            Optional[dg.Mapping[str, T]]: A combined dictionary, or None if both inputs are None.

        Raises:
            ValueError: If there are conflicting key-value pairs in the dictionaries.
        """
        if d1 is None:
            return d2
        if d2 is None:
            return d1

        res_dict: dict[str, T] = {}
        res_dict.update(d1)

        for key, value in d2.items():
            if key in res_dict and res_dict[key] != value:
                error_msg = f"Can not merge configuration. '{key}' is defined with multiple values."
                logging.get_aif_logger(__name__).error(error_msg)
                logging.get_aif_logger(__name__).error("Value 1: %s", str(res_dict[key]))
                logging.get_aif_logger(__name__).error("Value 2: %s", str(value))
                raise ValueError(error_msg)

            res_dict[key] = value
        return res_dict

    def __str__(self) -> str:
        """Return a string representation of the schema definitions.

        Returns:
            str: A string describing the asset definitions in this schema.
        """
        if self.assets is None:
            return "No asset definitions"

        assets_str = ""
        for asset in self.assets:
            assets_str += "Asset Definition: " + str(asset)

        return assets_str


def create_main_defs(definitions: list[DagsterSchemaDefinitions]) -> dg.Definitions:
    """Create a unified Dagster Definitions object from multiple schema definitions.

    This function merges different schema definitions into one and ensures that
    required resources like the output_notebook_io_manager are present.

    Parameters:
        definitions: List of schema definitions to merge.

    Returns:
        dg.Definitions: A unified Dagster Definitions object.

    Raises:
        ValueError: If there are conflicting definitions during the merge.
    """
    complete_defs: DagsterSchemaDefinitions = definitions[0]

    for dg_defs in definitions[1:]:
        complete_defs.merge(dg_defs)

    if complete_defs.resources is None:
        complete_defs.resources = {}

    res = complete_defs.resources
    if "output_notebook_io_manager" not in res.keys():
        res["output_notebook_io_manager"] = dgm.ConfigurableLocalOutputNotebookIOManager()

    defs = dg.Definitions(
        assets=list(complete_defs.assets) if complete_defs.assets is not None else None,
        schedules=list(complete_defs.schedules) if complete_defs.schedules is not None else None,
        sensors=list(complete_defs.sensors) if complete_defs.sensors is not None else None,
        jobs=list(complete_defs.jobs) if complete_defs.jobs is not None else None,
        resources=complete_defs.resources,
        executor=None,
        loggers=complete_defs.loggers,
        asset_checks=list(complete_defs.asset_checks) if complete_defs.asset_checks is not None else None,
    )

    return defs


def run_jobs_for_assets(assets: list) -> None:
    """Create and run a materializing job for the provided assets.

    This utility function creates a job that materializes all the provided assets
    and runs it in-process. It's particularly useful for local debugging.

    Parameters:
        assets: List of assets to materialize.

    Raises:
        Exception: If there's an error during job execution.
    """
    test_job: UnresolvedAssetJobDefinition = dg.define_asset_job(name="test_job", selection=assets)
    defs = dg.Definitions(assets=assets, jobs=[test_job])

    result = defs.get_job_def("test_job").execute_in_process(instance=dg.DagsterInstance.get())

    if result.success:
        print("Job execution succeeded")
    else:
        print("Job execution failed")
