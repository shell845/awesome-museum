from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class MuseumPlugin(AirflowPlugin):
    name = "museum_plugin"
    operators = [
        operators.DataWranglingOperator,
        operators.DataStagingOperator,
        operators.DataQualityCheckOperator,
        operators.DataTransformationOperator
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.ManageConnections
    ]
