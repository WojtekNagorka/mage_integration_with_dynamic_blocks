import io
import pandas as pd
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
import math
from os import path
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.orchestration.db import db_connection
from mage_ai.orchestration.triggers.api import trigger_pipeline
import sys
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    db_connection.start_session()


    pipeline_runs = PipelineRun.query.filter(
        #set to pipeline name
        PipelineRun.pipeline_uuid == 'first_mage_task',  
        PipelineRun.status == PipelineRun.PipelineRunStatus.COMPLETED
    )

    last_trigger_run=pipeline_runs.all()[-1].execution_date
    
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        numberOfRowsToUpdate = loader.load(f"SELECT COUNT(*) FROM Products WHERE last_update_time > '{last_trigger_run}';")['count'][0]


    df=pd.DataFrame()
    df['rows']=[numberOfRowsToUpdate]
    df['time']=[last_trigger_run]
    return df  



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
