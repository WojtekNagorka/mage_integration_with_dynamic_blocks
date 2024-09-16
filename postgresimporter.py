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




@data_loader
def load_data_from_postgres(df,*args, **kwargs):
    """
    Template for loading data from a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    numberOfRowsToUpdate=df['rows'][0]
    last_trigger_run=df['time'][0]
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'


    chunk_size=500  
    updated=0
    queries=[]

    blocks=math.ceil(numberOfRowsToUpdate/chunk_size)

    while updated<numberOfRowsToUpdate:
        chunk=min(numberOfRowsToUpdate-updated,chunk_size)
        queries.append(f"SELECT * FROM Products WHERE last_update_time > '{last_trigger_run}' OFFSET {updated} ROWS FETCH NEXT {chunk} ROWS ONLY;")
        updated+=chunk_size
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    metadata=[]
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        data=[[loader.load(queries[i]),{'id'}] for i in range(blocks)] 
        return [data,metadata]
    

