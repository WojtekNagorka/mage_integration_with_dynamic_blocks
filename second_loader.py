from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path
from pandas import DataFrame
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_mysql(df,*args, **kwargs):
    """
    Template for loading data from a MySQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#mysql
    """
    data=df[0]
    params=df[1]

    #Instead of product_name insert the unique identifier
    keys=tuple((data['product_name'][i] for i in range(len(data))))
    
    query = f'SELECT * FROM Products WHERE product_name IN {keys}'  
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    metadata=[]
    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        return [loader.load(query),data,params,metadata]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
