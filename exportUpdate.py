from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_mysql(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a MySQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#mysql
    """
    table_name = 'Products'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    updateDf=df[0]
    insertDf=df[1]
    if len(updateDf)>0:
        with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            update_query = """
            UPDATE Products SET price = '%s' WHERE product_name='%s';
            """                                                                                                                                                          
            for index, row in updateDf.iterrows():
                loader.execute(update_query %(row['price'],row['product_name']))
            loader.commit()

    return insertDf
