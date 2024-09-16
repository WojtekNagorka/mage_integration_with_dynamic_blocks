from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
from pandas import DataFrame
import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_in_postgres(df, *args, **kwargs) -> DataFrame:
    """
    Performs a transformation in Postgres
    """
    print(df)
    dataFrame=df[1] #source
    data=df[0] #target
    params=df[2]
    if isinstance(params, str):
        params = set([params]) 
    # Insert the column name you want to merge by
    mergeOn='product_name'


    ### If the column you want to merge by is an int
    # dataFrame[mergeOn] = dataFrame[mergeOn].astype('int64')
    # data[mergeOn] = data[mergeOn].astype('int64')

    ### If the column you want to merge by is a string:
    dataFrame[mergeOn] = dataFrame[mergeOn].astype('str')
    data[mergeOn] = data[mergeOn].astype('str')

    merged = pd.merge(dataFrame, data, on=[mergeOn], how='outer', indicator=True)
    colnamesOriginal=dataFrame.columns.difference(set([mergeOn]))
    colnamesToDrop=[col+"_y" for col in colnamesOriginal]
    merged = merged.drop(colnamesToDrop, axis=1)
      
    for column in colnamesOriginal:
        merged = merged.rename(columns={f'{column}_x': f'{column}'})

    mergedInsert = merged[merged['_merge'] != 'both']
    mergedUpdate = merged[merged['_merge'] == 'both']

    mergedInsert.drop(columns=params,axis=1,inplace=True)
    mergedInsert.drop(columns={"_merge"},axis=1,inplace=True)

    mergedUpdate.drop(columns={"_merge"},axis=1,inplace=True)

    metadata=[]


    


    return [mergedUpdate,mergedInsert,metadata]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
