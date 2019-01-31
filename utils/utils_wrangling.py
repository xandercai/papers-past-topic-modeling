'''
wrangling.py
---
This module provide data wrangling functions using PySpark
'''

from bs4 import BeautifulSoup
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *

def gen_region(path):
    """
    generate publisher-region relationship dataframe.
    input: path of the html file 'https://paperspast.natlib.govt.nz/newspapers/all#region'
    output: publisher-region relationship dataframe
    """

    # read webpage
    #path = r'../temp/Papers Past _ Explore all newspapers.html'
    with open(path, 'r') as f:
        html = f.read()

    # get table
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find('table', attrs={'class':'table datatable'})
    table_rows = table.find_all('tr')
    res = []
    for tr in table_rows:
        td = tr.find_all('td')
        row = [tr.text.strip() for tr in td if tr.text.strip()]
        if row:
            res.append(row)

    # transform table to pandas dataframe
    df_region = pd.DataFrame(res, columns=['publisher_', 'region', 'start_', 'end_']) # column_ means it will be drop later

    # transform pandas dataframe to pyspark dataframe
    df_region = spark.createDataFrame(df_region).orderBy('region')


    # update df_region for Bay Of Plenty Times and New Zealand Free Lance
    df_region = df_region.withColumn(
        'publisher_',
        F.when(
            F.col('publisher_') == 'Bay of Plenty Times',
            F.lit('Bay Of Plenty Times')
        ).otherwise(
            F.col('publisher_')
        )
    ).withColumn(
        'publisher_',
        F.when(
            F.col('publisher_') == 'Free Lance',
            F.lit('New Zealand Free Lance')
        ).otherwise(
            F.col('publisher_')
        )
    )

    return df_region


def wrangle_data(df, df_region, spark, wen=False, path=None):
    """
    wrangling raw dataset to get clean dataset.
    input:
        df: dataframe of raw dataset
        spark: spark session
    output:
        dataframe of clean dataset
    """

    # Clean NA to avoid nonetype.
    df = df.na.drop(subset=['content'])

    # deduplicate
    df = df.drop_duplicates(subset=['id'])

    # generate charactor counts of each document
    df = df.withColumn('length', F.length('content'))

    # remove rows with length under min_count
    min_count = 160
    df = df.filter(df.length >= min_count)

    # extract feature date
    df = df.withColumn('date', df['time'].cast(DateType()))

    # extract feature ads
    df = df.withColumn('ads', df.title.contains('dvertisement'))

    # remove redandunt parts of title
    df = df.withColumn('title_', F.regexp_extract(F.col('title'), '(.*)(\s\(.*\))', 1))

    # modify empty string
    df = df.withColumn(
        'title_',
        F.when(
            F.col('title_') == '',
            F.lit('Untitled Illustration')
        ).otherwise(
            F.col('title_')
        )
    )

    df = (df.join(df_region, df.publisher == df_region.publisher_, how='left')
          .select(F.col('id'),
                  F.col('publisher'),
                  F.col('region'),
                  F.col('date'),
                  F.col('ads'),
                  F.col('title'),
                  F.col('content'))
          )


    df = df.na.fill({'region':'unknown'})

    df = df.orderBy('id')

    if wen == True:
        #path = r'../data/dataset'
        if path != None:
            df.write.csv(path, mode='overwrite', compression='gzip')
        else:
            print('Error, path is None.')

    return df
