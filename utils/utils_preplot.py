'''
preplot.py
---
This module provide data wrangling functions using PySpark
'''

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *



def argmax(cols, *args):
    return [c for c, v in zip(cols, args) if v == max(args)][0]


def search_dominant(df):
    """
    find the dominant topic of each sample/row/document
    input: dataframe of weight of each topic
    output: the raw dominant topic number dataframe
    """
    #print('search_dominant')

    argmax_udf = lambda cols: F.udf(lambda *args: argmax(cols, *args), StringType())
    return (df
            .withColumn('dominant',argmax_udf(df.columns[2:])(*df.columns[2:]))
            .select(F.col('index').alias('index_'), F.col('dominant')))


def load_doctopic(f, n, spark):
    '''
    load raw doc-topic matrix file to dataframe.
    input:
        f: file path with file name
        n: topic number
    output:
        return the raw doc-topic matrix dataframe
    '''
    #print('load_doctopic')

    # generate new column names
    columns = [str(x) for x in list(range(n))]
    columns.insert(0, 'id')
    columns.insert(0, 'index')

    # load data
    df = (
        spark.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "true")
        .option("delimiter", "\t")
        .option("comment", "#")
        .load(f)
    )

    # change columns name and drop # column which is table index and useless
    return df.toDF(*columns)


def add_dominant(df_doctopic, df_dom):
    #print('add_dominant')
    # add the dominant topics to doc-topic matrix
    return (df_doctopic
            .join(df_dom, df_doctopic.index == df_dom.index_)
            .drop('index_')
            .orderBy('index'))


def add_metadata(df_doctopic, df_meta):
    #print('add_metadata')
    # add the metadata to doc-topic matrix
    return (df_doctopic
            .join(df_meta, df_doctopic.id == df_meta.id_)
            .withColumn('year', F.date_format('date', 'yyyy'))
            .drop('id_')
            .drop('date')
            .orderBy('index'))


def gen_dominant(df_doctopic, df_topics):
    '''
    generate dominant topics dataframe,
    add topic words colunm based on df_doctopic.
    input:
        df_doctopic: processed doc-topic matrix dataframe.
        df_topics: raw dataframe from topics words file.
    output:
        return processed dominant topics dataframe
    '''
    #print('gen_dominant')

    return (df_doctopic
            .join(df_topics, df_doctopic.dominant == df_topics.topic)
            .select(F.col('id'),
                    F.col('region'),
                    F.col('year'),
                    F.col('dominant'),
                    F.col('words')))


def gen_avgweight(df_doctopic):
    '''
    generate dataframe of average weight of topics.
    input:
        df_doctopic: processed doc-topic matrix dataframe.
    output:
        return year average weight topics dataframe
    '''
    #print('gen_avgweight')

    return (df_doctopic.drop('index').drop('id').drop('dominant').drop('region')
            .groupBy('year').avg().orderBy('year'))



def preplot(f_doctopic, df_meta, df_topics, spark):
    '''
    generate dataframes to plot
    input:
        f_doctopic: file path with file name
        df_meta:  metadata from dataset dataframe.
        df_topics: topic words dataframe.
    return:
        dominant topics dataframe
        year average weight topics dataframe
    '''
    #print('preplot')

    topic_number = df_topics.count()

    df_doctopic = load_doctopic(f_doctopic, topic_number, spark)

    df_dom = search_dominant(df_doctopic)

    df_doctopic = add_dominant(df_doctopic, df_dom)

    df_doctopic = add_metadata(df_doctopic, df_meta)

    df_dominant = gen_dominant(df_doctopic, df_topics)

    df_avgweight = gen_avgweight(df_doctopic)

    return df_dominant, df_avgweight
