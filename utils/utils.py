'''
data.py
---
This module provide data loading functions using PySpark
'''

import os
from pprint import pprint

import findspark
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from matplotlib import pyplot as plt
plt.style.use('ggplot')
import seaborn as sns
sns.axes_style("darkgrid")


#https://stackoverflow.com/questions/2104080/how-to-check-file-size-in-python
def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

def file_size(file_path):
    """
    this function will return the file size
    """
    if os.path.isfile(file_path):
        file_info = os.stat(file_path)
        return convert_bytes(file_info.st_size)


def print_filesize(files):
    """
    this function will print the file size of a file list
    """
    i = 1
    for file in files:
        print(i, file, file_size(file))
        i += 1



def conf_pyspark():
    """
    configure and initiate PySpark
    input: none
    output: sc, spark
    """
    findspark.init()

    # start a Spark context and session
    conf = SparkConf()
    conf.setAppName('local')
    conf.set('spark.driver.cores', 6) # set processor number
    conf.set('spark.driver.memory', '62g') # set memory size
    conf.set('spark.driver.maxResultSize', '4g')

    # for avoid import error caused by udf in utils files
    myPyFiles = ['../utils/utils_preplot.py']


    try:
        sc.stop()
        sc = SparkContext(conf=conf, pyFiles=myPyFiles)
    except:
        sc = SparkContext(conf=conf, pyFiles=myPyFiles)

    # passing spark context ot sql module
    spark = SparkSession(sc)

    # print configurations
    pprint(spark.sparkContext._conf.getAll())

    return sc, spark


def load_dataset(dataset, spark):
    """
    load dataset to a dataframe and retrun the dataframe.
    input: dataset:
                   'papers_pas' the raw dataset
                   'dataset' the clean dataset
           spark: spark session
    output: dataframe loaded, if dataset is not exist, return -1
    """


    if dataset == 'raw':

        path = r'../data/papers_past'

        data_schema = StructType([
            StructField('id', IntegerType()),
            StructField('url', StringType()),
            StructField('publisher', StringType()),
            StructField('time', StringType()),
            StructField('title', StringType()),
            StructField('content', StringType())
        ])

        df = (
            spark.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("inferSchema", "false")
            .option("delimiter", "\t")
            .schema(data_schema)
            .load(path)
        )

    elif dataset == 'clean':

        path = r'../data/dataset/clean'

        data_schema = StructType([
            StructField('id', IntegerType()),
            StructField('publisher', StringType()),
            StructField('region', StringType()),
            StructField('date', DateType()),
            StructField('ads', BooleanType()),
            StructField('title', StringType()),
            StructField('content', StringType())
        ])

        df = (
            spark.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("inferSchema", "false")
            .schema(data_schema)
            .load(path)
            .orderBy('id')
        )

    elif dataset == 'meta':

        path = r'../data/dataset/sample/meta'

        data_schema = StructType([
            StructField('id', IntegerType()),
            StructField('publisher', StringType()),
            StructField('region', StringType()),
            StructField('date', DateType()),
            StructField('ads', BooleanType())
        ])

        df = (
            spark.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("inferSchema", "false")
            .schema(data_schema)
            .load(path)
            .orderBy('id')
        )

    else:

        print('Wrong dataset, only "papers_past" and "dataset" are avalible.')
        df = -1

    return df



# for plot:

def filter_topics(df, topic_list):
    # remove data out of df if topic is not in topic_list
    return (df[df['topic'].isin(topic_list)])

def filter_regions(df, region_list):
    # remove data out of df if topic is not in topic_list
    return (df[df['region'].isin(region_list)])


def plot_topics(df, kind='', col_order=None, adjust_top=0.97, title=None, height=3.5, col_wrap=2):
    if kind == 'scatter':
        g = sns.catplot(x="year", y='weight', hue="topic",
                        col='keywords', col_wrap=col_wrap, col_order=col_order,
                        kind='strip', height=height, aspect=2, jitter=1, dodge=False, legend=False,
                        s=4, alpha=0.6, edgecolors='w',
                        data=df)
        g.fig.suptitle("Dominant Topics Distribution of {}".format(title), fontsize=16)

    elif kind == 'bar':
        g = sns.catplot(x="year", hue="topic",
                        col='keywords', col_wrap=col_wrap, col_order=col_order,
                        kind='count', height=height, aspect=2, dodge=False, legend=False,
                        data=df)
        g.fig.suptitle("Dominant Topics Count of {}".format(title), fontsize=16)

    elif kind == 'point':
        g = sns.catplot(x="year", y='weight',
                        col='keywords', col_wrap=col_wrap, col_order=col_order,
                        kind='point', height=height, aspect=2, dodge=False, s=1, legend=False,
                        markers='.', scale=0.5,
                        data=df)
        g.fig.suptitle("Average Weight of {}".format(title), fontsize=16)

    else:
        print('wrong kind.')

    g.fig.subplots_adjust(top=adjust_top)
    g.set_xticklabels(rotation=90, step=2)
    return g
