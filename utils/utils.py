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
from wordcloud import WordCloud
plt.style.use('ggplot')
import seaborn as sns
sns.axes_style("darkgrid")
dpi=80


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
    conf.set('spark.driver.maxResultSize', '8g')

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
                        s=4, alpha=0.5, edgecolors='w',
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

    elif kind == 'region':
        g = sns.catplot(x="year", y='weight',
                        col='region', col_wrap=col_wrap, col_order=col_order,
                        kind='point', height=height, aspect=2, dodge=False, s=1, legend=False,
                        markers='.', scale=0.5,
                        data=df)
        g.fig.suptitle("Average Weight of topic {}".format(title), fontsize=16)

    else:
        print('wrong kind.')

    g.set_xticklabels(rotation=90, step=2)
    g.fig.tight_layout(pad=0, w_pad=0, h_pad=0.5)
    g.fig.subplots_adjust(top=adjust_top)
    return g



def addWeight(keywords):
    k = keywords.split()
    s = sum(range(len(k)+1))
    v = [i / s for i in range(len(k), 0, -1)]
    return dict(zip(k, v))


def plot_wordcloud(df_plt, topics, words=20, cols=4, adjust_top=0.94, path=None):
    """
    input:
        df_plt: dataframe to plot
        topics: topic number to plot
        words: words number to plot
        cols: column number in plot
    """

    df_plt['cloudwords'] = df_plt['keywords'].map(addWeight)


    plt_topics = topics
    plt_words  = words
    plt_cols   = cols

    cloud = WordCloud(width=800,
                      height=600,
                      max_words=plt_words,
                      colormap='rainbow')

    fig, axes = plt.subplots(int(plt_topics/4), plt_cols,
                             figsize=(13,(13/5)*(topics/cols)), dpi=dpi,
                             sharex=True, sharey=True)

    for i, ax in enumerate(axes.flatten()):
        fig.add_subplot(ax)
        cloud.generate_from_frequencies(df_plt.iloc[i]['cloudwords'],
                                        max_font_size=200)
        plt.gca().imshow(cloud)
        plt.gca().set_title('Topic:{} Weight:{:.5f}'
                            .format(df_plt.iloc[i]['topic'],
                                    df_plt.iloc[i]['weight']),
                            fontdict=dict(size=14))
        plt.gca().axis('off')

    fig.suptitle("Most Popular Topics Over Time", fontsize=16)
    plt.axis('off')
    plt.margins(x=0, y=0)
    plt.tight_layout(pad=0, w_pad=0, h_pad=0)
    fig.subplots_adjust(top=adjust_top)

    if path != None:
        plt.savefig(path, dpi=dpi)
        plt.show()
        plt.close()
        return

    plt.show()


def plot_heatmap(df_plt, T=False,
                 title='Annual Average Weight Over Time',
                 path=None):

    if T == False:
        fig, ax = plt.subplots(figsize=(13, 20), dpi=dpi)
        sns.heatmap(df_plt,
                    cmap='coolwarm',
                    cbar=True,
                    cbar_kws={'shrink':0.2, 'pad':0.005},
                    annot=False,
                    square=True,
                    ax=ax
                    )
        plt.xlabel('Year')
        plt.ylabel('Topic')

    else:
        fig, ax = plt.subplots(figsize=(20, 13), dpi=dpi)
        sns.heatmap(df_plt.T,
                    cmap='coolwarm',
                    cbar=True,
                    cbar_kws={'shrink':0.2, 'pad':0.005},
                    annot=False,
                    square=True,
                    ax=ax
                    )
        plt.ylabel('Year')
        plt.xlabel('Topic')

    plt.title(title, fontdict=dict(size=16))
    plt.xticks(rotation='90')
    plt.tight_layout(pad=0, w_pad=0, h_pad=0)

    if path != None:
        plt.savefig(path, dpi=dpi)
        plt.show()
        plt.close()
        return

    plt.show()


def plot_avg(df_avgweight, col_wrap=10, col_order=None,
             height=1.5, scale=0.2, adjust_top=0.96,
             title='Annual Average Weight of Topics Over Time',
             path=None):

    g = sns.catplot(x="year", y='weight',  col='topic',
                    col_wrap=col_wrap, col_order=col_order,
                    kind='point', height=height, aspect=1,
                    dodge=False, s=1, legend=False,
                    markers='.', scale=scale,
                    data=df_avgweight)

    g.fig.suptitle(title, fontsize=16)
    g.set_xticklabels(visible=False)
    g.set_yticklabels(visible=False)
    plt.tight_layout(pad=0, w_pad=0, h_pad=0.6)
    g.fig.subplots_adjust(top=adjust_top)

    if path != None:
        plt.savefig(path, dpi=dpi)
        plt.show()
        plt.close()
        return

    plt.show()



def plot_hot(topic, df_topics, df_domtopic, df_avgweight, adjust_top=0.94, path=None):

    df_plt_dom = filter_topics(df_domtopic, [topic])
    df_plt_avg = filter_topics(df_avgweight, [topic])

    fig = plt.figure(figsize=(13,13))

    ax = fig.add_subplot(3,1,1)
    ax = sns.pointplot(x='year', y='weight',
                       color='tab:red',
                       markers='.',
                       scale=0.5,
                       data=df_plt_avg)
    ax.get_xaxis().set_visible(False)
    ax.set_ylabel('Annual Average Weight')

    ax = fig.add_subplot(3,1,2)
    ax = sns.stripplot(x='year', y='weight',
                       color='tab:blue',
                       jitter=1.0001,
                       alpha=0.3,
                       data=df_plt_dom)
    ax.get_xaxis().set_visible(False)
    ax.set_ylabel('Dominant Topic Weight')

    ax1 = fig.add_subplot(3,1,3)

    ax1 = sns.countplot(x='year',
                        color='tab:green',
                        dodge=False,
                        data=df_plt_dom)
    ax1.set_ylabel('Dominant Topic Count')
    ax1.set_xlabel('Year')
    ax1.set_xticklabels(ax.get_xticklabels(), rotation=90)

    ax2 = ax1.twinx()
    ax2 = sns.countplot(x='year',
                        color='tab:green',
                        dodge=False,
                        facecolor=(0, 0, 0, 0),
                        linewidth=1,
                        edgecolor='tab:olive',
                        data=df_domtopic)
    ax2.grid(False)
    ax2.set_ylabel('Total Documents Count')

    fig.suptitle('Topic#{}\n{}'
                 .format(topic,
                         df_topics.iloc[topic][2][0:100]+'...'),
                 fontsize=16)

    plt.tight_layout(pad=0, w_pad=0, h_pad=0)
    fig.subplots_adjust(top=adjust_top)

    if path != None:
        plt.savefig(path, dpi=dpi)
        plt.show()
        plt.close()
        return

    plt.show()

