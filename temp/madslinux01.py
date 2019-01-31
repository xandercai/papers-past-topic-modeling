from pyspark.sql import functions as F
from pyspark.sql.types import *


path =  'hdfs:///project/nlnz/digital/papers_past/dataset_raw'

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
df.cache()

path = 'hdfs:///project/nlnz/digital/papers_past/temp/region.csv'

data_schema = StructType([
    StructField('publisher_', StringType()),
    StructField('region', StringType())
])

df_region = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(data_schema)
    .load(path)
)


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

path = 'hdfs:///project/nlnz/digital/papers_past/dataset_clean'
df.write.csv(path, mode='overwrite', compression='gzip')

df.limit(5).show()
df.count().show()


df_train = (df
            .select(F.col('id'),
                    F.col('title'),
                    F.col('content'))
            .orderBy('id'))
df_train.cache();

df.unpersist();

path = 'hdfs:///project/nlnz/digital/papers_past/dataset_train'
df_train.write.csv(path, sep='\t', mode='overwrite')

df_train.limit(5).show()
df_train.count().show()

df_train.unpersist();
