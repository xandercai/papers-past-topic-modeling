#####################################
# LDA Topic Modeling using pySpark
#####################################

import re
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, \
                               Tokenizer, \
                               StopWordsRemover, \
                               CountVectorizer, \
                               IDF
                               #NGram, \
                               #HashingTF, \
from pyspark.ml.clustering import LDA, \
                                  LDAModel
                                  #DistributedLDAModel


#################
# load data
#################
data_schema = StructType([
    StructField('id', IntegerType()),
    StructField('url', StringType()),
    StructField('publisher', StringType()),
    StructField('time', StringType()),
    StructField('title', StringType()),
    StructField('content', StringType())
])
data = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("delimiter", "\t")
    .schema(data_schema)
    #.load("hdfs:////user/xca24/papers_past/Observer.txt")
    .load("hdfs:////user/xca24/papers_past/Lyttelton_Times.txt")
)
data.cache()
data.show(10, True)

# Clean NA to avoid nonetype.
data = data.na.drop(subset=["content"])

#################
# instance
#################
tokenizer = RegexTokenizer(inputCol="content", outputCol="tokens", pattern="[a-zA-Z]{4,15}", gaps=False)
#tokenizer = Tokenizer(inputCol="content", outputCol="tokens")
remover = StopWordsRemover(inputCol="tokens", outputCol="words")
#print("StopWords:", remover.getStopWords())
cv = CountVectorizer(inputCol="words", outputCol="tf")
idf = IDF(minDocFreq=10, inputCol="tf", outputCol="features")

#################
# step by step or jump to pipeline
#################

tmp = data
tmp = tokenizer.transform(tmp)
tmp = remover.transform(tmp)

cvmodel = cv.fit(tmp) # Here Report Error!!!

tmp = cvmodel.transform(tmp)

#hashingTF = HashingTF(inputCol="words", outputCol="tf")
#tmp = hashingTF.transform(tmp)

idfModel = idf.fit(tmp)
df_model = idfModel.transform(tmp)

#tmp.show(10, True)
#tmp.limit(1).collect()

#################
# all in pipeline
#################
pipeline = Pipeline(stages=[tokenizer, remover, cv, idf])
# Fit the pipeline
bow = pipeline.fit(data)
df_model = bow.transform(data).select('id','words','features')
print("\nProcessed data:")
df_model.show()

#################
# LDA Topic Modeling
#################
num_topics=40
max_iterations=1000
lda = LDA(k=num_topics, maxIter=max_iterations)
ldaModel = lda.fit(df_model)


# Print topics and top-weighted terms
topics = ldaModel.describeTopics(maxTermsPerTopic=10)
vocabArray = cvmodel.vocabulary
numTopics = 20

ListOfIndexToWords = F.udf(lambda wl: list([vocabArray[w] for w in wl]))
FormatNumbers = F.udf(lambda nl: ["{:1.4f}".format(x) for x in nl])

topics.select(ListOfIndexToWords(topics.termIndices).alias('words'))\
              .show(truncate=False, n=numTopics)
topics.select(FormatNumbers(topics.termWeights).alias('weights'))\
              .show(truncate=False, n=numTopics)

