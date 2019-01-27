# Topic Modeling on Historical Newspapers of New Zealand

#### A UC MADS Data601 Project
---

## Background

The National Library of New Zealand’s job is to “collect, connect and co-create knowledge” for the benefit of Aotearoa New Zealand. As New Zealand’s library of legal deposit, NLNZ collects publications and resources created in New Zealand, as well as metadata about these items. Increasingly, the National Library works with digital collections and datasets. DigitalNZ is part of the National Library of New Zealand, inside the Department of Internal Affairs, and works with institutions around New Zealand to make our country’s digital cultural heritage collections visible. As they put it, “DigitalNZ is the search site for all things New Zealand. We connect you to reliable digital collections from our content partners — libraries, museums, galleries, government departments, the media, community groups and others.” 

Papers Past is a digitised collection of New Zealand’s historical publications. Currently, the collection contains newspapers from 1839 to 1949. The newspaper articles have been digitised using Optical Character Recognition (OCR), but sometimes with poor quality results. DigitalNZ currently provides this digitised text in its search results for a substantial portion of the Papers Past collection, but would like to explore ways to provide more readable and useful metadata to its users. 


## Project Aim

The National Library of New Zealand’s job is to “collect, connect and co-create knowledge” for the benefit of Aotearoa New Zealand. As New Zealand’s library of legal deposit, NLNZ collects publications and resources created in New Zealand, as well as metadata about these items. Increasingly, the National Library works with digital collections and datasets. DigitalNZ is part of the National Library of New Zealand, inside the Department of Internal Affairs, and works with institutions around New Zealand to make our country’s digital cultural heritage collections visible. As they put it, “DigitalNZ is the search site for all things New Zealand. We connect you to reliable digital collections from our content partners — libraries, museums, galleries, government departments, the media, community groups and others.” 

Papers Past is a digitised collection of New Zealand’s historical publications. Currently, the collection contains newspapers from 1839 to 1949. The newspaper articles have been digitised using Optical Character Recognition (OCR), but sometimes with poor quality results. DigitalNZ currently provides this digitised text in its search results for a substantial portion of the Papers Past collection, but would like to explore ways to provide more readable and useful metadata to its users. 

## Data

The Papers Past dataset has:
* total 33 GB,
* total 68 files,
* total 16,731,578 documents,
* each file contains 112 to 3,007,465 documents.

## Requirement and Setup

To run the notebooks locally, you will need Python3 as well as the libraries recorded in the requirement.txt file. I recommend managing Python and the libraries using `pip`.

To install, first install `pip`, then you can duplicate the environment for these notebooks by running (in the command line):

```console
pip install -r /path/to/requirements.txt
```

## Contents

Part | File | Comment
---|---|---
1-loading | [1-load.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/1-loading/1-load.ipynb) | Load and learn the raw dataset situation.
2-wrangling | [1-wrangling.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/2-wrangling/1-wrangling.ipynb) | Data clean and feature engineering.
3-exploring | [1-explore.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/3-exploring/1-explore.ipynb) | Analyze and visualize the clean dataset.
4-preprocessing | [1-preprocess.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/4-preprocessing/1-preprocess.ipynb) | Experiment and discussion about OCR,<br/>spelling correction and other NLP text preprocesses.
5-modeling | [1-datasets.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/5-modeling/1-datasets.ipynb) | Split and extract sample set and subsets.
 | [2-model.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/5-modeling/2-model.ipynb) | Topic modeling process.
6-analyzing | [1-prepare.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/6-analyzing/1-prepare.ipynb) | Prepare dataframes for analysis and visualization.
 | [2-analysis-train.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/6-analyzing/2-analysis-train.ipynb) | Analyze and visualize train set,<br/>which could represent the full dataset.
 | [3-analysis-wwi.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/6-analyzing/3-analysis-wwi.ipynb) | Analyze and visualize dataset during WWI,<br/>which focus on the topics of different time range.
 | [4-analysis-regions.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/6-analyzing/4-analysis-regions.ipynb) | Analyze and visualize dataset from specific regions,<br/>which focus on the topics of different region.
 | [5-analysis-ads.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/6-analyzing/5-analysis-ads.ipynb) | Analyze and visualize dataset from specific label (advertisements),<br/>which focus on the topics of different label (advertisements or not).
7-applying | [1-mining.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/7-applying/1-mining.ipynb) | The application of data mining -<br/>using linear regression to explore the correlation of topics.
 | [2-sentiment.ipynb](https://github.com/xandercai/papers-past-topic-modeling/blob/master/7-applying/2-sentiment.ipynb) | The application of sentiment analysis -<br/>using a sentiment analysis package to learn the historical sentiment.




