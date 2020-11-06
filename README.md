# spark_word2vec
Implementing Spark distributed computing platform to train word2vec model


## Content
* Define UDF - wordToSeg

* Spark SQL - Appllying wordToSeg()

* Define UDF - wordToList()

* Spark SQL - Applying wordToList()

* word2Vec Training


## Define UDF - wordToSeg
Implmenting refular expression to remove all non-Chinese character, then utilize jieba libraries to segment words.

**Use addFile() to deliver user defined dictionary to all nodes in Spark cluster for jieba word segmentation.**

**Use jieba.dt.initialized to make sure jieba only impoer user defined dictionary once.**


```
spark.sparkContext.addFile('mydict_3.txt')
def wordToSeg(x):
    if not jieba.dt.initialized:
        jieba.load_userdict('mydict_3.txt')
```
