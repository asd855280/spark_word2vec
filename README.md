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

## Spark SQL - Appllying wordToSeg()
Call UDF-wordToSeg() in Spark SQL syntax. Quickly apply UDF to specified columns on each record.

```
recipes_seg = spark.sql('''select url, img_url, title, time, author, word2Seg(ingredient) ingredient, 
                        word2Seg(steps) steps, word2Seg(comment) comment,
                        word2Seg(category) category from recipes''')
```


## Define UDF - wordToList()
Defining function to split strings into list contains segmented words

## Spark SQL - Applying wordToList()
Call UDF-wordToList() in Spark SQL syntax, spliting words into list on specified columns from all records.

## word2Vec Training

