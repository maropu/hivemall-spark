[![Build Status](https://travis-ci.org/maropu/hivemall-spark.svg?branch=master)](https://travis-ci.org/maropu/hivemall-spark)
[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/maropu/hivemall-spark/blob/master/LICENSE)

NOTE: This work will be merged into hivemall in [a future release](https://github.com/myui/hivemall/tree/dev/spark)!

This is a simple wrapper implementation of [Hivemall](https://github.com/myui/hivemall/) for Spark.
This can make highly scalable machine learning algorithms available in DataFrame and HiveContext.

Quick Installation
--------------------

```
// Fetch an initialization script for hivemall-spark
# wget https://raw.githubusercontent.com/maropu/hivemall-spark/master/scripts/ddl/define-udfs.sh

// Download Spark-v1.5.1 from 'http://spark.apache.org/downloads.html' and
// invoke an interactive shell
# $SPARK_HOME/bin/spark-shell --packages maropu:hivemall-spark:0.0.6

scala> :load define-udfs.sh
```

Hivemall in DataFrame
--------------------
[DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes) is a distributed collection
of data with names, types, and qualifiers.
To apply Hivemall fuctions in DataFrame, you type codes below;

```
// Assume that an input format is as follows:
//   1.0,[1:0.5,3:0.3,8:0.1]
//   0.0,[2:0.1,3:0.8,7:0.4,9:0.1]
//   ...
scala> val trainRdd = sc.textFile(...).map(HmLabeledPoint.parse)

scala> :paste

sqlContext.createDataFrame(trainRdd)
  .train_logregr(add_bias($"feature"), $"label")
  .groupby("feature")
  .agg("weight"->"avg")
```

More details can be found in [tutorials](./tutorials).

Hivemall in HiveContext
--------------------
For those try Hivemall in [HiveContext](https://spark.apache.org/docs/latest/sql-programming-guide.html#hive-tables),
run a script to register the user-defined functions of Hivemall in spark-shell and
say a SQL statements as follows;

```
scala> :paste

sql("
  SELECT feature, AVG(weight) AS weight
    FROM (
      SELECT train_logregr(add_bias(features), label AS(feature, weight)
        FROM trainTable
    ) model
    GROUP BY feature")
```

Hivemall in Spark Streaming
--------------------
Spark Streaming is an extension of the core Spark API that enables scalable,
high-throughput, fault-tolerant stream processing of live data streams.
A Hivemall model built from a batch of training data can be easily
applied into these streams;

```
// Assume that an input streaming format is as follows:
//   1.0,[1:0.5,3:0.3,8:0.1]
//   0.0,[2:0.1,3:0.8,7:0.4,9:0.1]
//   ...
scala> val testData = ssc.textFileStream(...).map(LabeledPoint.parse)

scala> :paste

testData.predict { case testDf =>
  // Explode features in input streams
  val testDf_exploded = ...

  val predictDf = testDf_exploded
    .join(model, testDf_exploded("feature") === model("feature"), "LEFT_OUTER")
    .select($"rowid", ($"weight" * $"value").as("value"))
    .groupby("rowid").sum("value")
    .select($"rowid", sigmoid($"SUM(value)"))

  predictDf
}

```

Support Status
--------------------
Hivemall v0.4.1-alpha.6 is currently incorporated in hivemall-spark and the most functions can be available in HiveContext.
On the other hand, functions listed below are available in DataFrame:

* regression

* binary/multiclass classification

* nearest neighbors

* some utility functions

API Documentations
--------------------
TBW

System Requirements
--------------------

* Spark 1.6.1

Presentations
------------

 * [Hivemall Meetup#1](http://eventdots.jp/event/458208) - [Slide](http://www.slideshare.net/maropu0804/20150512-hivemall-meetup1)

 * [Cloudera World Tokyo 2015](https://groups.google.com/forum/#!topic/hadoop-jp/ZanMFB3F4LQ) - [Slide](http://www.slideshare.net/maropu0804/cloudera-world-tokyo-2015)

TODO
--------------------

* Support python APIs for Hivemall

