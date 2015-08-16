[![Build Status](https://travis-ci.org/maropu/hivemall-spark.svg?branch=master)](https://travis-ci.org/maropu/hivemall-spark)
[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/maropu/hivemall-spark/blob/master/LICENSE)

This is a simple wrapper implementation of [Hivemall](https://github.com/myui/hivemall/) for Spark.
This can make highly scalable machine learning algorithms available in DataFrame and HiveContext.

Installation
--------------------

```
# $SPARK_HOME/bin/spark-shell --packages maropu:hivemall-spark:0.0.3

scala> sqlContext.sql("add jar maropu_hivemall-spark-0.0.3.jar")
```

Hivemall in DataFrame
--------------------
[DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes) is a distributed collection
of data with names, types, and qualifiers.
To apply Hivemall fuctions in DataFrame, you type codes below;

```
// Fetch an initialization script for hivemall-spark
# wget https://raw.githubusercontent.com/maropu/hivemall-spark/master/scripts/ddl/define-dfs.sh

scala> :load define-dfs.sh

// Assume that an input format is as follows:
//   1.0,[1:0.5,3:0.3,8:0.1]
//   0.0,[2:0.1,3:0.8,7:0.4,9:0.1]
//   ...
scala> val trainTable = sc.textFile(...).map(HmLabeledPoint.parse)

scala> sqlContext.createDataFrame(trainTable)
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
// Fetch an initialization script for hivemall-spark
# wget https://raw.githubusercontent.com/maropu/hivemall-spark/master/scripts/ddl/define-udfs.sh

scala> :load define-udfs.sh

sqlContext.sql("
  SELECT feature, AVG(weight) AS weight
    FROM (
      SELECT train_logregr(add_bias(features), label AS(feature, weight)
        FROM trainTable
    ) model
    GROUP BY feature")
```

Support Status
--------------------
Hivemall v0.3.1 is currently incorporated in hivemall-spark.
The most functions can be available in HiveContext.
On the other hand, binary/multiclass classification and regression is supported in DataFrame
and other tasks such as recommendation will appear in upcomming releases.

API Documentations
--------------------
TBW

System Requirements
--------------------

* Spark 1.4.1

Presentations
------------
[Hivemall Meetup#1](http://eventdots.jp/event/458208) - [Slide](http://www.slideshare.net/maropu0804/20150512-hivemall-meetup1)

TODO
--------------------

* Support python APIs for Hivemall

* Implement the wrappers of Spark Streaming and MLlib
