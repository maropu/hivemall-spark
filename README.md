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
# wget https://github.com/maropu/hivemall-spark/blob/master/scripts/ddl/define-dfs.sh

scala> :load define-dfs.sh

scala> val trainTable = sc.parallelize(
  HmLabeledPoint(0.0, "1:0.8" :: "2:0.2" :: Nil) ::
  HmLabeledPoint(1.0, "2:0.7" :: Nil) ::
  HmLabeledPoint(0.0, "1:0.9" :: Nil) :: Nil)

scala> sqlContext.createDataFrame(trainTable)
  .train_logregr($"label", add_bias($"feature"))
  .groupBy("feature")
  .agg("weight"->"avg")
```

More details can be found in [tutorials](./tutorials).

Hivemall in HiveContext
--------------------
For those try Hivemall in [HiveContext](https://spark.apache.org/docs/latest/sql-programming-guide.html#hive-tables),
run a script to register the user-defined functions of Hivemall in spark-shell and
say a SQL statements as follows;

```
# wget https://github.com/maropu/hivemall-spark/blob/master/scripts/ddl/define-udfs.sh

scala> :load define-udfs.sh

sqlContext.sql("
  SELECT model.feature, AVG(model.weight) AS weight
    FROM (
      SELECT train_logregr(add_bias(features), label AS(feature, weight)
        FROM trainTable
    ) model
    GROUP BY model.feature")
```

System Requirements
--------------------

* Spark 1.4

* Hive 0.13

Presentations
------------
[Hivemall Meetup#1](http://eventdots.jp/event/458208) - [Slide](http://www.slideshare.net/maropu0804/20150512-hivemall-meetup1)

TODO
--------------------

* Support python APIs for Hivemall

* Implement the wrappers of Spark Streaming and MLlib
