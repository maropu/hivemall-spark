This is a simple wrapper implementation of [Hivemall](https://github.com/myui/hivemall/) for Spark.
This can make highly scalable machine learning algorithms available in DataFrame and HiveContext.

Installation
--------------------

```
git clone https://github.com/maropu/hivemall-spark.git

cd hivemall-spark

./bin/sbt assembly

<your spark>/bin/spark-shell --jars hivemall-spark-assembly-0.0.1.jar

scala> sqlContext.sql("add jar hivemall-spark-assembly-0.0.1.jar")
```

To avoid this annoying option in spark-shell, you can set the hivemall jar at `spark.jars`
in <your spark>/conf/spark-default.conf.

Hivemall in DataFrame
--------------------
[DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes) is a distributed collection
of data with names, types, and qualifiers.
To apply Hivemall fuctions in DataFrame, you type codes below;

```
import org.apache.spark.ml.regression.HivemallLabeledPoint
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HivemallOps._
import sqlContext.implicits._ // sqlContext is a instance of SQLContext

val trainTable = sc.parallelize(
  HivemallLabeledPoint(0.0f, "1:0.8" :: "2:0.2" :: Nil) ::
  HivemallLabeledPoint(1.0f, "2:0.7" :: Nil) ::
  HivemallLabeledPoint(0.0f, "1:0.9" :: Nil) :: Nil)

sqlContext.createDataFrame(trainTable)
  .train_logregr(add_bias($"feature"), $"label")
  .groupBy("_c0") // _c0:feature _c1:weight
  .agg("_c1" -> "avg")
```

Hivemall in HiveContext
--------------------
For those try Hivemall in [HiveContext](https://spark.apache.org/docs/latest/sql-programming-guide.html#hive-tables),
run a script to register the functions of Hivemall in spark-shell and
say a SQL statements as follows;

```
:load <hivemall-spark>/scripts/define-all.sh

sqlContext.sql("
  SELECT model.feature, AVG(model.weight) AS weight
    FROM (
      SELECT train_logregr(add_bias(features), label AS(feature, weight)
        FROM trainTable
    ) model
    GROUP BY model.feature")
```

Current Status
--------------------
The current implementation of Spark cannot correctly handle UDF/UDAF/UDTF in Hive.
Related issues for hivemall-spark are as follows;

* [SPARK-6747] (https://issues.apache.org/jira/browse/SPARK-6747) Support List<> as a return type in Hive UDF ([#5395](https://github.com/apache/spark/pull/5395))

* [SPARK-6921] (https://issues.apache.org/jira/browse/SPARK-6921) Support Map<K,V> as a return type in Hive UDF ([#XXXX](https://github.com/apache/spark/pull/XXXX))

* [SPARK-6734](https://issues.apache.org/jira/browse/SPARK-6734) Add UDTF.close support in Generate ([#5383](https://github.com/apache/spark/pull/5383))

* [SPARK-4233](https://issues.apache.org/jira/browse/SPARK-4233) WIP:Simplify the UDAF API (Interface) ([#3247](https://github.com/apache/spark/pull/3247))

System Requirements
--------------------

* Spark 1.4

* Hive 0.13

Presentations
------------
[Hivemall Meetup#1](http://eventdots.jp/event/458208) - [Slide](http://www.slideshare.net/maropu0804/20150512-hivemall-meetup1)

TODO
--------------------

* Support the other functions of Hivemall in DataFrame (Currently only support the implementation of hivemall.regression.*)

* Register this package as a [spark package](http://spark-packages.org/)

        For easy installations:
          <your spark>/bin/spark-shell --packages hivemall-spark:0.0.1

* Improve test/prediction phases by using code generation

        SELECT sigmoid(codegen_lr_model(to_vector(features)) as prob
          FROM test_data

* Support python APIs for Hivemall


