This is a simple wrapper implementation of [Hivemall](https://github.com/myui/hivemall/) for Spark.
This can make highly scalable machine learning algorithms available in HiveContext, DataFrame, and ML Pipeline.

Installation
--------------------

```
git clone https://github.com/maropu/hivemall-spark.git

cd hivemall-spark

./bin/sbt assembly

<your spark>/bin/spark-shell --jars <hivemall-spark>/target/scala-2.10/hivemall-spark-assembly-0.0.1.jar
```

To avoid this annoying option in spark-shell, you can set the hivemall jar at `spark.jars`
in <your spark>/conf/spark-default.conf.

Hivemall in DataFrame
--------------------

```
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HivemallOps._

import sqlContext.implicits._

case class TrainData(label: Float, feature: Seq[String])

val trainTable = sc.parallelize(
  TrainData(0.0f, "1:0.8" :: "2:0.2" :: Nil) ::
  TrainData(1.0f, "2:0.7" :: Nil) ::
  TrainData(0.0f, "1:0.9" :: Nil) :: Nil)

sqlContext.createDataFrame(trainTable)
  .train_logregr(add_bias($"feature"), $"label")
```

Hivemall in HiveContext
--------------------
For those try Hivemall in HiveContext, run a script to register the functions of Hivemall in spark-shell and
say a SQL statements as follows;

```
:load <hivemall-spark>/scripts/define-all.sh

sqlContext.sql("
  SELECT model.feature, CAST(AVG(model.weight) AS FLOAT) AS weight
    FROM (
      SELECT train_logregr(add_bias(features), CAST(label AS FLOAT)) AS(feature, weight)
        FROM trainTable
    ) model
    GROUP BY model.feature")
```

Hivemall in Spark ML Pipeline
--------------------

TBD


Current Status
--------------------
The current implementation of Spark cannot correctly handle UDF/UDAF/UDTF in Hive.
Related issues for hivemall-spark are as follows;

* [SPARK-6747] (https://issues.apache.org/jira/browse/SPARK-6747) Support List<> as a return type in Hive UDF ([#5395](https://github.com/apache/spark/pull/5395))

* [SPARK-6921] (https://issues.apache.org/jira/browse/SPARK-6921) Support Map<K,V> as a return type in Hive UDF ([#XXXX](https://github.com/apache/spark/pull/XXXX))

* [SPARK-6734](https://issues.apache.org/jira/browse/SPARK-6734) Add UDTF.close support in Generate ([#5383](https://github.com/apache/spark/pull/5383))

* [SPARK-4233](https://issues.apache.org/jira/browse/SPARK-4233) WIP:Simplify the UDAF API (Interface) ([#3247](https://github.com/apache/spark/pull/3247))

System requirements
--------------------

* Spark 1.4 (Expected)

* Hive 0.13

TODO
--------------------

* Support the other functions of Hivemall in DataFrame

* Support Hive 0.12

* Support Spark ML Pipeline

* Register this package as a [spark package](http://spark-packages.org/)

        For easy Installation: <your spark>/bin/spark-shell --packages hivemall-spark:0.0.1

