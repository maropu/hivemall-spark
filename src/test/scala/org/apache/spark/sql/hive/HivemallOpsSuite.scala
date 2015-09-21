/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import hivemall.tools.RegressionDatagen
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.HivemallUtils._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, QueryTest, Row, functions}
import org.apache.spark.test.TestDoubleWrapper._
import org.apache.spark.test.TestUtils._

import scala.collection.mutable.Seq
import scala.reflect.runtime.{universe => ru}

class HivemallOpsSuite extends QueryTest {
  import org.apache.spark.sql.hive.HivemallOpsSuite._

  test("hivemall_version") {
    assert(DummyInputData.select(hivemall_version()).collect.toSet === Set(Row("0.3.1")))
    /**
     * TODO: Why a test below does fail?
     *
     * checkAnswer(
     *   DummyInputData.select(hivemall_version()).distinct,
     *   Row("0.3.1")
     * )
     *
     * The test throw an exception below:
     *
     * [info] - hivemall_version *** FAILED ***
     * [info]   org.apache.spark.sql.AnalysisException: Cannot resolve column name "HiveSimpleUDF#hivemall.HivemallVersionUDF()" among (HiveSimpleUDF#hivemall.Hivemall VersionUDF());
     * [info]   at org.apache.spark.sql.DataFrame$$anonfun$resolve$1.apply(DataFrame.scala:159)
     * [info]   at org.apache.spark.sql.DataFrame$$anonfun$resolve$1.apply(DataFrame.scala:159)
     * [info]   at scala.Option.getOrElse(Option.scala:120)
     * [info]   at org.apache.spark.sql.DataFrame.resolve(DataFrame.scala:158)
     * [info]   at org.apache.spark.sql.DataFrame$$anonfun$30.apply(DataFrame.scala:1227)
     * [info]   at org.apache.spark.sql.DataFrame$$anonfun$30.apply(DataFrame.scala:1227)
     * [info]   at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:244)
     */
  }

  test("add_bias") {
    assert(TinyTrainData.select(add_bias($"features")).collect.toSet
      === Set(
        Row(Seq("1:0.8", "2:0.2", "0:1.0")),
        Row(Seq("2:0.7", "0:1.0")),
        Row(Seq("1:0.9", "0:1.0"))))
  }

  test("extract_feature") {
    assert(TinyTrainData.select(extract_feature("1:0.8")).collect.toSet
      === Set(Row("1")))
  }

  test("extract_weight") {
    assert(TinyTrainData.select(extract_weight("3:0.1")).collect.toSet
      === Set(Row("0.1")))
  }

  test("explode_array") {
    assert(TinyTrainData.explode_array("features")
        .select($"feature").collect.toSet
      === Set(Row("1:0.8"), Row("2:0.2"), Row("2:0.7"), Row("1:0.9")))
  }

  test("add_feature_index") {
    val DoubleListData = {
      val rowRdd = TestHive.sparkContext.parallelize(
          Row(0.8 :: 0.5 :: Nil) ::
          Row(0.3 :: 0.1 :: Nil) ::
          Row(0.2 :: Nil) ::
          Nil
        )
      TestHive.createDataFrame(
        rowRdd,
        StructType(
          StructField("data", ArrayType(DoubleType), true) ::
          Nil)
        )
    }

    assert(DoubleListData.select(add_feature_index($"data")).collect.toSet
      === Set(
        Row(Seq("1:0.8", "2:0.5")),
        Row(Seq("1:0.3", "2:0.1")),
        Row(Seq("1:0.2"))))
  }

  test("mhash") {
    // Assume no exception
    assert(DummyInputData.select(mhash("test")).count == DummyInputData.count)
  }

  test("sha1") {
    // Assume no exception
    assert(DummyInputData.select(sha1("test")).count == DummyInputData.count)
  }

  test("rowid") {
    val df = DummyInputData.select(rowid())
    assert(df.distinct.count == df.count)
  }

  // TODO: Support testing equality between two floating points
  test("rescale") {
   assert(TinyTrainData.select(rescale(2.0f, 1.0, 5.0)).collect.toSet
      === Set(Row(0.25f)))
  }

  test("zscore") {
   assert(TinyTrainData.select(zscore(1.0f, 0.5, 0.5)).collect.toSet
      === Set(Row(1.0f)))
  }

  test("normalize") {
    assert(TinyTrainData.select(normalize($"features")).collect.toSet
      === Set(
        Row(Seq("1:0.9701425", "2:0.24253562")),
        Row(Seq("2:1.0")),
        Row(Seq("1:1.0"))))
  }

  test("sigmoid") {
    /**
     * TODO: SigmodUDF only accepts floating-point types in spark-v1.5.0?
     * This test throws an exception below:
     *
     * [info]   org.apache.spark.sql.catalyst.analysis.UnresolvedException: Invalid call to dataType on unresolved object, tree: 'data
     * [info]   at org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute.dataType(unresolved.scala:59)
     * [info]   at org.apache.spark.sql.hive.HiveSimpleUDF$$anonfun$method$1.apply(hiveUDFs.scala:119)
     */
    val rows = DummyInputData.select(sigmoid($"data")).collect
    assert(rows(0).getDouble(0) ~== 0.500)
    assert(rows(1).getDouble(0) ~== 0.731)
    assert(rows(2).getDouble(0) ~== 0.880)
    assert(rows(3).getDouble(0) ~== 0.952)
  }

  /**
   * TODO: The test below currently fails because Spark
   * can't handle Map<K, V> as a return type in Hive UDF correctly.
   * This issue was reported in SPARK-6921.
   */
  ignore("sort_by_feature") {
    val IntFloatMapData = {
      val rowRdd = TestHive.sparkContext.parallelize(
          Row(Map(1->0.3f, 2->0.1f, 3->0.5f)) ::
          Row(Map(2->0.4f, 1->0.2f)) ::
          Row(Map(2->0.4f, 3->0.2f, 1->0.1f, 4->0.6f)) ::
          Nil
        )
      TestHive.createDataFrame(
        rowRdd,
        StructType(
          StructField("data", MapType(IntegerType, FloatType), true) ::
          Nil)
        )
    }

    checkAnswer(IntFloatMapData.select(sort_by_feature($"data")),
      Seq(
        Row(Map(1->0.3f, 2->0.1f, 3->0.5f),
        Row(Map(1->0.2f, 2->0.4f)),
        Row(Map(1->0.1f, 2->0.4f, 3->0.2f, 4->0.6f))))
    )
  }


  test("invoke regression functions") {
    Seq(
      "train_adadelta",
      "train_adagrad",
      "train_arow_regr",
      "train_arowe_regr",
      "train_arowe2_regr",
      "train_logregr",
      "train_pa1_regr",
      "train_pa1a_regr",
      "train_pa2_regr",
      "train_pa2a_regr"
    )
    .map { func =>
      invokeFunc(new HivemallOps(TinyTrainData), func, Seq($"features", $"label"))
        .foreach(_ => {}) // Just call it
    }
  }

  test("invoke classifier functions") {
    Seq(
      "train_perceptron",
      "train_pa",
      "train_pa1",
      "train_pa2",
      "train_cw",
      "train_arow",
      "train_arowh",
      "train_scw",
      "train_scw2",
      "train_adagrad_rda"
    )
    .map { func =>
      invokeFunc(new HivemallOps(TinyTrainData), func, Seq($"features", $"label"))
        .foreach(_ => {}) // Just call it
    }
  }

  test("invoke multiclass classifier functions") {
    Seq(
      "train_multiclass_perceptron",
      "train_multiclass_pa",
      "train_multiclass_pa1",
      "train_multiclass_pa2",
      "train_multiclass_cw",
      "train_multiclass_arow",
      "train_multiclass_scw",
      "train_multiclass_scw2"
    )
    .map { func =>
      // TODO: Why is a label type [Int|Text] only in multiclass classifiers?
      invokeFunc(new HivemallOps(TinyTrainData), func, Seq($"features", $"label".cast(IntegerType)))
        .foreach(_ => {}) // Just call it
    }
  }

  ignore("check classification precision") {
    Seq(
      "train_adadelta",
      "train_adagrad",
      "train_arow_regr",
      "train_arowe_regr",
      "train_arowe2_regr",
      "train_logregr",
      "train_pa1_regr",
      "train_pa1a_regr",
      "train_pa2_regr",
      "train_pa2a_regr"
    )
    .map { func =>
      checkRegrPrecision(func)
    }
  }

  ignore("check classifier precision") {
    Seq(
      "train_perceptron",
      "train_pa",
      "train_pa1",
      "train_pa2",
      "train_cw",
      "train_arow",
      "train_arowh",
      "train_scw",
      "train_scw2",
      "train_adagrad_rda"
    )
    .map { func =>
      checkClassifierPrecision(func)
    }
  }

  test("user-defined aggregators for ensembles") {
    Seq("voted_avg", "weight_voted_avg")
      .map { udaf =>
        TinyScoreData.groupby().agg("score"->udaf)
          .foreach(_ => {})
      }
  }

  test("user-defined aggregators for evaluation") {
    Seq("mae", "mse", "rmse")
      .map { udaf =>
        invokeFunc(Double2Data.groupby(), udaf, "predict", "target")
      }
    Seq("f1score")
      .map { udaf =>
      invokeFunc(IntList2Data.groupby(), udaf, "predict", "target")
    }
  }

  test("amplify functions") {
    assert(TinyTrainData.amplify(3, $"label", $"features").count() == 9)
    assert(TinyTrainData.rand_amplify(3, 128, $"label", $"features").count() == 9)
    assert(TinyTrainData.part_amplify(3).count() == 9)
  }

  test("lr_datagen") {
    assert(TinyTrainData.lr_datagen("-n_examples 100 -n_features 10 -seed 100").count >= 100)
  }
}

object HivemallOpsSuite {

  val DummyInputData = {
    val rowRdd = TestHive.sparkContext.parallelize(
        (0 until 4).map(Row(_))
      )
    val df = TestHive.createDataFrame(
      rowRdd,
      StructType(
        StructField("data", IntegerType, true) ::
        Nil)
      )
    df
  }

  val IntList2Data = {
    val rowRdd = TestHive.sparkContext.parallelize(
        Row(8 :: 5 :: Nil, 6 :: 4 :: Nil) ::
        Row(3 :: 1 :: Nil, 3 :: 2 :: Nil) ::
        Row(2 :: Nil, 3 :: Nil) ::
        Nil
      )
    TestHive.createDataFrame(
      rowRdd,
      StructType(
        StructField("target", ArrayType(IntegerType), true) ::
        StructField("predict", ArrayType(IntegerType), true) ::
        Nil)
      )
  }

  val Double2Data = {
    val rowRdd = TestHive.sparkContext.parallelize(
        Row(0.8, 0.3) ::
        Row(0.3, 0.9) ::
        Row(0.2, 0.4) ::
        Nil
      )
    TestHive.createDataFrame(
      rowRdd,
      StructType(
        StructField("predict", DoubleType, true) ::
        StructField("target", DoubleType, true) ::
        Nil)
      )
  }

  val TinyTrainData = {
    val rowRdd = TestHive.sparkContext.parallelize(
        Row(0.0, "1:0.8" :: "2:0.2" :: Nil) ::
        Row(1.0, "2:0.7" :: Nil) ::
        Row(0.0, "1:0.9" :: Nil) ::
        Nil
      )
    val df = TestHive.createDataFrame(
      rowRdd,
      StructType(
        StructField("label", DoubleType, true) ::
        StructField("features", ArrayType(StringType), true) ::
        Nil)
      )
    df
  }

  val TinyTestData = {
    val rowRdd = TestHive.sparkContext.parallelize(
        Row(0.0, "1:0.6" :: "2:0.1" :: Nil) ::
        Row(1.0, "2:0.9" :: Nil) ::
        Row(0.0, "1:0.2" :: Nil) ::
        Row(0.0, "2:0.1" :: Nil) ::
        Row(0.0, "0:0.6" :: "2:0.4" :: Nil) ::
        Nil
      )
    val df = TestHive.createDataFrame(
      rowRdd,
      StructType(
        StructField("label", DoubleType, true) ::
        StructField("features", ArrayType(StringType), true) ::
        Nil)
      )
    df
  }

  val TinyScoreData = {
    val rowRdd = TestHive.sparkContext.parallelize(
        Row(0.8f) :: Row(-0.3f) :: Row(0.2f) ::
        Nil
      )
    val df = TestHive.createDataFrame(
      rowRdd,
      StructType(
        StructField("score", FloatType, true) ::
        Nil)
      )
    df
  }

  def invokeFunc(cls: Any, func: String, args: Any*): DataFrame = try {
    // Invoke a function with the given name via reflection
    val im = scala.reflect.runtime.currentMirror.reflect(cls)
    val mSym = im.symbol.typeSignature.member(ru.newTermName(func)).asMethod
    im.reflectMethod(mSym).apply(args: _*)
      .asInstanceOf[DataFrame]
  } catch {
    case e: Exception =>
      assert(false, s"Invoking ${func} failed because: ${e.getMessage}")
      null // Not executed
  }

  def checkRegrPrecision(func: String): Unit = {
    // Build a model
    val model = {
      val res = invokeFunc(new HivemallOps(LargeRegrTrainData),
        func, Seq(add_bias($"features"), $"label"))
      if (!res.columns.contains("conv")) {
        res.groupby("feature").agg("weight"->"avg")
      } else {
        res.groupby("feature").argmin_kld("weight", "conv")
      }
    }.as("feature", "weight")

    // Data preparation
    val testDf = LargeRegrTrainData
      .select(rowid(), $"label".as("target"), $"features")
      .cache

    val testDf_exploded = testDf
      .explode_array($"features")
      .select($"rowid", extract_feature($"feature"), extract_weight($"feature"))

    // Do prediction
    val predict = testDf_exploded
      .join(model, testDf_exploded("feature") === model("feature"), "LEFT_OUTER")
      .select($"rowid", ($"weight" * $"value").as("value"))
      .groupby("rowid").sum("value")
      .as("rowid", "predicted")

    // Evaluation
    val eval = predict
      .join(testDf, predict("rowid") === testDf("rowid"))
      .groupby()
      .agg(Map("target"->"avg", "predicted"->"avg"))
      .as("target", "predicted")

    val diff = eval.map {
      case Row(target: Double, predicted: Double) =>
        Math.abs(target - predicted)
    }.first

    expectResult(diff > 0.10, s"Low precision -> func:${func} diff:${diff}")
  }

  def checkClassifierPrecision(func: String): Unit = {
    // Build a model
    val model = {
      val res = invokeFunc(new HivemallOps(LargeClassifierTrainData),
        func, Seq(add_bias($"features"), $"label"))
      if (!res.columns.contains("conv")) {
        res.groupby("feature").agg("weight"->"avg")
      } else {
        res.groupby("feature").argmin_kld("weight", "conv")
      }
    }.as("feature", "weight")

    // Data preparation
    val testDf = LargeClassifierTestData
      .select(rowid(), $"label".as("target"), $"features")
      .cache

    val testDf_exploded = testDf
      .explode_array($"features")
      .select($"rowid", extract_feature($"feature"), extract_weight($"feature"))

    // Do prediction
    val predict = testDf_exploded
      .join(model, testDf_exploded("feature") === model("feature"), "LEFT_OUTER")
      /**
       * TODO: This sentence throw an exception below:
       *
       * [info]   org.apache.spark.sql.catalyst.analysis.UnresolvedException: Invalid call to dataType on unresolved object, tree: 'value
       * [info]   at org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute.dataType(unresolved.scala:59)
       * [info]   at org.apache.spark.sql.hive.HiveSimpleUDF$$anonfun$method$1.apply(hiveUDFs.scala:119)
       * [info]   at org.apache.spark.sql.hive.HiveSimpleUDF$$anonfun$method$1.apply(hiveUDFs.scala:119)
       */
      .select($"rowid", ($"weight" * $"value").as("value"))
      .groupby("rowid").sum("value")
      .select($"rowid", functions.when(sigmoid($"sum(value)") > 0.50, 1.0).otherwise(0.0).as("predicted"))

    // Evaluation
    val eval = predict
      .join(testDf, predict("rowid") === testDf("rowid"))
      .where($"target" === $"predicted")

    val precision = (eval.count + 0.0) / predict.count

    expectResult(precision < 0.10, s"Low precision -> func:${func} value:${precision}")
  }

  // Only used in this local scope
  private[this] val LargeRegrTrainData = RegressionDatagen.exec(
    TestHive, n_partitions = 2, min_examples = 100000, seed = 3).cache

  private[this] val LargeRegrTestData = RegressionDatagen.exec(
    TestHive, n_partitions = 2, min_examples = 100, seed = 3).cache

  private[this] val LargeClassifierTrainData = RegressionDatagen.exec(
    TestHive, n_partitions = 2, min_examples = 100000, seed = 5, cl = true).cache

  private[this] val LargeClassifierTestData = RegressionDatagen.exec(
    TestHive, n_partitions = 2, min_examples = 100, seed = 5, cl = true).cache
}
