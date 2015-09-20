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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{QueryTest, Row}

import scala.collection.mutable.Seq

class HivemallOpsSuite extends QueryTest {
  import org.apache.spark.sql.hive.HivemallOpsSuite._
  import org.apache.spark.sql.hive.test.TestHive.implicits._
  import org.apache.spark.test.TestDoubleWrapper._

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
    val df = LargeTrainData.select(rowid())
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
    val rows = DummyInputData.select(sigmoid($"data".cast(FloatType))).collect
    assert(rows(0).getFloat(0) ~== 0.5f)
    assert(rows(1).getFloat(0) ~== 0.731f)
    assert(rows(2).getFloat(0) ~== 0.880f)
    assert(rows(3).getFloat(0) ~== 0.952f)
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

  test("train_adadelta") {
    assert(
      TinyTrainData
        .train_adadelta(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_adagrad") {
    assert(
      TinyTrainData
        .train_adagrad(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_arow_regr") {
    assert(
      TinyTrainData
        .train_arow_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .argmin_kld("weight", "conv")
        .count() > 0)
  }

  test("train_arowe_regr") {
    assert(
      TinyTrainData
        .train_arowe_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .argmin_kld("weight", "conv")
        .count() > 0)
  }

  test("train_arowe2_regr") {
    assert(
      TinyTrainData
        .train_arowe_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .argmin_kld("weight", "conv")
        .count() > 0)
  }

  test("train_logregr") {
    assert(
      TinyTrainData
        .train_logregr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_pa1_regr") {
    assert(
      TinyTrainData
        .train_pa1_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_pa1a_regr") {
    assert(
      TinyTrainData
        .train_pa1a_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_pa2_regr") {
    assert(
      TinyTrainData
        .train_pa2_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_pa2a_regr") {
    assert(
      TinyTrainData
        .train_pa2a_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_perceptron") {
    assert(
      TinyTrainData
        .train_perceptron(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_pa") {
    assert(
      TinyTrainData
        .train_pa(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_pa1") {
    assert(
      TinyTrainData
        .train_pa1(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_pa2") {
    assert(
      TinyTrainData
        .train_pa2(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_cw") {
    assert(
      TinyTrainData
        .train_cw(add_bias($"features"), $"label")
        .groupby("feature")
        .argmin_kld("weight", "conv")
        .count() > 0)
  }

  test("train_arow") {
    assert(
      TinyTrainData
        .train_arow(add_bias($"features"), $"label")
        .groupby("feature")
        .argmin_kld("weight", "conv")
        .count() > 0)
  }

  test("train_arowh") {
    assert(
      TinyTrainData
        .train_arowh(add_bias($"features"), $"label")
        .groupby("feature")
        .argmin_kld("weight", "conv")
        .count() > 0)
  }

  test("train_scw") {
    assert(
      TinyTrainData
        .train_scw(add_bias($"features"), $"label")
        .groupby("feature")
        .argmin_kld("weight", "conv")
        .count() > 0)
  }

  test("train_scw2") {
    assert(
      TinyTrainData
        .train_scw2(add_bias($"features"), $"label")
        .groupby("feature")
        .argmin_kld("weight", "conv")
        .count() > 0)
  }

  test("train_adagrad_rda") {
    assert(
      TinyTrainData
        .train_adagrad_rda(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_multiclass_perceptron") {
    assert(
      TinyTrainData
        // TODO: Why is a label type [Int|Text] only in multiclass classifiers?
        .train_multiclass_perceptron(add_bias($"features"), $"label".cast(IntegerType))
        .groupby("label", "feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_multiclass_pa") {
    assert(
      TinyTrainData
        .train_multiclass_pa(add_bias($"features"), $"label".cast(IntegerType))
        .groupby("label", "feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_multiclass_pa1") {
    assert(
      TinyTrainData
        .train_multiclass_pa1(add_bias($"features"), $"label".cast(IntegerType))
        .groupby("label", "feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_multiclass_pa2") {
    assert(
      TinyTrainData
        .train_multiclass_pa2(add_bias($"features"), $"label".cast(IntegerType))
        .groupby("label", "feature")
        .agg("weight"->"avg")
        .count() > 0)
  }

  test("train_multiclass_cw") {
    assert(
      TinyTrainData
        .train_multiclass_cw(add_bias($"features"), $"label".cast(IntegerType))
        .groupby("label", "feature")
        .argmin_kld("weight", "conv")
        .count() > 0)
  }

  test("train_multiclass_scw") {
    assert(
      TinyTrainData
        .train_multiclass_scw(add_bias($"features"), $"label".cast(IntegerType))
        .groupby("label", "feature")
        .argmin_kld("weight", "conv")
        .count() > 0)
  }

  test("train_multiclass_scw2") {
    assert(
      TinyTrainData
        .train_multiclass_scw2(add_bias($"features"), $"label".cast(IntegerType))
        .groupby("label", "feature")
        .argmin_kld("weight", "conv")
        .count() > 0)
  }

  test("voted_avg") {
    assert(TinyScoreData.groupby().agg("score"->"voted_avg").count() > 0)
  }

  test("weight_voted_avg") {
    assert(TinyScoreData.groupby().agg("score"->"weight_voted_avg").count() > 0)
  }

  test("f1score") {
    assert(IntList2Data.groupby().f1score("target", "predict").count > 0)
  }

  test("mae") {
    assert(Double2Data.groupby().mae("predict", "target").count > 0)
  }

  test("mse") {
    assert(Double2Data.groupby().mse("predict", "target").count > 0)
  }

  test("rmse") {
    assert(Double2Data.groupby().rmse("predict", "target").count > 0)
  }

  test("amplify") {
    assert(TinyTrainData.amplify(3, $"label", $"features").count() == 9)
  }

  test("rand_amplify") {
    assert(TinyTrainData.rand_amplify(3, 128, $"label", $"features").count() == 9)
  }

  test("part_amplify") {
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

  val LargeTrainData = RegressionDatagen.exec(
    TestHive, n_partitions = 2, min_examples = 10000, seed = 3)

  val LargeTestData = RegressionDatagen.exec(
    TestHive, n_partitions = 2, min_examples = 1000, seed = 15)
}
