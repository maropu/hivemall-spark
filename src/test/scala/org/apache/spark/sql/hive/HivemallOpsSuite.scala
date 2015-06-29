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

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.HivemallUtils._
import org.apache.spark.sql.test._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class HivemallOpsSuite extends FunSuite {
  import org.apache.spark.sql.hive.HivemallOpsSuite._
  import org.apache.spark.sql.test.TestSQLContext.implicits._
  import org.apache.spark.test.TestDoubleWrapper._

  test("hivemall_version") {
    assert(DummyInputData.select(hivemall_version()).head
      === Row("0.3.1"))
  }

  test("add_bias") {
    assert(TinyTrainData.select(add_bias($"features")).collect.toSet
      === Set(
        Row(ArrayBuffer("1:0.8", "2:0.2", "0:1.0")),
        Row(ArrayBuffer("2:0.7", "0:1.0")),
        Row(ArrayBuffer("1:0.9", "0:1.0"))))
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
      val rowRdd = TestSQLContext.sparkContext.parallelize(
          Row(0.8 :: 0.5 :: Nil) ::
          Row(0.3 :: 0.1 :: Nil) ::
          Row(0.2 :: Nil) ::
          Nil
        )
      TestSQLContext.createDataFrame(
        rowRdd,
        StructType(
          StructField("data", ArrayType(DoubleType), true) ::
          Nil)
        )
    }

    assert(DoubleListData.select(add_feature_index($"data")).collect.toSet
      === Set(
        Row(ArrayBuffer("1:0.8", "2:0.5")),
        Row(ArrayBuffer("1:0.3", "2:0.1")),
        Row(ArrayBuffer("1:0.2"))))
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
    assert(DummyInputData.select(rowid())
      .collect.map { case Row(rowid: String) => rowid }.distinct.size == 4)
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
        Row(ArrayBuffer("1:0.9701425", "2:0.24253562")),
        Row(ArrayBuffer("2:1.0")),
        Row(ArrayBuffer("1:1.0"))))
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
      val rowRdd = TestSQLContext.sparkContext.parallelize(
          Row(Map(1 -> 0.3f, 2 -> 0.1f, 3 -> 0.5f)) ::
          Row(Map(2 -> 0.4f, 1 -> 0.2f)) ::
          Row(Map(2 -> 0.4f, 3 -> 0.2f, 1 -> 0.1f, 4 -> 0.6f)) ::
          Nil
        )
      TestSQLContext.createDataFrame(
        rowRdd,
        StructType(
          StructField("data", MapType(IntegerType, FloatType), true) ::
          Nil)
        )
    }

    assert(IntFloatMapData.select(sort_by_feature($"data")).collect.toSet
      === Set(
        Row(Map(1 -> 0.3f, 2 -> 0.1f, 3 -> 0.5f),
        Row(Map(1 -> 0.2f, 2 -> 0.4f)),
        Row(Map(1 -> 0.1f, 2 -> 0.4f, 3 -> 0.2f, 4 -> 0.6f)))))
  }

  test("train_adadelta") {
    assert(
      TinyTrainData
        .train_adadelta(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight" -> "avg")
        .count() > 0)
  }

  test("train_adagrad") {
    assert(
      TinyTrainData
        .train_adagrad(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight" -> "avg")
        .count() > 0)
  }

  test("train_arow_regr") {
    assert(
      TinyTrainData
        .train_arow_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight" -> "avg")
        .count() > 0)
  }

  test("train_arowe_regr") {
    assert(
      TinyTrainData
        .train_arowe_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight" -> "avg")
        .count() > 0)
  }

  test("train_arowe2_regr") {
    assert(
      TinyTrainData
        .train_arowe_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight" -> "avg")
        .count() > 0)
  }

  test("train_logregr") {
    assert(
      TinyTrainData
        .train_logregr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight" -> "avg")
        .count() > 0)
  }

  test("train_pa1_regr") {
    assert(
      TinyTrainData
        .train_pa1_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight" -> "avg")
        .count() > 0)
  }

  test("train_pa1a_regr") {
    assert(
      TinyTrainData
        .train_pa1a_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight" -> "avg")
        .count() > 0)
  }

  test("train_pa2_regr") {
    assert(
      TinyTrainData
        .train_pa2_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight" -> "avg")
        .count() > 0)
  }

  test("train_pa2a_regr") {
    assert(
      TinyTrainData
        .train_pa2a_regr(add_bias($"features"), $"label")
        .groupby("feature")
        .agg("weight" -> "avg")
        .count() > 0)
  }

  test("voted_avg") {
    assert(TinyScoreData.groupby().agg("score" -> "voted_avg").count() > 0)
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

  ignore("lr_datagen") {
    assert(TinyTrainData.lr_datagen("-n_examples 100 -n_features 10 -seed 100") == 100)
  }
}

object HivemallOpsSuite {

  val DummyInputData = {
    val rowRdd = TestSQLContext.sparkContext.parallelize(
        (0 until 4).map(Row(_))
      )
    val df = TestSQLContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("data", IntegerType, true) ::
        Nil)
      )
    df
  }

  val TinyTrainData = {
    val rowRdd = TestSQLContext.sparkContext.parallelize(
        Row(0.0, "1:0.8" :: "2:0.2" :: Nil) ::
        Row(1.0, "2:0.7" :: Nil) ::
        Row(0.0, "1:0.9" :: Nil) ::
        Nil
      )
    val df = TestSQLContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("label", DoubleType, true) ::
        StructField("features", ArrayType(StringType), true) ::
        Nil)
      )
    df
  }

  val TinyTestData = {
    val rowRdd = TestSQLContext.sparkContext.parallelize(
        Row(0.0, "1:0.6" :: "2:0.1" :: Nil) ::
        Row(1.0, "2:0.9" :: Nil) ::
        Row(0.0, "1:0.2" :: Nil) ::
        Row(0.0, "2:0.1" :: Nil) ::
        Row(0.0, "0:0.6" :: "2:0.4" :: Nil) ::
        Nil
      )
    val df = TestSQLContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("label", DoubleType, true) ::
        StructField("features", ArrayType(StringType), true) ::
        Nil)
      )
    df
  }

  val TinyScoreData = {
    val rowRdd = TestSQLContext.sparkContext.parallelize(
        Row(0.8f) :: Row(-0.3f) :: Row(0.2f) ::
        Nil
      )
    val df = TestSQLContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("score", FloatType, true) ::
        Nil)
      )
    df
  }
}
