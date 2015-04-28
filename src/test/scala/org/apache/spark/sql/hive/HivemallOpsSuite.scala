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

import org.apache.spark.sql.catalyst.expressions.{EmptyRow, Literal}
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.HivemallUtils._
import org.apache.spark.sql.test._
import org.apache.spark.sql.test.TestSQLContext.implicits._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class HivemallOpsSuite extends FunSuite {
  import HivemallOpsSuite._

  test("add_bias") {
    assert(TinyTrainData.select(add_bias($"feature")).collect.toSet
      == Set(Row(ArrayBuffer("1:0.8", "2:0.2", "0:1.0")),
        Row(ArrayBuffer("2:0.7", "0:1.0")),
        Row(ArrayBuffer("1:0.9", "0:1.0"))))
  }

  test("extract_feature") {
    assert(TinyTrainData.select(extract_feature($"feature")).collect.toSet
      == Set(Row(ArrayBuffer("1", "2")),
        Row(ArrayBuffer("2")),
        Row(ArrayBuffer("1"))))
  }

  test("extract_weight") {
    assert(TinyTrainData.select(extract_weight($"feature")).collect.toSet
      == Set(Row(ArrayBuffer(0.8f, 0.2f)),
        Row(ArrayBuffer(0.7f)),
        Row(ArrayBuffer(0.9f))))
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
      == Set(Row(ArrayBuffer("1:0.8", "2:0.5")),
        Row(ArrayBuffer("1:0.3", "2:0.1")),
        Row(ArrayBuffer("1:0.2"))))
  }

  // TODO: Support testing equality between two floating points
  test("rescale") {
   assert(TinyTrainData.select(rescale(2.0f, 1.0d, 5.0d)).collect.toSet
      == Set(Row(0.25f)))
  }

  test("zscore") {
   assert(TinyTrainData.select(zscore(1.0f, 0.5d, 0.5d)).collect.toSet
      == Set(Row(1.0f)))
  }

  ignore("normalize") {
    assert(TinyTrainData.select(normalize($"feature")).collect.toSet
      == Set(Row(ArrayBuffer("1:0.9701425", "2:0.24253562")),
        Row(ArrayBuffer("2:1.0")),
        Row(ArrayBuffer("1:1.0"))))
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
      == Set(Row(Map(1 -> 0.3f, 2 -> 0.1f, 3 -> 0.5f),
        Row(Map(1 -> 0.2f, 2 -> 0.4f)),
        Row(Map(1 -> 0.1f, 2 -> 0.4f, 3 -> 0.2f, 4 -> 0.6f)))))
  }

  /**
   * TODO: The tests below currently fail because Spark
   * can't handle HiveGenericUDTF correctly.
   * This issue was reported in SPARK-6734 and a PR of github
   * was made in #5383.
   */
  ignore("train_adadelta") {
    val test = LargeTrainData.train_adadelta(add_bias($"feature"), $"label")
    assert(test.count > 0)
  }
  ignore("train_adagrad") {
    val test = LargeTrainData.train_adagrad(add_bias($"feature"), $"label")
    assert(test.count > 0)
  }
  ignore("train_arow_regr") {
    val test = LargeTrainData.train_arow_regr(add_bias($"feature"), $"label")
    assert(test.count > 0)
  }
  ignore("train_logregr") {
    val test = LargeTrainData.train_logregr(add_bias($"feature"), $"label")
    assert(test.count > 0)
  }
  ignore("amplify") {
    val test = LargeTrainData.amplify(3, $"*")
    assert(test.count > 0)
  }
  ignore("rand_amplify") {
    val test = LargeTrainData.rand_amplify(3, $"*")
    assert(test.count > 0)
  }
}

object HivemallOpsSuite {

  val TinyTrainData = {
    val rowRdd = TestSQLContext.sparkContext.parallelize(
        Row(0.0f, "1:0.8" :: "2:0.2" :: Nil) ::
        Row(1.0f, "2:0.7" :: Nil) ::
        Row(0.0f, "1:0.9" :: Nil) ::
        Nil
      )
    val df = TestSQLContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("label", FloatType, true) ::
        StructField("feature", ArrayType(StringType), true) ::
        Nil)
      )
    df.registerTempTable("TinyTrainData")
    df
  }

  val LargeTrainData = {
    val train1 = TestSQLContext.sparkContext.parallelize(
      (0 until 10000).map { i =>
        Row(0.0f, Seq("1:" + Random.nextDouble, "2:" + Random.nextDouble))
      })
    val train2 = TestSQLContext.sparkContext.parallelize(
      (0 until 10000).map { i =>
        Row(1.0f, Seq("1:" + Random.nextDouble, "2:" + (0.5 + Random.nextDouble)))
      })
    val rowRdd = train1 ++ train2
    val df = TestSQLContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("label", FloatType, true) ::
        StructField("feature", ArrayType(StringType), true) ::
        Nil)
      )
    df.registerTempTable("LargeTrainData")
    df
  }
}
