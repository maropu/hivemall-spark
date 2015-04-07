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

import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.test._
import org.apache.spark.sql.test.TestSQLContext.implicits._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class HivemallOpsSuite extends FunSuite {
  import org.apache.spark.sql.hive.HivemallOpsSuite._

  test("add_bias") {
    assert(regressionTinyData.select(add_bias($"feature")).collect.toSet
      == Set(Row(ArrayBuffer("1:0.8", "2:0.2", "0:1.0")),
        Row(ArrayBuffer("2:0.7", "0:1.0")),
        Row(ArrayBuffer("1:0.9", "0:1.0"))))
  }

  ignore("logress") {
    /**
     * TODO: This test currently fails because Spark can't handle
     * HiveGenericUDTF correctly.
     * This issue was reported in SPARK-6734 and a PR of github
     * was made in #5383.
     */
    val test = regressionLargeData.train_logregr(add_bias($"feature"), $"label")
    assert(test.count > 0)
  }
}

object HivemallOpsSuite {

  val regressionTinyData = {
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
    df.registerTempTable("regressionTinyData")
    df
  }

  val regressionLargeData = {
    val train1 = TestSQLContext.sparkContext.parallelize(
      (0 until 10000).map(i => Row(0.0f, Seq("1:" + Random.nextDouble, "2:" + Random.nextDouble))))
    val train2 = TestSQLContext.sparkContext.parallelize(
      (0 until 10000).map(i => Row(1.0f, Seq("1:" + Random.nextDouble, "2:" + (0.5 + Random.nextDouble)))))
    val rowRdd = train1 ++ train2
    val df = TestSQLContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("label", FloatType, true) ::
        StructField("feature", ArrayType(StringType), true) ::
        Nil)
      )
    df.registerTempTable("regressionLargeData")
    df
  }
}
