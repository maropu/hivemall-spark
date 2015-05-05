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

package org.apache.spark.ml.regression

import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.test._
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class HivemallLogressSuite extends FunSuite {
  import org.apache.spark.ml.regression.HivemallLogressSuite._

  test("tiny training data") {
    val result = HivemallLogress().setDimsParam(3)
      .fit(TinyTrainData)
      .transform(TinyTestData)
      .select("features", "label", "prediction")
      .collect()

    assert(result.length > 0)
  }
}

// For syntax sugar in SQLContext#createDataFrame()
case object VectorType extends VectorUDT

object HivemallLogressSuite {

  /**
   * TODO: Throw an exception in scala reflection
   * when LabeledPoint used, and why?
   */
  val TinyTrainData = TestSQLContext.createDataFrame(
     TestSQLContext.sparkContext.parallelize(
        Row(1.0, Vectors.dense(0.0, 1.1, 0.1)) ::
        Row(0.0, Vectors.dense(2.0, 1.0, 0.9)) ::
        Row(0.0, Vectors.dense(2.0, 1.3, 1.0)) ::
        Row(1.0, Vectors.dense(0.0, 1.2, 0.5)) ::
        Nil
      ),
      StructType(
        StructField("label", DoubleType, true) ::
        StructField("features", VectorType, true) ::
        Nil)
      )

  val TinyTestData = TestSQLContext.createDataFrame(
     TestSQLContext.sparkContext.parallelize(
        Row(1.0, Vectors.dense(0.0, 1.1, 0.1)) ::
        Row(0.0, Vectors.dense(2.0, 1.0, 0.9)) ::
        Row(0.0, Vectors.dense(2.0, 1.3, 1.0)) ::
        Row(1.0, Vectors.dense(0.0, 1.2, 0.5)) ::
        Nil
      ),
      StructType(
        StructField("label", DoubleType, true) ::
        StructField("features", VectorType, true) ::
        Nil)
      )
}
