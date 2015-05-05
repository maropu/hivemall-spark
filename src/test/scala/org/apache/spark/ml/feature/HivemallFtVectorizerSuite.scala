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

package org.apache.spark.ml.feature

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.TestSQLContext.implicits._

import org.scalatest.FunSuite

class HivemallFtVectorizerSuite extends FunSuite {
  import org.apache.spark.sql.hive.HivemallOpsSuite._

  test("transform into sparse vectors (default)") {
    val vectorizer = new HivemallFtVectorizer()
      .setInputCol("features").setOutputCol("ftvec").setDimsParam(4)
    val df = vectorizer.transform(TinyTrainData)
    val (s_size, s_indices, s_values) = df.select($"ftvec").map(_.get(0))
      .collect.map { case SparseVector(size, indices, values) =>
        (size, indices.toSeq, values.toSeq)
      }
      .ensuring(_.length == 3)
      .unzip3

    assert(s_size(0) == 4)  // Vector 1
    assert(s_indices(0) === Seq(1, 2))
    assert(s_values(0) === Seq(.8d, .2d))
    assert(s_size(1) == 4)  // Vector 2
    assert(s_indices(1) === Seq(2))
    assert(s_values(1) === Seq(.7d))
    assert(s_size(2) == 4)  // Vector 3
    assert(s_indices(2) === Seq(1))
    assert(s_values(2) === Seq(.9d))
  }

  test("transform into dense vectors") {
    val vectorizer = new HivemallFtVectorizer()
      .setInputCol("features").setOutputCol("ftvec").setDenseParam(true).setDimsParam(4)
    val df = vectorizer.transform(TinyTrainData)
    val s_values = df.select($"ftvec").map(_.get(0))
      .collect.map { case DenseVector(values) => values }.toSeq
      .ensuring(_.length === 3)

    assert(s_values(0) === Seq(.0d, .8d, .2d, .0d)) // Vector 1
    assert(s_values(1) === Seq(.0d, .0d, .7d, .0d)) // Vector 2
    assert(s_values(2) === Seq(.0d, .9d, .0d, .0d)) // Vector 3
  }
}
