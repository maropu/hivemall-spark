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

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.TestSQLContext.implicits._

import org.scalatest.FunSuite

class HivemallFtVectorizerSuite extends FunSuite {
  import org.apache.spark.sql.hive.HivemallOpsSuite._

  test("transform into sparse vectors") {
    val vectorizer = new HivemallFtVectorizer().setInputCol("feature").setOutputCol("ftvec")
    val df = vectorizer.transform(TinyTrainData)
    val (s_size, s_indices, s_values) = df.select($"ftvec").map(_.get(0))
      .collect.map { case SparseVector(size, indices, values) =>
        (size, indices.toSeq, values.toSeq)
      }
      .ensuring(_.length == 3)
      .unzip3

    // Vector 1
    assert(s_size(0) == 1024)
    assert(s_indices(0) === Seq(1, 2))
    assert(s_values(0) === Seq(0.8d, 0.2d))

    // Vector 2
    assert(s_size(1) == 1024)
    assert(s_indices(1) === Seq(2))
    assert(s_values(1) === Seq(0.7d))

    // Vector 3
    assert(s_size(2) == 1024)
    assert(s_indices(2) === Seq(1))
    assert(s_values(2) === Seq(0.9d))
  }
}
