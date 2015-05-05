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

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.HivemallFtVectorizer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.TestSQLContext.implicits._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class HivemallLogressSuite extends FunSuite {
  import org.apache.spark.sql.hive.HivemallOpsSuite._

  test("tiny training data") {
    // Configure a ML pipeline, which consists of two stages: vectorizer and lr
    val vectorizer = new HivemallFtVectorizer().setInputCol("feature").setOutputCol("ftvec")
    val lr = new HivemallLogress().setFeaturesCol("ftvec")
    val pipeline = new Pipeline().setStages(Array(vectorizer, lr))

    // Fit the pipeline to training data
    // TODO: Annoying type casts for labels
    val model = pipeline.fit(
      TinyTrainData.select($"label".cast(DoubleType).as("label"), $"feature"))

    /**
     * Make predictions on test data
     *
     * model.transform(TinyTestData)
     *   .select("ftvec", "label", "prediction")
     *   .collect()
     *   .foreach { case Row(ftvec: Vector, label: Float, prediction: Double) =>
     *   println(s"($ftvec, $label) -> prediction=$prediction")
     * }
     */
  }
}
