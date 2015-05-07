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
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.HivemallFtVectorizer
import org.apache.spark.ml.utils.RegressionDatagen
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext.implicits._
import org.apache.spark.sql.types._

import xerial.core.util.Timer

import org.scalatest.FunSuite

class HivemallLogressSuite extends FunSuite with Timer {
  import org.apache.spark.sql.hive.HivemallOpsSuite._

  test("tiny training data") {
    // Configure a ML pipeline, which consists of two stages:
    // HivemallFtVectorizer and HivemallLogress
    // TODO: Annoying type casts for labels
    val hivemallPipeline = new Pipeline().setStages(
      Array(
        new HivemallFtVectorizer()
          .setInputCol("features").setOutputCol("ftvec")
          .setDimsParam(1024),
        new HivemallLogress()
          .setFeaturesCol("ftvec")
          .setDimsParam(1024)))

    // Fit the pipeline to tiny training data
    val model = hivemallPipeline.fit(
      TinyTrainData.select(
        $"label".cast(DoubleType).as("label"),
        $"features"))

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

  ignore("benchmark with mllib logistic regression training") {
    // TODO: Annoying type casts for labels
    val synData = RegressionDatagen.exec(
        TestSQLContext,
        min_examples = 10000,
        n_features = 100,
        n_dims = 1024,
        dense = false,
        cl = true) // 0.0 or 0.1 for label of LogisticRegression
      .select(
        $"label".cast(DoubleType).as("label"),
        $"features")
      .cache

    // Transform into spark-specific vectors
    val trainData =
      new HivemallFtVectorizer().setInputCol("features").setOutputCol("ftvec")
        .setDimsParam(1024)
        .transform(synData)
        .cache

    val hivemallLogress = new HivemallLogress()
        .setFeaturesCol("ftvec")
        .setDimsParam(1024)

    val mllibLogress = new LogisticRegression()
        .setFeaturesCol("ftvec")
        .setMaxIter(10)
        .setRegParam(0.01)

    time("training benchmark", repeat = 1) {
      block("hivemall", repeat = 3) {
        hivemallLogress.fit(trainData)
      }
      block("mllib", repeat = 3) {
        mllibLogress.fit(trainData)
      }
    }
  }

  ignore("benchmark with Hivemall outer-join prediction") {
    // TODO: Annoying type casts for labels
    val trainData = RegressionDatagen.exec(
        TestSQLContext,
        min_examples = 10000,
        n_features = 100,
        n_dims = 1024,
        dense = false)
      .select(
        $"label".cast(DoubleType).as("label"),
        $"features")

    // Configure a ML pipeline, which consists of two stages:
    // HivemallFtVectorizer and HivemallLogress
    // TODO: Annoying type casts for labels
    val hivemallPipeline = new Pipeline().setStages(
      Array(
        new HivemallFtVectorizer()
          .setInputCol("features").setOutputCol("ftvec")
          .setDimsParam(1024),
        new HivemallLogress()
          .setFeaturesCol("ftvec")
          .setDimsParam(1024)))

    // Fit the pipeline to generated synthetic training data
    val model = hivemallPipeline.fit(trainData)

    // Generate a sequence of test data for benchmarks
    val testData = RegressionDatagen.exec(
        TestSQLContext,
        min_examples = 10000,
        n_features = 100,
        n_dims = 1024,
        dense = false)

    // Just process it
    def exec(df: DataFrame): Unit = df.head(1)

    time("test/prediction benchmark", repeat = 1) {
      block("hivemall-spark", repeat = 3) {
        exec(model.transform(testData))
      }
      block("hivemall-original", repeat = 3) {
        // exec(...)
      }
    }
  }
}
