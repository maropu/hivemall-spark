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

package org.apache.spark.ml.evaluation

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.DoubleType

/**
 * :: AlphaComponent ::
 *
 * Evaluator for regression, which expects two input columns: prediction and label.
 */
@AlphaComponent
class RegressionEvaluator extends Evaluator with Params
with HasPredictionCol with HasLabelCol {

  /**
   * param for metric name in evaluation
   * @group param
   */
  val metricName: Param[String] = new Param(this, "metricName",
    "metric name in evaluation "
      + "(explainedVariance|meanAbsoluteError|meanSquaredError|rootMeanSquaredError|r2)",
    Some("meanSquaredError"))

  /** @group getParam */
  def getMetricName: String = get(metricName)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  override def evaluate(dataset: DataFrame, paramMap: ParamMap): Double = {
    val map = this.paramMap ++ paramMap

    val schema = dataset.schema
    checkInputColumn(schema, map(predictionCol), DoubleType)
    checkInputColumn(schema, map(labelCol), DoubleType)

    val predictionAndLabels = dataset.select(map(predictionCol), map(labelCol))
      .map { case Row(prediction: Double, label: Double) =>
      (prediction, label)
    }
    val metrics = new RegressionMetrics(predictionAndLabels)
    val metric = map(metricName) match {
      case "explainedVariance" =>
        metrics.explainedVariance
      case "meanAbsoluteError" =>
        metrics.meanAbsoluteError
      case "meanSqaureError" =>
        metrics.meanSquaredError
      case "rootMeanSqaureError" =>
        metrics.rootMeanSquaredError
      case "r2" =>
        metrics.r2
      case other =>
        throw new IllegalArgumentException(s"Does not support metric $other.")
    }
    metric
  }
}