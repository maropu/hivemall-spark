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

import java.util.NoSuchElementException

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.param._
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.HivemallUtils._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
 * Params for logistic regression in Hivemall.
 */
private[regression] trait HivemallLogressParams extends RegressorParams
  with HasBiasParam with HasDenseParam with HasDimsParam
  with HasStepNumParam  // -total_steps
  with HasExponentParam // -power_t
  with HasEta0Param     // -eta0

/**
 * Hivemall logistic regression.
 */
@AlphaComponent
class HivemallLogress extends Regressor[Vector, HivemallLogress, HivemallLogressModel]
  with HivemallLogressParams {

  // Set default values for parameters
  setBiasParam(true)
  setDenseParam(false)
  setDimsParam(1024)
  setStepNumParam(-1)

  /** @group setParam */
  def setBiasParam(p: Boolean): this.type = set(biasParam, p)

  /** @group setParam */
  def setDenseParam(p: Boolean): this.type = set(denseParam, p)

  /** @group setParam */
  def setDimsParam(p: Int): this.type = set(dimsParam, p)

  /** @group setParam */
  def setStepNumParam(p: Int): this.type = set(nStepParam, p)

  /** @group setParam */
  def setPowerParam(p: Double): this.type = set(powerParam, p)

  /** @group setParam */
  def setEta0Param(p: Double): this.type = set(eta0Param, p)

  override protected def train(dataset: DataFrame, paramMap: ParamMap): HivemallLogressModel = {
    // Extract label points from dataset. If dataset is persisted, do not persist labelPoints.
    val labelPoints = extractLabeledPoints(dataset, paramMap)
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      labelPoints.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // Transform LabelPoint into hivemall-specific formats
    val hmData = labelPoints.map { x =>
      val features = x.features match {
        case sx: SparseVector =>
          (0 until sx.indices.length).map {
            d => sx.indices(d) + ":" + sx.values(d)
          }
        case dx: DenseVector =>
          dx.toArray.toSeq.zipWithIndex.map { case (value, d) =>
            (d + 1) + ":" + value
          }
        case _ =>
          throw new IllegalArgumentException(
            s"HivemallLogress doesn't support vector type ${x.getClass}.")
      }
      Row(x.label.toFloat, features)
    }

    // Process given options for Hivemall
    val options = new StringBuilder
    if (paramMap(denseParam)) options.append("-dense ")
    if (paramMap(dimsParam) > 0) options.append(s"-dims ${paramMap(dimsParam)} ")
    if (paramMap(nStepParam) > 0) options.append(s"-total_steps ${paramMap(nStepParam)} ")

    try {
      val p = paramMap(powerParam)
      options.append(s"-total_steps ${p} ")
    } catch {
      case e: NoSuchElementException =>
        // Do nothing
    }

    try {
      val p = paramMap(eta0Param)
      options.append(s"-eta0 ${p} ")
    } catch {
      case e: NoSuchElementException =>
        // Do nothing
    }

    import dataset.sqlContext.implicits._

    // Train model
    val df = dataset.sqlContext.createDataFrame(
        hmData,
        StructType(
          StructField("label", FloatType, true) ::
          StructField("features", ArrayType(StringType), true) ::
          Nil)
      )
      .train_logregr(
        if (paramMap(denseParam)) add_bias($"features") else $"features",
        $"label",
        options.toString)
      .select(
        $"_c0".cast(IntegerType).as("feature"),
        $"_c1".as("weight"))
      .groupBy($"feature")
        .agg("weight" -> "avg")

    val hmModel = df.select(
        $"feature",
        // TODO: Any smart handling to rename this column?
        df(df.columns(1)).as("weight"))

    val retModel = if (hmModel.count > 0) {
      // Extract intercept from trained weights
      val intercept = hmModel.where($"feature" === 0) match {
        case df if df.count == 1 => df.select($"weight").map(_.getDouble(0)).reduce(_ + _)
        case df =>.0d
      }

      // Wrap weights with Vector
      val weights = hmModel.where($"feature" !== 0).sort($"feature") match {
        case d =>
          if (paramMap(denseParam)) {
            // Dense weights
            Vectors.dense(d.select($"weight").map(_.getDouble(0)).collect)
          } else {
            // Sparse weights
            val data = d.map(row => (row.getInt(0), row.getDouble(1))).collect
            Vectors.sparse(data.length, data)
          }
      }

      new HivemallLogressModel(this, paramMap, weights, intercept)
    } else {
      // Invalid weights
      new HivemallLogressModel(
        this, paramMap,
        Vectors.dense(new Array[Double](paramMap(dimsParam))),
        .0d)
    }

    if (handlePersistence) {
      labelPoints.unpersist()
    }

    retModel
  }
}

/**
 * Model produced by [[HivemallLogress]].
 */
@AlphaComponent
class HivemallLogressModel private[ml] (
    override val parent: HivemallLogress,
    override val fittingParamMap: ParamMap,
    val weights: Vector,
    val intercept: Double)
  extends RegressionModel[Vector, HivemallLogressModel]
  with HivemallLogressParams {

  override protected def predict(features: Vector): Double = {
    BLAS.dot(features, weights) + intercept
  }

  override protected def copy(): HivemallLogressModel = {
    val m = new HivemallLogressModel(parent, fittingParamMap, weights, intercept)
    Params.inheritValues(this.paramMap, this, m)
    m
  }
}
