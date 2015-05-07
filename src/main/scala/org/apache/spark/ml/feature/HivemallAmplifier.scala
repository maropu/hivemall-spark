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

import org.apache.spark.annotation.DeveloperApi

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.HivemallUtils._

/**
 * Transform Hivemall features into Spark-specific vectors.
 */
class HivemallAmplifier extends Transformer
    with HasLabelCol with HasFeaturesCol with HasScaleFactor with HasBufferNum {

  // Set default values for parameters
  setLabelCol("label")
  setFeaturesCol("features")
  setScaleFactor(3)
  setBufferNum(1024)

  /** @group setParam */
  def setLabelCol(p: String): this.type = set(labelCol, p)

  /** @group setParam */
  def setFeaturesCol(p: String): this.type = set(featuresCol, p)

  /** @group setParam */
  def setScaleFactor(p: Int): this.type = set(scaleParam, p)

  /** @group setParam */
  def setBufferNum(p: Int): this.type = set(nBufferParam, p)

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    val map = this.paramMap ++ paramMap
    // TODO: Handle VectorUDT
    dataset.rand_amplify(
      map(scaleParam), map(nBufferParam),
      // TODO: Replace the arguments below with dataset.col("*")
      dataset.col(map(labelCol)),
      dataset.col(map(featuresCol)))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    /** Validate the input type, and throw an exception if invalid */
    val map = this.paramMap ++ paramMap
    if (!schema.fieldNames.contains(map(labelCol))) {
      throw new IllegalArgumentException(
        s"No label column ${map(labelCol)} exists.")
    }
    if (!schema.fieldNames.contains(map(featuresCol))) {
      throw new IllegalArgumentException(
        s"No features column ${map(featuresCol)} exists.")
    }
    // This transformer does not touch a schema
    schema
  }
}
