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

package org.apache.spark.ml

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

/**
 * This is a wrapper for [[org.apache.spark.ml.Pipeline]] in Spark.
 * This modification excludes some transformers only
 * for training when passing into PipelineModel.
 */
class MLPipeline extends Pipeline {

  override def fit(dataset: DataFrame, paramMap: ParamMap): PipelineModel = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = this.paramMap ++ paramMap
    val theStages = map(stages)
    // Search for the last estimator.
    var indexOfLastEstimator = -1
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      stage match {
        case _: Estimator[_] =>
          indexOfLastEstimator = index
        case _ =>
      }
    }
    var curDataset = dataset
    val transformers = ListBuffer.empty[Transformer]
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      if (index <= indexOfLastEstimator) {
        val transformer = stage match {
          case estimator: Estimator[_] =>
            estimator.fit(curDataset, paramMap)
          case t: Transformer =>
            t
          case _ =>
            throw new IllegalArgumentException(
              s"Do not support stage $stage of type ${stage.getClass}")
        }
        if (index < indexOfLastEstimator) {
          curDataset = transformer.transform(curDataset, paramMap)
        }
        transformer match {
          case t: TrainOnlyTransfomer => // Skip unnecessary transformers in PipelineModel
          case _ => transformers += transformer
        }
      } else {
        transformers += stage.asInstanceOf[Transformer]
      }
    }

    new PipelineModel(this, map, transformers.toArray)
  }
}
