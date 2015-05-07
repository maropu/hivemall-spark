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

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{HasDimsParam, HasDenseParam, BooleanParam, ParamMap}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vectors, VectorUDT, Vector}
import org.apache.spark.sql.types.{ArrayType, StringType, DataType}

/**
 * Transform Hivemall features into Spark-specific vectors.
 */
class HivemallFtVectorizer
    extends UnaryTransformer[Seq[String], Vector, HivemallFtVectorizer]
    with HasDenseParam with HasDimsParam {

  // Set default values for parameters
  setDenseParam(false)
  setDimsParam(1024)

  /** @group setParam */
  def setDenseParam(p: Boolean): this.type = set(denseParam, p)

  /** @group setParam */
  def setDimsParam(p: Int): this.type = set(dimsParam, p)

  override protected def createTransformFunc(paramMap: ParamMap)
      : Seq[String] => Vector = {
    val map = this.paramMap ++ paramMap
    if (map(denseParam)) {
      // Dense features
      i: Seq[String] => {
        val features = new Array[Double](map(dimsParam))
        i.map { ft =>
          val s = ft.split(":").ensuring(_.size == 2)
          features(s(0).toInt) = s(1).toDouble
        }
        Vectors.dense(features)
      }
    } else {
      // Sparse features
      i: Seq[String] => {
        val features = i.map { ft =>
          val s = ft.split(":").ensuring(_.size == 2)
          (s(0).toInt, s(1).toDouble)
        }
        Vectors.sparse(map(dimsParam), features)
      }
    }
  }

  override protected def outputDataType: DataType = new VectorUDT()

  /** Validate the input type, and throw an exception if invalid */
  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == ArrayType(StringType, true),
      s"Input type must be Array[String], but got $inputType.")
  }
}
