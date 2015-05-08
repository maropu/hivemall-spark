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

package org.apache.spark.sql.codegen

import org.apache.spark.mllib.linalg.{Vector, SparseVector, DenseVector}

private[sql] object ModelCodegenerator {
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox

  // For reflective compilation
  private val toolBox = runtimeMirror(getClass.getClassLoader).mkToolBox()

  /** Codegen a given linear model. */
  def codegen(weights: Vector, intercept: Double): Vector => Double = {
    val w = weights match {
      case v: DenseVector =>
        q"""Vectors.dense(${v.values})"""
      case v: SparseVector =>
        q"""Vectors.sparse(${v.size}, ${v.indices}, ${v.values})"""
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported vector type: $weights")
    }
    toolBox.eval(
      q"""
          import org.apache.spark.sql.hive.HivemallUtils._
          import org.apache.spark.mllib.linalg.{Vector, Vectors}
          (feature: Vector) => {
            val w = ${w}
            dot(w, feature) + ${intercept}
          }
      """).asInstanceOf[Vector => Double]
  }
}
