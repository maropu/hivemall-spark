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

package org.apache.spark.sql.hive

import org.apache.spark.ml.feature.HivemallFtVectorizer
import org.apache.spark.mllib.linalg.BLAS
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.codegen.ModelCodegenerator
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column

object HivemallUtils {

  /**
   * An implicit conversion to avoid doing annoying transformation.
   */
  @inline implicit def toIntLiteral(i: Int) = Column(Literal(i, IntegerType))
  @inline implicit def toFloatLiteral(i: Float) = Column(Literal(i, FloatType))
  @inline implicit def toDoubleLiteral(i: Double) = Column(Literal(i, DoubleType))
  @inline implicit def toStringLiteral(i: String) = Column(Literal(i, StringType))

  /**
   * Check whether the given schema contains a column of the required data type.
   * @param colName  column name
   * @param dataType  required column data type
   */
  def checkColumnType(schema: StructType, colName: String, dataType: DataType): Unit = {
    val actualDataType = schema(colName).dataType
    require(actualDataType.equals(dataType),
      s"Column $colName must be of type $dataType but was actually $actualDataType.")
  }

  // Free to access dot-product methods for codegen
  def dot(x: Vector, y: Vector): Double = BLAS.dot(x, y)

  // Transform Hivemall features into a Spark-specific vector
  def toVector(features: Seq[String], dense: Boolean = false, dims: Int = 1024): Vector =
    HivemallFtVectorizer.func(dense, dims)(features)

  // Codegen a given linear model
  def codegenModel(weights: Vector, intercept: Double = 0.0): Vector => Double =
    ModelCodegenerator.codegen(weights, intercept)
}
