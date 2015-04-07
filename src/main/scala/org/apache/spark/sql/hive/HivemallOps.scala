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

import hivemall.regression.LogressUDTF
import hivemall.ftvec.AddBiasUDFWrapper

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan}
import org.apache.spark.sql.{Column, DataFrame}

/**
 * A wrapper of hivemall for DataFrame.
 *
 * @groupname regression
 * @groupname ftvec
 */
class HivemallOps(df: DataFrame) {

  /**
   * An implicit conversion to avoid doing annoying transformation.
   */
  @inline private implicit def toDataFrame(logicalPlan: LogicalPlan) =
    DataFrame(df.sqlContext, logicalPlan)

  /**
   * @see hivemall.regression.LogressUDTF
   * @group regression
   */
  def train_logregr(exprs: Column*): DataFrame = {
    Generate(new HiveGenericUdtf(new HiveFunctionWrapper("hivemall.regression.LogressUDTF"), Nil, exprs.map(_.expr)),
      join = true, outer = false, None, df.logicalPlan)
  }
}

object HivemallOps {

  /**
   * Implicitly inject the [[HivemallOps]] into [[DataFrame]].
   */
  implicit def dataFrameToHivemallOps(df: DataFrame): HivemallOps =
    new HivemallOps(df)

  /**
   * An implicit conversion to avoid doing annoying transformation.
   */
  @inline private implicit def toColumn(expr: Expression) = Column(expr)

  /**
   * @see hivemall.ftvec.AddBiasUDF
   * @group ftvec
   */
  def add_bias(exprs: Column*): Column = {
    new HiveGenericUdf(new HiveFunctionWrapper("hivemall.ftvec.AddBiasUDFWrapper"), exprs.map(_.expr))
  }
}