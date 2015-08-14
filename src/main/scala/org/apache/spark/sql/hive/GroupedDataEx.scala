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

import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Cube, Rollup}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, GroupedData}

class GroupedDataEx protected[sql](
    df: DataFrame,
    groupingExprs: Seq[Expression],
    private val groupType: GroupedData.GroupType)
  extends GroupedData(df, groupingExprs, groupType) {

  private[this] def toDF(aggExprs: Seq[NamedExpression]): DataFrame = {
    val aggregates = if (df.sqlContext.conf.dataFrameRetainGroupColumns) {
        val retainedExprs = groupingExprs.map {
          case expr: NamedExpression => expr
          case expr: Expression => Alias(expr, expr.prettyString)()
        }
        retainedExprs ++ aggExprs
      } else {
        aggExprs
      }
    groupType match {
      case GroupedData.GroupByType =>
        DataFrame(
          df.sqlContext, Aggregate(groupingExprs, aggregates, df.logicalPlan))
      case GroupedData.RollupType =>
        DataFrame(
          df.sqlContext, Rollup(groupingExprs, df.logicalPlan, aggregates))
      case GroupedData.CubeType =>
        DataFrame(
          df.sqlContext, Cube(groupingExprs, df.logicalPlan, aggregates))
    }
  }

  private[this] def strToExpr(expr: String): (Expression => Expression) = {
    expr.toLowerCase match {
      case "avg" | "average" | "mean" => Average
      case "max" => Max
      case "min" => Min
      case "sum" => Sum
      case "count" | "size" =>
        // Turn count(*) into count(1)
        (inputExpr: Expression) => inputExpr match {
          case s: Star => Count(Literal(1))
          case _ => Count(inputExpr)
        }
    }
  }

  /**
   * A list of added UDAF in [[GroupedDataEx]]:
   *  - voted_avg
   *  - weight_voted_avg
   */
  override def agg(exprs: Map[String, String]): DataFrame = {
    toDF(exprs.map { case (colName, expr) =>
      val a = expr match {
        case "voted_avg" => new HiveUdaf(
          new HiveFunctionWrapper("hivemall.ensemble.bagging.VotedAvgUDAF"),
          Seq(df(colName).expr))
        case "weight_voted_avg" => new HiveUdaf(
          new HiveFunctionWrapper("hivemall.ensemble.bagging.WeightVotedAvgUDAF"),
          Seq(df(colName).expr))
        case _ => strToExpr(expr)(df(colName).expr)
      }
      Alias(a, a.prettyString)()
    }.toSeq)
  }

  // This function assumes that input types is correct
  private[this] def aggHivemallColumn2(col1: String, col2: String)(hiveFunc: HiveFunctionWrapper)
    : DataFrame = {
    val udaf = new HiveUdaf(hiveFunc, Seq(col1, col2).map(df.resolve))
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  // TODO: Need to merge type check codes
  private[this] def checkNumericType(colName: String) = {
    if (!df.resolve(colName).dataType.isInstanceOf[NumericType]) {
      throw new AnalysisException(s""""$colName" must be a numeric column.""")
    }
  }

  private[this] def checkStringType(colName: String) = {
    if (!df.resolve(colName).dataType.isInstanceOf[String]) {
      throw new AnalysisException(s""""$colName" must be a string column.""")
    }
  }

  /**
   * @see hivemall.ensemble.ArgminKLDistanceUDAF
   */
  def argmin_kld(weight: String, conv: String): DataFrame = {
    checkNumericType(weight)
    checkNumericType(conv)
    aggHivemallColumn2(weight, conv)(
      new HiveFunctionWrapper("hivemall.ensemble.ArgminKLDistanceUDAF"))
  }

  /**
   * @see hivemall.ensemble.MaxValueLabelUDAF"
   */
  def max_label(score: String, label: String): DataFrame = {
    checkNumericType(score)
    checkStringType(label)
    aggHivemallColumn2(score, label)(
      new HiveFunctionWrapper("hivemall.ensemble.MaxValueLabelUDAF"))
  }

  /**
   * @see hivemall.ensemble.ArgminKLDistanceUDAF
   */
  def maxrow(score: String, label: String): DataFrame = {
    checkNumericType(score)
    checkStringType(label)
    aggHivemallColumn2(label, label)(
      new HiveFunctionWrapper("hivemall.ensemble.MaxRowUDAF"))
  }
}
