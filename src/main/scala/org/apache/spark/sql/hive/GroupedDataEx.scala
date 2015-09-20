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
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
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

  override def agg(exprs: Map[String, String]): DataFrame = {
    toDF(exprs.map { case (colName, expr) =>
      val a = expr match {
        case "voted_avg" => HiveUDAF(
          new HiveFunctionWrapper("hivemall.ensemble.bagging.VotedAvgUDAF"),
          Seq(df(colName).expr))
        case "weight_voted_avg" => HiveUDAF(
          new HiveFunctionWrapper("hivemall.ensemble.bagging.WeightVotedAvgUDAF"),
          Seq(df(colName).expr))
        case _ => strToExpr(expr)(df(colName).expr)
      }
      Alias(a, a.prettyString)()
    }.toSeq)
  }

  private[this] def checkType(colName: String, expected: DataType) = {
    val dataType = df.resolve(colName).dataType
    if (dataType != expected) {
      throw new AnalysisException(
        s""""$colName" must be $expected, however it is $dataType""")
    }
  }

  /**
   * @see hivemall.ensemble.ArgminKLDistanceUDAF
   */
  def argmin_kld(weight: String, conv: String): DataFrame = {
    // CheckType(weight, NumericType)
    // CheckType(conv, NumericType)
    val udaf = HiveUDAF(
      new HiveFunctionWrapper("hivemall.ensemble.ArgminKLDistanceUDAF"),
      Seq(weight, conv).map(df.resolve))
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.ensemble.MaxValueLabelUDAF"
   */
  def max_label(score: String, label: String): DataFrame = {
    // checkType(score, NumericType)
    checkType(label, StringType)
    val udaf = HiveGenericUDAF(
      new HiveFunctionWrapper("hivemall.ensemble.MaxValueLabelUDAF"),
      Seq(score, label).map(df.resolve))
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.ensemble.MaxRowUDAF
   */
  def maxrow(score: String, label: String): DataFrame = {
    // checkType(score, NumericType)
    checkType(label, StringType)
    val udaf = HiveGenericUDAF(
      new HiveFunctionWrapper("hivemall.ensemble.MaxRowUDAF"),
      Seq(score, label).map(df.resolve))
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.evaluation.FMeasureUDAF
   */
  def f1score(predict: String, target: String): DataFrame = {
    checkType(target, ArrayType(IntegerType))
    checkType(predict, ArrayType(IntegerType))
    val udaf = HiveUDAF(
      new HiveFunctionWrapper("hivemall.evaluation.FMeasureUDAF"),
      Seq(target, predict).map(df.resolve))
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.evaluation.MeanAbsoluteErrorUDAF
   */
  def mae(predict: String, target: String): DataFrame = {
    checkType(predict, DoubleType)
    checkType(target, DoubleType)
    val udaf = HiveUDAF(
      new HiveFunctionWrapper("hivemall.evaluation.MeanAbsoluteErrorUDAF"),
      Seq(predict, target).map(df.resolve))
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.evaluation.MeanSquareErrorUDAF
   */
  def mse(predict: String, target: String): DataFrame = {
    checkType(predict, DoubleType)
    checkType(target, DoubleType)
    val udaf = HiveUDAF(
      new HiveFunctionWrapper("hivemall.evaluation.MeanSquaredErrorUDAF"),
      Seq(predict, target).map(df.resolve))
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.evaluation.RootMeanSquareErrorUDAF
   */
  def rmse(predict: String, target: String): DataFrame = {
    checkType(predict, DoubleType)
    checkType(target, DoubleType)
    val udaf = HiveUDAF(
      new HiveFunctionWrapper("hivemall.evaluation.RootMeanSquaredErrorUDAF"),
      Seq(predict, target).map(df.resolve))
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }
}
