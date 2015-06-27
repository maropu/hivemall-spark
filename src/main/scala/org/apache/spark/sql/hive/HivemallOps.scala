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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{ScalaUdf, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, Column, DataFrame}

// Used in explode_array()
case class Ft(feature: String)

/**
 * A wrapper of hivemall for DataFrame.
 *
 * @groupname misc
 * @groupname regression
 * @groupname ftvec
 * @groupname ftvec.amplify
 * @groupname ftvec.hashing
 * @groupname ftvec.scaling
 * @groupname tools.mapred
 * @groupname tools.math
 * @groupname dataset
 */
class HivemallOps(df: DataFrame) {

  /**
   * An implicit conversion to avoid doing annoying transformation.
   */
  @inline private implicit def toDataFrame(logicalPlan: LogicalPlan) =
    DataFrame(df.sqlContext, logicalPlan)

  /**
   * @see hivemall.regression.AdaDeltaUDTF
   * @group regression
   */
  def train_adadelta(exprs: Column*): DataFrame = {
    Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.regression.AdaDeltaUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.AdaGradUDTF
   * @group regression
   */
  def train_adagrad(exprs: Column*): DataFrame = {
    Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.regression.AdaGradUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.AROWRegressionUDTF
   * @group regression
   */
  def train_arow_regr(exprs: Column*): DataFrame = {
    Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.regression.AROWRegressionUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.AROWRegressionUDTF$AROWe
   * @group regression
   */
  def train_arowe_regr(exprs: Column*): DataFrame = {
    Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.regression.AROWRegressionUDTF$AROWe"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.AROWRegressionUDTF$AROWe2
   * @group regression
   */
  def train_arowe2_regr(exprs: Column*): DataFrame = {
    Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.regression.AROWRegressionUDTF$AROWe2"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.LogressUDTF
   * @group regression
   */
  def train_logregr(exprs: Column*): DataFrame = {
    Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.regression.LogressUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.PassiveAggressiveRegressionUDTF
   * @group regression
   */
  def train_pa1_regr(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.regression.PassiveAggressiveRegressionUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.PassiveAggressiveRegressionUDTF.PA1a
   * @group regression
   */
  def train_pa1a_regr(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.regression.PassiveAggressiveRegressionUDTF$PA1a"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.PassiveAggressiveRegressionUDTF.PA2
   * @group regression
   */
  def train_pa2_regr(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.regression.PassiveAggressiveRegressionUDTF$PA2"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.PassiveAggressiveRegressionUDTF.PA2a
   * @group regression
   */
  def train_pa2a_regr(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.regression.PassiveAggressiveRegressionUDTF$PA2a"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.ftvec.amplify.AmplifierUDTF
   * @group ftvec.amplify
   */
  def amplify(exprs: Column*): DataFrame = {
    val outputAttr = exprs.drop(1).map {
      case Column(expr: NamedExpression) => UnresolvedAttribute(expr.name)
      case Column(expr: Expression) => UnresolvedAttribute(expr.prettyString)
    }
    Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.ftvec.amplify.AmplifierUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      outputAttr,
      df.logicalPlan)
  }

  /**
   * @see hivemall.ftvec.amplify.RandomAmplifierUDTF
   * @group ftvec.amplify
   */
  def rand_amplify(exprs: Column*): DataFrame = {
    val outputAttr = exprs.drop(2).map {
      case Column(expr: NamedExpression) => UnresolvedAttribute(expr.name)
      case Column(expr: Expression) => UnresolvedAttribute(expr.prettyString)
    }
    Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.ftvec.amplify.RandomAmplifierUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      outputAttr,
      df.logicalPlan)
  }

  /**
   * @see hivemall.dataset.LogisticRegressionDataGeneratorUDTF
   * @group dataset
   */
  def lr_datagen(exprs: Column*): DataFrame = {
    Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.dataset.LogisticRegressionDataGeneratorUDTFWrapper"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("label", "features").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * Split Seq[String] into pieces.
   * @group ftvec
   */
  def explode_array(input: String): DataFrame = {
    df.explode(df.col(input)) { case Row(v: Seq[_]) =>
      v.map(s => Ft(s.asInstanceOf[String]))
    }
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
   * @see hivemall.HivemallVersionUDF
   * @group misc
   */
  def hivemall_version(): Column = {
    new HiveSimpleUdf(new HiveFunctionWrapper(
      "hivemall.HivemallVersionUDF"), Nil)
  }

  /**
   * @see hivemall.ftvec.AddBiasUDF
   * @group ftvec
   */
  def add_bias(exprs: Column*): Column = {
    new HiveGenericUdf(new HiveFunctionWrapper(
      "hivemall.ftvec.AddBiasUDFWrapper"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.ExtractFeatureUdf
   * @group ftvec
   *
   * TODO: This throws java.lang.ClassCastException because
   * HiveInspectors.toInspector has a bug in spark.
   * Need to fix it later.
   */
  def extract_feature(exprs: Column*): Column = {
    // new HiveGenericUdf(new HiveFunctionWrapper(
    //   "hivemall.ftvec.ExtractFeatureUDFWrapper"), exprs.map(_.expr))
    val f: String => String = (s: String) => {
      s.split(':') match {
        case d if d.size == 2 => d(0)
        case _ => ""
      }
    }
    Column(ScalaUdf(f, StringType, exprs.map(_.expr)))
  }

  /**
   * @see hivemall.ftvec.ExtractWeightUdf
   * @group ftvec
   *
   * TODO: This throws java.lang.ClassCastException because
   * HiveInspectors.toInspector has a bug in spark.
   * Need to fix it later.
   */
  def extract_weight(exprs: Column*): Column = {
    // new HiveGenericUdf(new HiveFunctionWrapper(
    //   "hivemall.ftvec.ExtractWeightUDFWrapper"), exprs.map(_.expr))
    val f: String => String = (s: String) => {
      s.split(':') match {
        case d if d.size == 2 => d(1)
        case _ => ""
      }
    }
    Column(ScalaUdf(f, StringType, exprs.map(_.expr)))
  }

  /**
   * @see hivemall.ftvec.AddFeatureIndexUDFWrapper
   * @group ftvec
   */
  def add_feature_index(exprs: Column*): Column = {
    new HiveGenericUdf(new HiveFunctionWrapper(
      "hivemall.ftvec.AddFeatureIndexUDFWrapper"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.SortByFeatureUDF
   * @group ftvec
   */
  def sort_by_feature(exprs: Column*): Column = {
    new HiveSimpleUdf(new HiveFunctionWrapper(
      "hivemall.ftvec.SortByFeatureUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.hashing.MurmurHash3UDF
   * @group ftvec.hashing
   */
  def mhash(exprs: Column*): Column = {
    new HiveSimpleUdf(new HiveFunctionWrapper(
      "hivemall.ftvec.hashing.MurmurHash3UDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.hashing.Sha1UDF
   * @group ftvec.hashing
   */
  def sha1(exprs: Column*): Column = {
    new HiveSimpleUdf(new HiveFunctionWrapper(
      "hivemall.ftvec.hashing.Sha1UDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.scaling.RescaleUDF
   * @group ftvec.scaling
   */
  def rescale(exprs: Column*): Column = {
    new HiveSimpleUdf(new HiveFunctionWrapper(
      "hivemall.ftvec.scaling.RescaleUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.scaling.ZScoreUDF
   * @group ftvec.scaling
   */
  def zscore(exprs: Column*): Column = {
    new HiveSimpleUdf(new HiveFunctionWrapper(
      "hivemall.ftvec.scaling.ZScoreUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.scaling.L2NormalizationUDF
   * @group ftvec.scaling
   */
  def normalize(exprs: Column*): Column = {
    new HiveGenericUdf(new HiveFunctionWrapper(
      "hivemall.ftvec.scaling.L2NormalizationUDFWrapper"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.tools.mapred.RowIdUDF
   * @group tools.mapred
   */
  def rowid(): Column = {
    new HiveGenericUdf(new HiveFunctionWrapper(
      "hivemall.tools.mapred.RowIdUDFWrapper"), Nil)
  }

  /**
   * @see hivemall.tools.math.SigmoidUDF
   * @group tools.math
   */
  def sigmoid(exprs: Column*): Column = {
    new HiveSimpleUdf(new HiveFunctionWrapper(
      "hivemall.tools.math.SigmodUDF"), exprs.map(_.expr))
  }
}
