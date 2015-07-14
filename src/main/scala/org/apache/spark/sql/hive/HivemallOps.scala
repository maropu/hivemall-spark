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
import org.apache.spark.sql._

// Used in explode_array()
private[spark] case class Feature(feature: String)

/**
 * A wrapper of hivemall for DataFrame.
 *
 * @groupname misc
 * @groupname regression
 * @groupname classifier
 * @groupname classifier
 * @groupname classifier.multiclass
 * @groupname ensemble
 * @groupname ensemble.bagging
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
   * @see hivemall.classifier.PerceptronUDTF
   * @group classifier
   */
  def train_perceptron(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.PerceptronUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.PassiveAggressiveUDTF
   * @group classifier
   */
  def train_pa(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.PassiveAggressiveUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.PassiveAggressiveUDTF$PA1
   * @group classifier
   */
  def train_pa1(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.PassiveAggressiveUDTF$PA1"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.PassiveAggressiveUDTF$PA2
   * @group classifier
   */
  def train_pa2(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.PassiveAggressiveUDTF$PA2"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.ConfidenceWeightedUDTF
   * @group classifier
   */
  def train_cw(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.ConfidenceWeightedUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.AROWClassifierUDTF
   * @group classifier
   */
  def train_arow(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.AROWClassifierUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.AROWClassifierUDTF$AROWh
   * @group classifier
   */
  def train_arowh(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.AROWClassifierUDTF$AROWh"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.SoftConfideceWeightedUDTF$SCW1
   * @group classifier
   */
  def train_scw(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.SoftConfideceWeightedUDTF$SCW1"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.SoftConfideceWeightedUDTF$SCW1
   * @group classifier
   */
  def train_scw2(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.SoftConfideceWeightedUDTF$SCW2"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.AdaGradRDAUDTF
   * @group classifier
   */
  def train_adagrad_rda(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.AdaGradRDAUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassPerceptronUDTF
   * @group classifier.multiclass
   */
  def train_multiclass_perceptron(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassPerceptronUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.PassiveAggressiveUDTF
   * @group classifier.multiclass
   */
  def train_multiclass_pa(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.PassiveAggressiveUDTF$PA1
   * @group classifier.multiclass
   */
  def train_multiclass_pa1(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA1"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.PassiveAggressiveUDTF$PA2
   * @group classifier.multiclass
   */
  def train_multiclass_pa2(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA2"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassConfidenceWeightedUDTF
   * @group classifier.multiclass
   */
  def train_multiclass_cw(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassConfidenceWeightedUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassAROWClassifierUDTF
   * @group classifier.multiclass
   */
  def train_multiclass_arow(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassAROWClassifierUDTF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassSoftConfidenceWeightedUDTF$SCW1
   * @group classifier.multiclass
   */
  def train_multiclass_scw(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW1"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassSoftConfidenceWeightedUDTF$SCW2
   * @group classifier.multiclass
   */
  def train_multiclass_scw2(exprs: Column*): DataFrame = {
     Generate(new HiveGenericUdtf(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW2"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * Groups the [[DataFrame]] using the specified columns, so we can run aggregation on them.
   * See [[GroupedDataEx]] for all the available aggregate functions.
   *
   * TODO: This class bypasses the original GroupData
   * so as to support user-defined aggregations.
   * Need a more smart injection into existing DataFrame APIs.
   *
   * A list of added UDAF:
   *  - voted_avg
   *  - weight_voted_avg
   *  - max_label
   *  - maxrow
   *  - argmin_kld
   */
  def groupby(cols: Column*): GroupedDataEx = {
    new GroupedDataEx(df, cols.map(_.expr), GroupedData.GroupByType)
  }

  /**
  /**
   * @see hivemall.ensemble.bagging.VotedAvgUDAF
   * @group ensemble.bagging
   */
  def voted_avg(exprs: Column*): DataFrame = {
     XXX(new HiveUdaf(
        new HiveFunctionWrapper("hivemall.ensemble.bagging.VotedAvgUDAF"),
        exprs.map(_.expr)),
      join=false, outer=false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }*/

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
   * Amplify and shuffle data inside partitions.
   * @group ftvec.amplify
   */
  def part_amplify(xtimes: Int): DataFrame = {
    val rdd = df.rdd.mapPartitions({ iter =>
      val elems = iter.flatMap{ row =>
        Seq.fill[Row](xtimes)(row)
      }
      scala.util.Random.shuffle(elems)
    }, true)
    df.sqlContext.createDataFrame(rdd, df.schema)
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
      v.map(s => Feature(s.asInstanceOf[String]))
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
    new HiveGenericUdf(new HiveFunctionWrapper(
      "hivemall.ftvec.ExtractFeatureUDFWrapper"), exprs.map(_.expr))
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
    // HadoopUtils#getTaskId() does not work correctly in Spark, so
    // monotonicallyIncreasingId() is used for rowid() instead of
    // hivemall.tools.mapred.RowIdUDFWrapper.
    functions.monotonicallyIncreasingId()
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
