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

package org.apache.spark.ml.utils

import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.HivemallUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object RegressionDatagen {

  /**
   * Generate data for regression/classification.
   * See [[hivemall.dataset.LogisticRegressionDataGeneratorUDTF]]
   * for the details of arguments below.
   */
  def exec(sc: SQLContext,
           n_examples: Int = 1000,
           n_features: Int = 10,
           n_dims: Int = 200,
           seed: Int = 43,
           dense: Boolean = false,
           prob_one: Float = 0.6f,
           sort:Boolean = false,
           cl: Boolean = false): DataFrame = {
    val df = sc.createDataFrame(
      sc.sparkContext.parallelize(Row(0) :: Nil),
        StructType(
          StructField("data", IntegerType, true) ::
          Nil)
      )
    import sc.implicits._
    df.lr_datagen(
      s"-n_examples $n_examples -n_features $n_features -n_dims $n_dims -prob_one $prob_one"
        + (if (dense) " -dense" else "")
        + (if (sort) " -sort" else "")
        + (if (cl) " -cl" else ""))
      .select($"_c0".as("label"), $"_c1".as("feature"))
  }
}
