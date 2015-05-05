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

import org.apache.spark.ml.utils.RegressionDatagen
import org.apache.spark.sql.types._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext.implicits._

import org.scalatest.FunSuite

class HivemallAmplifierSuite extends FunSuite {

  ignore("amplify") {
    // TODO: Annoying type casts for labels
    val data = RegressionDatagen.exec(
        TestSQLContext,
        min_examples = 10000,
        n_features = 100,
        n_dims = 1024,
        dense = false)
      .select(
        $"label".cast(DoubleType).as("label"),
        $"features")

    assert(new HivemallAmplifier().setScaleFactor(3).setBufferNum(1024)
      .transform(data).count == 30000)
  }
}
