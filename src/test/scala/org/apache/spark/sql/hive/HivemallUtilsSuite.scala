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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class HivemallUtilsSuite extends FunSuite {
  import org.apache.spark.test.TestDoubleWrapper._

  test("transformHivemallModel") {
    val hivemallModel = {
      val rowRdd = TestSQLContext.sparkContext.parallelize(
          Row("0", 0.34f) ::  // intercept
          Row("1", -0.12f) ::
          Row("2", 0.80f) ::
          Row("4", 0.49f) ::
          Row("8", -0.47f) ::
          Nil
        )
      TestSQLContext.createDataFrame(
        rowRdd,
        StructType(
          StructField("feature", StringType, true) ::
          StructField("weight", FloatType, true) ::
          Nil)
        )
    }

    val (weights, intercept) =
      HivemallUtils.transformHivemallModel(hivemallModel)

    assert(weights.toString.equals("(1024,[1,2,4,8],[-0.12,0.8,0.49,-0.47])"))
    assert(intercept ~== 0.34)
  }
}
