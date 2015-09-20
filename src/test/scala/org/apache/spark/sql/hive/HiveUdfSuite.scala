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

import org.apache.spark.sql.{QueryTest, Row}

class HiveUdfSuite extends QueryTest {
  import org.apache.spark.sql.hive.HivemallOpsSuite._
  import org.apache.spark.sql.hive.test.TestHive.implicits._
  import org.apache.spark.sql.hive.test.TestHive.sql

  ignore("hivemall_version") {
    sql(s"CREATE TEMPORARY FUNCTION hivemall_version " +
      s"AS '${classOf[hivemall.HivemallVersionUDF].getName}'")
    checkAnswer(
      sql(s"SELECT DISTINCT hivemall_version()"),
      Row("0.3.1")
    )
    // sql("DROP TEMPORARY FUNCTION IF EXISTS hivemall_version")
    // TestHive.reset()
  }

  ignore("train_logregr") {
    TinyTrainData.registerTempTable("TinyTrainData")
    sql(s"CREATE TEMPORARY FUNCTION train_logregr " +
      s"AS '${classOf[hivemall.regression.LogressUDTF].getName}'")
    sql(s"CREATE TEMPORARY FUNCTION add_bias " +
      s"AS '${classOf[hivemall.ftvec.AddBiasUDFWrapper].getName}'")
    val model = sql(
      "SELECT feature, AVG(weight) AS weight " +
        "FROM (SELECT train_logregr(add_bias(features), label) AS (feature, weight)" +
        "  FROM TinyTrainData) t " +
        "GROUP BY feature")
    checkAnswer(model.select($"feature"), Seq(Row("0"), Row("1"), Row("2")))
    // Why 'train_logregr' is not registered in HiveMetaStore
    // ERROR RetryingHMSHandler: MetaException(message:NoSuchObjectException
    //   (message:Function default.train_logregr does not exist))
    //
    // sql("DROP TEMPORARY FUNCTION IF EXISTS train_logregr")
    // TestHive.reset()
  }
}

