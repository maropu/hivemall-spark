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

import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.{QueryTest, Row}

class HiveUdfSuite extends QueryTest {
  import org.apache.spark.sql.hive.test.TestHive.sql
  import org.apache.spark.sql.hive.HivemallOpsSuite._

  ignore("train_logregr") {
    TinyTrainData.registerTempTable("TinyTrainData")
    sql(s"CREATE TEMPORARY FUNCTION train_logregr AS '${classOf[hivemall.regression.LogressUDTF].getName}'")
    checkAnswer(
      sql("SELECT train_logregr(label, features) FROM TinyTrainData"),
      Seq(Row("1", 0.5), Row("2", 0.1)))
    sql("DROP TEMPORARY FUNCTION IF EXISTS train_logregr")
    TestHive.reset()
  }
}

