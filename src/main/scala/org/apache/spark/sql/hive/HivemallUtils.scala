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

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types._

object HivemallUtils {

  /**
   * An implicit conversion to avoid doing annoying transformation.
   * This class must be in o.a.spark.sql._ because
   * a Column class is private.
   */
  @inline implicit def toIntLiteral(i: Int) = Column(Literal.create(i, IntegerType))
  @inline implicit def toFloatLiteral(i: Float) = Column(Literal.create(i, FloatType))
  @inline implicit def toDoubleLiteral(i: Double) = Column(Literal.create(i, DoubleType))
  @inline implicit def toStringLiteral(i: String) = Column(Literal.create(i, StringType))
}
