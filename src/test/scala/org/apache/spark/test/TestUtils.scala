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

package org.apache.spark.test

import org.apache.spark.Logging

object TestUtils extends Logging {
  def expectResult(res: Boolean, errMsg: String) = if (res) {
    logWarning(errMsg)
  }
}

// TODO: Any same function in o.a.spark.*?
class TestDoubleWrapper(d: Double) {
  // Check an equality between Double values
  def ~==(d: Double): Boolean = Math.abs(this.d - d) < 0.001
}

object TestDoubleWrapper {
  @inline implicit def toTestDoubleWrapper(d: Double) = new TestDoubleWrapper(d)
}
