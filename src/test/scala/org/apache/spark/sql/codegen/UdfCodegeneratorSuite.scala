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

package org.apache.spark.sql.codegen

import org.apache.spark.mllib.linalg.Vectors

import org.scalatest.FunSuite

class UdfCodegeneratorSuite extends FunSuite {
  import org.apache.spark.test.TestDoubleWrapper._

  test("dense model codegen") {
    val weights = Vectors.dense(Array(0.1, 0.4, 0.2, 0.3))
    val intercept: Double = 0.25
    def codegenFunc = UdfCodegenerator.codegen(weights, intercept)
    val denseFtvec = Vectors.dense(Array(0.3, 0.2, 0.1, 0.1))
    assert(codegenFunc(denseFtvec) ~== 0.41)
    val sparseFtvec = Vectors.sparse(4, Array(0, 2), Array(0.2, 0.4))
    assert(codegenFunc(sparseFtvec) ~== 0.35)
  }

  test("sparse model codegen") {
    val weights = Vectors.sparse(4, Array(1, 3), Array(0.1, 0.5))
    val intercept: Double = 0.35
    def codegenFunc = UdfCodegenerator.codegen(weights, intercept)
    val denseFtvec = Vectors.dense(Array(0.0, 0.4, 0.8, 0.2))
    assert(codegenFunc(denseFtvec) ~== 0.49)
    val sparseFtvec = Vectors.sparse(4, Array(1, 2), Array(0.1, 0.1))
    assert(codegenFunc(sparseFtvec) ~== 0.36)
  }
}
