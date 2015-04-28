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

package org.apache.spark.ml.param

private[ml] trait HasBiasParam extends Params {
  /**
   * bias param for features
   * @group param
   */
  val biasParam: BooleanParam =
    new BooleanParam(this, "biasParam", "Add bias or not")

  /** @group getParam */
  def getBiasParam: Boolean = get(biasParam)
}

private[ml] trait HasDenseParam extends Params {
  /**
   * param for model density
   * @group param
   */
  val denseParam: BooleanParam =
    new BooleanParam(this, "denseParam", "Use dense model or not")

  /** @group getParam */
  def getDenseParam: Boolean = get(denseParam)
}

private[ml] trait HasDimsParam extends Params {
  /**
   * param for feature demensions
   * @group param
   */
  val dimsParam: IntParam =
    new IntParam(this, "dimsParam", "The dimension of model")

  /** @group getParam */
  def getDimsParam: Int = get(dimsParam)
}
