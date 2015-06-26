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

import java.util.StringTokenizer

import hivemall.HivemallException

import scala.collection.mutable.ListBuffer

/**
 * Class that represents the features and labels of a data point for Hivemall.
 *
 * @param label Label for this data point.
 * @param features List of features for this data point.
 */
case class HivemallLabeledPoint(label: Double, features: Seq[String]) {
  override def toString: String = {
    "%s,%s".format(label, features.mkString("[", ",", "]"))
  }
}

object HivemallLabeledPoint {

  // Simple parser for HivemallLabeledPoint
  def parse(s: String) = {
    val (label, features) = s.indexOf(',') match {
      case d if d >= 0 => (s.substring(0, d - 1), s.substring(d + 1))
      case _ => throw new HivemallException("Can't parse an input.")
    }
    HivemallLabeledPoint(
      label.toDouble,
      parseTuple(new StringTokenizer(features, "[],", true)))
  }

  private def parseTuple(tokenizer: StringTokenizer): Seq[String] = {
    val items = ListBuffer.empty[String]
    var parsing = true
    var allowComma = false
    while (parsing && tokenizer.hasMoreTokens()) {
      val token = tokenizer.nextToken()
      if (token == "[") {
        items ++= parseTuple(tokenizer)
        parsing = false
        allowComma = true
      } else if (token == ",") {
        if (allowComma) {
          allowComma = false
        } else {
          throw new HivemallException("Found ',' at a wrong position.")
        }
      } else if (token == "]") {
        parsing = false
      } else {
        items.append(token)
        allowComma = true
      }
    }
    if (parsing) {
      throw new HivemallException(s"A tuple must end with ']'.")
    }
    items
  }
}
