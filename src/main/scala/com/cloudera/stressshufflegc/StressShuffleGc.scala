/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.stressshufflegc

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.util.Random

case class SomeObject(num1: Int, num2: Double, num3: Long)

object StressShuffleGc {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Stress Shuffle GC"))

    val numMappers = args(0).toInt
    val numUniqueKeys = args(1).toInt
    val recordsPerMapper = args(2).toInt / numMappers
    val numReducers = args(3).toInt

    val seeds = (0 until numMappers).toArray
    val seedsRdd = sc.parallelize(seeds, numMappers)

    val pairRdd = seedsRdd.flatMap(generateRecords(_, numUniqueKeys, recordsPerMapper))

    pairRdd.groupByKey(numReducers).count()
  }

  def generateRecords(seed: Int, numUniqueKeys: Int, numRecords: Int):
      TraversableOnce[(Integer, SomeObject)] = {
    val rand = new Random(seed)
    for (i <- 0 until numRecords) yield {
      val randObject = SomeObject(rand.nextInt(), rand.nextDouble(), rand.nextLong())
      val randKey = rand.nextInt(numUniqueKeys)
      (new Integer(randKey), randObject)
    }
  }
}
