/**
 *  Copyright 2015 DBpedia Spotlight
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.dbpedia.spotlight.wikistats

import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfter, FunSuite}


/*
Test Suite for testing the Spark Application funtionality
 */

class WikiStatsTestSuite extends FunSuite with SharedSparkContext with BeforeAndAfter{

  /*
  Declaring common variables used across all the test cases
   */
  var uriCountsInput:String = _
  var pairCountsInput:String = _
  var totalCountsInput:String = _
  var tokenCountsInput:String = _

  before {
    uriCountsInput = "E:\\Counts_Testing\\UriCounts"
    pairCountsInput = "E:\\Counts_Testing\\PairCounts"
    totalCountsInput = "E:\\Counts_Testing\\TotalSfCounts"
    tokenCountsInput = "E:\\Counts_Testing\\TokenCounts"
  }


  test("Verify total records"){
    implicit val sc = sc_implicit
    implicit val sqlContext = new SQLContext(sc)

    val inputRDD = sc.textFile(uriCountsInput)

    val line = inputRDD.foreach{row => val rowSplit = row.split("\t")
                                   assert(rowSplit.length == 2)}

  }

  test("Verify total records"){
    implicit val sc = sc_implicit
    implicit val sqlContext = new SQLContext(sc)

    val inputRDD = sc.textFile(uriCountsInput)

    val line = inputRDD.map{row => val rowSplit = row.split("\t")
      assert(rowSplit.length == 2)}

  }


}
