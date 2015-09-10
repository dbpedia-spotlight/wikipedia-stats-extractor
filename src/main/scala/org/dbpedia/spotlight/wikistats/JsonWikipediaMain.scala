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
import org.apache.spark.{SparkConf, SparkContext}

/*
Entry point to generate JSON formated wikipedia dump
 */

object jsonWikipediaMain {

  def main(args: Array[String]): Unit = {

    //Setting the input parameters
    val inputWikiDump = args(0)
    val lang = args(1)
    val outputPath = args(2)

    val sparkConf = new SparkConf()
      .setAppName("JsonWikiPedia")

    implicit val sc = new SparkContext(sparkConf)

    //Initializing SqlContext for Use in Operating on DataFrames
    implicit val sqlContext = new SQLContext(sc)

    /*
    Parsing Starts from here
     */
    val wikipediaParser = new JsonPediaParser(inputWikiDump,
                                              lang,
                                              true)

    wikipediaParser.pageRDDs.saveAsTextFile(outputPath + "Jsonpedia")
    sc.stop()
  }

}

