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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/*
Entry point for Raw Wiki text
 */

object RawWikiMain {

  def main(args: Array[String]): Unit = {

    //Setting the input parameters
    val inputWikiDump = args(0)
    val lang = args(1)
    val outputPath = args(2)

    val sparkConf = new SparkConf()
      //.setMaster("local[5]")
      .setAppName("RawWikiText")
      //.set("spark.sql.shuffle.partitions","6")

    implicit val sc = new SparkContext(sparkConf)

    //Initializing SqlContext for Use in Operating on DataFrames
    implicit val sqlContext = new SQLContext(sc)

    /*
    Parsing and Processing Starts from here
     */
    val wikipediaParser = new JsonPediaParser(inputWikiDump,lang)

    val rawWikiStats = new RawWikiStats(lang)
    //wikipediaParser.getArticleText1().saveAsTextFile(outputPath + "RawWiki")


    rawWikiStats.buildRawWiki(wikipediaParser).saveAsTextFile(outputPath + "RawWiki")
    //wikipediaParser.getArticleText().saveAsTextFile(outputPath + "RawWiki")
    //wikipediaParser.getArticleText1().collect().foreach(println)

    sc.stop()
  }

}

