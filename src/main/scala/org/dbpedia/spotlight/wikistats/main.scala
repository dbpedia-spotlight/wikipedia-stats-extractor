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
import scala.collection.JavaConversions._

/*
Entry point for Code Execution
 */
object main {

  def main(args: Array[String]): Unit ={

    //TODO - Change the input file
    val inputWikiDump = "E:\\enwiki-pages-articles-latest.xml"

    val stopWordLoc = "E:\\stopwords.en.list"

    val sparkConf = new SparkConf()
                    .setMaster("local[2]")
                    .setAppName("WikiStats")
                    .set("spark.sql.shuffle.partitions","10")

    //TODO - Initialize with Proper Spark Settings
    implicit val sc = new SparkContext(sparkConf)


    //Wikipedia Dump Language
    //TODO - To Change in future to pass the language as input arguments. Defaulting to English for testing
    val lang = "en"

    //Initializing SqlContext for Use in Operating on DataFrames
    implicit val sqlContext = new SQLContext(sc)

    /*
    Parsing and Processing Starts from here
     */
    val wikipediaParser = new JsonPediaParser(inputWikiDump,lang)

    wikipediaParser.getSfs().collect().foreach(println)
    //Logic to calculate various counts
    val computeStats = new ComputeStats(lang)

    //Call FSA Spotter for getting the surface forms from article text
    val sfsSpotter = computeStats.buildCounts(wikipediaParser,stopWordLoc)



  }


}
