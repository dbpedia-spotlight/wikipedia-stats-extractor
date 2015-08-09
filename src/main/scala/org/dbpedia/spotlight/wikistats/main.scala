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

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/*
Entry point for Code Execution
 */

object main {

  def main(args: Array[String]): Unit ={

    //Setting the input parameters
    val inputWikiDump = args(0)
    val stopWordLoc = args(1)
    val lang = args(2)
    val outputPath = args(3)
    val stemmerString = args(4)

    val sparkConf = new SparkConf()
                    //.setMaster("local[5]")
                    .setAppName("WikiStats")
                    //.set("spark.sql.shuffle.partitions","6")

    implicit val sc = new SparkContext(sparkConf)

    //Initializing SqlContext for Use in Operating on DataFrames
    implicit val sqlContext = new SQLContext(sc)

    /*
    Parsing and Processing Starts from here
     */
    val wikipediaParser = new JsonPediaParser(inputWikiDump,lang)

    //Logic to calculate various counts
    val computeStats = new ComputeStats(lang)

    //Logic to build surface Form dataframes to be used for wiki stats counts
    val spotterSfsRDD = computeStats.spotSfsInWiki(wikipediaParser)

    val sfDfs = computeStats.setupJoinDfs(wikipediaParser, spotterSfsRDD)

    val joinedDf = computeStats.joinSfDF(wikipediaParser,sfDfs._1,sfDfs._2)//.repartition(6)

    val joinedDfPersist = joinedDf.persist(StorageLevel.MEMORY_AND_DISK)

    //Uri Counts
    computeStats.computeUriCounts(joinedDfPersist).saveAsTextFile(outputPath + "UriCounts")

    //Pair Counts
    computeStats.computePairCounts(joinedDfPersist).saveAsTextFile(outputPath + "PairCounts")

    //Total Surface Form counts
    computeStats.computeTotalSfs(sfDfs._1, sfDfs._2).saveAsTextFile(outputPath + "TotalSfCounts")

    //Token Counts
    computeStats.computeTokenCounts(wikipediaParser.getUriParagraphs(),stopWordLoc,stemmerString)
      .saveAsTextFile(outputPath + "TokenCounts")

    sc.stop()


  }


}
