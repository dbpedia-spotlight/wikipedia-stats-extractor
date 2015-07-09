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


import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.dbpedia.spotlight.db.{FSASpotter, AllOccurrencesFSASpotter}
import org.dbpedia.spotlight.wikistats.util.DBpediaUriEncode
import org.dbpedia.spotlight.wikistats.utils.SpotlightUtils
import scala.collection.JavaConversions._

/*
Class for computing various like uri, surface form and token Statistics on wikipedia dump
 */

class ComputeStats(lang:String) (implicit val sc: SparkContext,implicit val sqlContext: SQLContext){


  /*
  Method to get the list of surface forms as an RDD from the FSA Spotter
   */

  def buildCounts(wikipediaParser:JsonPediaParser): Unit={


    val allSfs = wikipediaParser.getSfs().collect().toList

    //Below Logic is to get Tokens from the list of Surface forms
    val tokens = wikipediaParser.getTokensInSfs()

    //Creating MemoryTokenTypeStore from the list of Tokens
    val tokenTypeStore = wikipediaParser.createTokenTypeStore(tokens)

    //Broadcast TokenTypeStore for creating tokenizer inside MapPartitions
    val tokenTypeStoreBc = sc.broadcast(tokenTypeStore)

    val lit = SpotlightUtils.createLanguageIndependentTokenzier(lang,tokenTypeStore)

    //Creating dictionary broadcast
    val fsaDict = FSASpotter.buildDictionaryFromIterable(allSfs,lit)
    val fsaDictBc = sc.broadcast(fsaDict)


    //Get wid and articleText for FSA spotter
    val textIdRDD = wikipediaParser.getArticleText()

    //textIdRDD.foreach(println)
    //Implementing the FSA Spotter logic

    //Declaring value for avoiding the whole class to be serialized
    val language = lang

    import sqlContext.implicits._
    //Logic to get the Surface Forms from FSA Spotter
    val totalSfsRDD = textIdRDD.mapPartitions(textIds => {
              textIds.map(textId => {
                      val allOccFSASpotter = new AllOccurrencesFSASpotter(fsaDictBc.value,
                        SpotlightUtils.createLanguageIndependentTokenzier(language,tokenTypeStoreBc.value))
                      allOccFSASpotter.extract(textId._2)
                                      .map(sfOffset => (textId._1,sfOffset._1))

              })
              .flatMap(idSf => idSf)
    })

    /*
    Creating two Dataframes for computing various counts
     */

    val totalSfDf = totalSfsRDD.toDF("wid", "sf2")
    val uriSfDf = wikipediaParser.getSfURI().toDF("wid", "sf1", "uri")

    counts(uriSfDf,totalSfDf)
  }

  /*
  Method to Compute various counts on the WikiDump
   */
  def counts(uriSfDf:DataFrame, totalSfDf:DataFrame) = {

    //Joining two Datasets
    val joinedDf = uriSfDf.join(totalSfDf,(uriSfDf("wid") === totalSfDf("wid"))
                                && (uriSfDf("sf1") === totalSfDf("sf2")),"left_outer")

    val language = lang
    //Get the URI Counts

    val uriCounts = joinedDf
                    .groupBy("uri")
                    .count
                    .rdd
                    .map(row => {
                        (row.getString(0),row.getLong(1))
                    })
                    .mapPartitions(uris => {
                                  val dbpediaEncode = new DBpediaUriEncode(language)
                                  uris.map(uri => (dbpediaEncode.uriEncode(uri._1),uri._2))

                    })
                    //.collect().foreach(println)

    //Pair Counts
    val pairCounts  = joinedDf
                      .groupBy("sf1","uri")
                      .count
                      .rdd
                      .map(row => {
                          (row.getString(0),row.getString(1),row.getLong(2))
                      })
                      .mapPartitions(urisfs => {
                                    val dbpediaEncode = new DBpediaUriEncode(language)
                                    urisfs.map(urisf => (urisf._1,dbpediaEncode.uriEncode(urisf._2),urisf._3))

                      })
                      //.collect().foreach(println)

    //Surface Form Counts Logic
    val sfAnnotatedCounts = uriSfDf
                            .groupBy("sf1")
                            .count

    val sfSpotterCounts = totalSfDf
                          .groupBy("sf2")
                          .count


    //Surface Form Counts
    val sfCountsDf = sfAnnotatedCounts
                     .join(sfSpotterCounts,sfAnnotatedCounts("sf1") === sfSpotterCounts("sf2"),"left_outer")

    val sfCountsAnnotated  = sfCountsDf
                             .rdd
                             .map(row => (row.getString(0),row.getLong(1),row.get(3)))
                             .map(row => if(row._3 == null) (row._1,row._2,1)
                                         else row)
                   //.collect().foreach(println)

    import sqlContext.implicits._
    //Sql Joining for finding the fake lowercase sfs

    val sfTable = sfCountsDf.registerTempTable("SFTable")

    val lowerCaseSf = sfCountsAnnotated.map(row => (row._1.toLowerCase,-1l,1))
                      .toDF("sf","an","cn")
                      .registerTempTable("SfLower")

    val lowerCaseSfJoin = sqlContext.sql("Select distinct sf,an,cn from SfLower, SFTable where sf1 <> sf")
                          .rdd
                          .map(row => (row.getString(0),row.getLong(1),row.get(2)))

    //Total Surface Form Counts

    val sfCounts = sfCountsAnnotated.union(lowerCaseSfJoin).collect().foreach(println)


  }
}
