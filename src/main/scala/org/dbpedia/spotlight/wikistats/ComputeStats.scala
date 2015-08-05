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


import java.util.Locale

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.dbpedia.spotlight.db.model.Stemmer
import org.dbpedia.spotlight.db.{FSASpotter, AllOccurrencesFSASpotter}
import org.dbpedia.spotlight.wikistats.util.DBpediaUriEncode
import org.dbpedia.spotlight.wikistats.utils.SpotlightUtils
import org.dbpedia.spotlight.db.tokenize.LanguageIndependentStringTokenizer
import org.dbpedia.spotlight.db.stem.SnowballStemmer

/*
Class for computing various like uri, surface form and token Statistics on wikipedia dump
 */

class ComputeStats(lang: String) (implicit val sc: SparkContext,implicit val sqlContext: SQLContext){


  /*
  Encapsulated Method to get the list of surface forms as an RDD from the FSA Spotter
   */

  def buildCounts(wikipediaParser: JsonPediaParser): (DataFrame, DataFrame)={

    val allSfs = wikipediaParser.getSfs().collect().toList

    //Below Logic is to get Tokens from the list of Surface forms
    val tokens = wikipediaParser.getTokensInSfs(allSfs)

    //Creating MemoryTokenTypeStore from the list of Tokens
    val tokenTypeStore = SpotlightUtils.createTokenTypeStore(tokens)

    //Broadcast TokenTypeStore for creating tokenizer inside MapPartitions
    val tokenTypeStoreBc = sc.broadcast(tokenTypeStore)

    val stemmer = new Stemmer()
    val lit = SpotlightUtils.createLanguageIndependentTokenzier(lang,
      tokenTypeStore,
      " ",
      stemmer)

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

      val stemmer = new Stemmer()
      val allOccFSASpotter = new AllOccurrencesFSASpotter(fsaDictBc.value,
        SpotlightUtils.createLanguageIndependentTokenzier(language,
          tokenTypeStoreBc.value,
          " ",
          stemmer))

      textIds.map(textId => {
        allOccFSASpotter.extract(textId._2)
          .map(sfOffset => (textId._1,sfOffset._1))

      })
        .flatMap(idSf => idSf)
    })

    /*
    Creating two surface form dataframes for computing various counts
     */

    val totalSfDf = totalSfsRDD.toDF("wid", "sf2")
    val uriSfDf = wikipediaParser.getSfURI().toDF("wid", "sf1", "uri")

    (totalSfDf.persist(StorageLevel.MEMORY_AND_DISK),uriSfDf.persist(StorageLevel.MEMORY_AND_DISK))
  }


  /*
  Method to join the surface form dataframes for URI and Pari counts
  Input:  - WikiParser, Sf dataframe from Spotter, Sf from wikidump
  Output: - Joined Dataframe
 */

  def joinSfDF(wikipediaParser:JsonPediaParser,
               totalSfDf:DataFrame,
               uriSfDf:DataFrame): DataFrame = {

    import sqlContext.implicits._

    //Joining the two Surface form Datasets which would be used for Counts downstream
    uriSfDf.join(totalSfDf,(uriSfDf("wid") === totalSfDf("wid"))
      && (uriSfDf("sf1") === totalSfDf("sf2")),"left_outer")
      .select("uri","sf1")
      .unionAll(wikipediaParser.getResolveRedirects.toDF("uri","sf1"))

  }

  /*
    Method to compute URI Counts on the WikiDump
    Input:  - Dataframe with the Uri and Surface form information
    Output: - RDD with the Uris and count
   */

  def computeUriCounts(joinedDf: DataFrame): RDD[(String, Long)] = {

    //Local variable to avoid serializing the whole object
    val language = lang

    joinedDf
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
  }

  /*
    Method to compute Pair Counts on the WikiDump
    Input:  - Dataframe with the Uri and Surface form information
    Output: - RDD with the Uris and Surface form counts
   */

  def computePairCounts(joinedDf: DataFrame): RDD[(String, String, Long)] =  {

    //Local variable to avoid serializing the whole object
    val language = lang

    joinedDf
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
  }

  /*
    Method to compute total Surface form Counts on the WikiDump
    Input:  - Dataframe with the Uri and Surface form information
    Output: - RDD with the surface forms, annotated counts and total counts
   */

  def computeTotalSfs(totalSfDf: DataFrame, uriSfDf:DataFrame): RDD[(String,Long,Any)] = {

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

    //Sql Joining for finding the fake lowercase sfs

    val lowerCaseSf = sfCountsAnnotated.map{row => if (!(row._1 == row._1.toLowerCase))
      (row._1.toLowerCase,-1l,1) else (None,None,None)}
      .filter(row => !(row._1 == None))
      .map(row => (row._1.toString,row._2.toString.toLong,row._3))

    sfCountsAnnotated.union(lowerCaseSf)


  }

  /*
    Method to compute total Surface form Counts on the WikiDump
    Input:  - RDD with uri and the paragraphs where it is annotated
    Output: - RDD with uri and Tokens
   */

  def computeTokenCounts(uriParaText: RDD[(String,String)],
                         stopWordLoc: String,
                         stemmerString: String): RDD[(String,String)] = {

    import sqlContext.implicits._

    val language = lang
    uriParaText.mapPartitions(part => {

      val snowballStemmer = new SnowballStemmer(stemmerString)
      val locale = new Locale(language)
      val list = new LanguageIndependentStringTokenizer(locale,snowballStemmer)
      val dbpediaEncode = new DBpediaUriEncode(language)
      val stemStopWords = collection.mutable.Set[String]()
      SpotlightUtils.createStopWordsSet(stopWordLoc).foreach(word =>
        list.tokenize(word).foreach(stemWord => stemStopWords += stemWord))

      part.map(row => (dbpediaEncode.wikiUriEncode(row._1),SpotlightUtils.countTokens(list.tokenize(row._2).filter(!stemStopWords.contains(_)).toList).mkString(",")))

    })

  }

}
