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
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.dbpedia.spotlight.db.{FSASpotter, AllOccurrencesFSASpotter}
import org.dbpedia.spotlight.db.model.Stemmer
import org.dbpedia.spotlight.db.tokenize.{LanguageIndependentTokenizer, LanguageIndependentStringTokenizer}
import org.dbpedia.spotlight.wikistats.util.DBpediaUriEncode
import scala.collection.JavaConversions._

/*
Class for computing various like uri, surface form and token Statistics on wikipedia dump
 */

class ComputeStats(lang:String) (implicit val sc: SparkContext,implicit val sqlContext: SQLContext){

  /*
Method to Create Dataframe and parse the WikiIds from the JSON text
*/
  def uriCounts(dfWikiRDD:DataFrame){

    //Print the JSON Schema
    //TODO - This is just for printing the Input JSON Schema. Will be removed at the end
    dfWikiRDD.printSchema()

    //Declaring a local variable to avoid Serializing the whole class
    val language = lang

    //Parse the individual WikiIds and create URI Counts
    val dfSurfaceForms = dfWikiRDD.select("links.id")//.where("")
                        .rdd
                        .map(artRow => artRow.getList[String](0))
                        .flatMap(articleIds => articleIds.map(id=>id))
                        .mapPartitions{ wikiIds =>
                                        val dbpediaEncode = new DBpediaUriEncode(language)
                                        wikiIds.map(wikiId => (dbpediaEncode.uriEncode(wikiId),1))}
                        .reduceByKey(_ + _)



    dfSurfaceForms.foreach(println)
  }

  /*
  Method to get the list of surface forms as an RDD from the FSA Spotter
   */

  def sfSpotter(wikipediaParser:JsonPediaParser,dfWikiRDD:DataFrame): Unit={

    //computeStats.sfCounts(wikipediaParser.getSfs())
    val allSfs = wikipediaParser.getSfs(dfWikiRDD)

    //Broadcasting variable for building FSA
    val sfsBroadcast = sc.broadcast(allSfs)

    //Below Logic is to get Tokens from the list of Surface forms
    val tokens = wikipediaParser.getTokens(allSfs,lang)

    //Broadcasting tokens
    val tokenBroadcast = sc.broadcast(tokens)

    //Get wid and articleText for FSA spotter
    val textIdRDD = wikipediaParser.getArticleText(dfWikiRDD)

    //Declaring a local variable to avoid Serializing the whole class
    val language = lang

    textIdRDD.mapPartitions(textIds => {
              //Implementing the FSA Spotter logic
              val stemmer = new Stemmer()
              val locale = new Locale(language)
              val lst = new LanguageIndependentStringTokenizer(locale, stemmer)
              val token = sfsBroadcast.value.flatMap( sf => lst.tokenizeUnstemmed(sf) ).toSet

              //Initalizing the Memory Token Store
              val tokenTypeStore = wikipediaParser.createTokenTypeStore(tokenBroadcast.value)

              //TODO Change the below logic to implement actual Stop words instead of the sample.
              //Creating a sample StopWords
              val stopWords = Set[String]("a","the","an","that")
              val lit = new LanguageIndependentTokenizer(stopWords,stemmer,locale,tokenTypeStore)
              //Building Dictionary and calling the extract method
              val allOccFSASpotter = new AllOccurrencesFSASpotter(FSASpotter.buildDictionaryFromIterable(sfsBroadcast.value,lit),lit)

              //Extracting the surface forms from FSA Spotter
              textIds.map(textId => {
                      allOccFSASpotter
                      .extract(textId._2)
                      .map(sfOffset => (textId._1,sfOffset._1))
                      .foreach(println)
              })
    })
  }

}
