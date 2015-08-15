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

import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.dbpedia.spotlight.db.{AllOccurrencesFSASpotter, FSASpotter}
import org.dbpedia.spotlight.db.model.Stemmer
import org.dbpedia.spotlight.wikistats.util.DBpediaUriEncode
import org.dbpedia.spotlight.wikistats.utils.SpotlightUtils
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

/*
Class to replace the Surface forms with the dbpedia Uri in the article text

 */

class RawWikiStats (lang: String) (implicit val sc: SparkContext,implicit val sqlContext: SQLContext){

  def buildRawWiki(wikipediaParser: JsonPediaParser): RDD[String] = {

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

    //Constructing Redirects Broadcast HashMap
    val redirectsRdd = wikipediaParser.constructResolvedRedirects()

    var redirectsMap = sc.accumulableCollection(HashMap[String, String]())
    redirectsRdd.foreach(row => {redirectsMap += row._1.toLowerCase -> row._2})

    val redirectsMapBc = sc.broadcast(redirectsMap.value)
    //Get wid and articleText for FSA spotter

    val textIdRDD = wikipediaParser.getArticleText()

    //Implementing the FSA Spotter logic

    //Declaring value for avoiding the whole class to be serialized
    val language = lang

    //Logic to get the Surface Forms from FSA Spotter
    val spotterSfsRDD = textIdRDD.mapPartitions(textIds => {

      val stemmer = new Stemmer()
      val allOccFSASpotter = new AllOccurrencesFSASpotter(fsaDictBc.value,
        SpotlightUtils.createLanguageIndependentTokenzier(language,
          tokenTypeStoreBc.value,
          " ",
          stemmer))

      val dbpediaEncode = new DBpediaUriEncode(language)

      textIds.map(textId => {

        val sfMap = textId._3.map(s => {
          (s._2 -> s._3)
        }).toMap

        val spots = textId._3.map(s => s._1).toList

        val spotterSfs = allOccFSASpotter.extract(textId._2, spots)
                                         .filter(sf => sfMap.contains(sf._1))
                                         .map(sf => (sf._1, sf._2, sfMap(sf._1)))

        //Storing the article text in a String Builder for replacing the sfs with dbpedia entities
        val changedArticleText = new StringBuilder(textId._2)
        var changeOffset = 0

        //Going through all the Sfs and replacing in the raw text

        spotterSfs.foreach(sf => {
          val redirectLink = if (redirectsMapBc.value.contains(sf._3.toLowerCase))
                                 redirectsMapBc.value(sf._3.toLowerCase).toString
                             else sf._3.toString

          val linkToReplace = " " + dbpediaEncode.uriEncode(redirectLink) + " "
          changedArticleText.replace(sf._2 + changeOffset,sf._2 + sf._1.length + changeOffset, linkToReplace)
          changeOffset += linkToReplace.length - sf._1.length
        })
        changedArticleText.toString()
      })
    })

    spotterSfsRDD
  }
}
