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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.dbpedia.spotlight.db.{AllOccurrencesFSASpotter, FSASpotter}
import org.dbpedia.spotlight.db.model.Stemmer
import org.dbpedia.spotlight.model._
import org.dbpedia.spotlight.wikistats.util.DBpediaUriEncode
import org.dbpedia.spotlight.wikistats.utils.SpotlightUtils
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

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

        var spots = ListBuffer[SurfaceFormOccurrence]()

        var sfMap = Map.empty[String, String]
        textId._3.foreach(s => {

          //Building the real Surface forms of the wiki article
          val spotToAdd = new SurfaceFormOccurrence(new SurfaceForm(s._1),new Text(textId._2),s._2.toInt,Provenance.Annotation, -1)
          spotToAdd.setFeature(new Nominal("spot_type", "real"))
          spots += spotToAdd
          sfMap += (s._1 -> s._3)
        })

        //Creating a list of sfs to be used for replacing the sf with the DBPedia entities
        var spotterSfs = Seq[(String, Int, String)]()
        allOccFSASpotter.extract(textId._2,spots.toList)
          .foreach(sfOffset => {
          spotterSfs = spotterSfs :+ (sfOffset._1,sfOffset._2,(if (sfMap.contains(sfOffset._1)) sfMap.get(sfOffset._1).get else sfOffset._1))
        })

        //Storing the article text in a String Builder for replacing the sfs with dbpedia entities
        val artText = new StringBuilder(textId._2)
        var changeOffset = 0

        //Going through all the Sfs and replacing in the raw text
        spotterSfs.map(sf => {
          val linkToReplace = dbpediaEncode.wikiUriEncode(sf._3)
          artText.replace(sf._2 + changeOffset,sf._2 + sf._1.length + changeOffset, linkToReplace)
          changeOffset += linkToReplace.length - sf._1.length
        })

        artText.toString()
      })
    })

    spotterSfsRDD
  }
}
