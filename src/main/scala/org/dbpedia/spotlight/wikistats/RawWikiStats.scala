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
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.dbpedia.spotlight.db.{AllOccurrencesFSASpotter, FSASpotter}
import org.dbpedia.spotlight.db.model.Stemmer
import org.dbpedia.spotlight.wikistats.utils.{SfDataElement, SpotlightUtils}


class RawWikiStats (lang: String) (implicit val sc: SparkContext,implicit val sqlContext: SQLContext){

  def buildRawWiki(wikipediaParser: JsonPediaParser): Unit = {

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

    //textIdRDD.persist(StorageLevel.MEMORY_AND_DISK)
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
          .map(sfOffset => (textId._1,sfOffset._1,sfOffset._2))

      })
        .flatMap(idSf => idSf)
    })

    /*
    Creating two surface form dataframes for finding the URI for the corresponding Sf
    */

    val totalSfDf = totalSfsRDD.toDF("wid2", "sf2","offset")
    val uriSfDf = wikipediaParser.getSfURI().toDF("wid1", "sf1", "uri")



    import sqlContext.implicits._

    //Joining to obtain the WikiId and the list of all surface forms obtained in the list

    val joinedDf = uriSfDf.join(totalSfDf,(uriSfDf("wid1") === totalSfDf("wid2"))
      && (uriSfDf("sf1") === totalSfDf("sf2")),joinType = "inner")
      .select("wid1","uri","sf1","offset")
      .rdd
      .map(row => (row.getLong(0),List((row.getString(1),row.getString(2),row.getInt(3)))))
      .reduceByKey(SpotlightUtils.addingTuples)
      .toDF("wid1","Sfs")

    val wikiTextDf = textIdRDD.toDF("wid","wikiText")


    joinedDf.join(wikiTextDf,(joinedDf("wid1") === wikiTextDf("wid")))
            .select("wikiText","Sfs")
            .rdd
            //.map(row => (row.getString(0),row.getList[(String, String, Int)](1).toArray.sortBy()))
            .collect().foreach(println)


  }

}
