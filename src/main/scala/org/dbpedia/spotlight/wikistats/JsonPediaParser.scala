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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.{rdd, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.dbpedia.spotlight.db.memory.MemoryTokenTypeStore
import org.dbpedia.spotlight.db.model.Stemmer
import org.dbpedia.spotlight.db.tokenize.LanguageIndependentStringTokenizer
import org.dbpedia.spotlight.model.TokenType
import org.dbpedia.spotlight.wikistats.wikiformat.XmlInputFormat
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/*
Class to Parse the Raw WikiPedia dump into individual JSON format articles
 */
class JsonPediaParser(lang:String)(implicit val sc: SparkContext,implicit val sqlContext:SQLContext) extends WikiPediaParser{

  /*
    Method to Begin the Parsing Logic
   */
  def parseJSON(pageRDDs:RDD[String]): DataFrame ={

    //Create Initial DataFrame by Parsing using JSONRDD. This is from Spark 1.3 onwards
    sqlContext.jsonRDD(pageRDDs)

  }

  /*
    Method to parse the XML dump into JSON
 */
  def parse(path: String, sc: SparkContext): RDD[String] = {

    val conf = new Configuration()

    //Setting all the Configuration parameters to be used during XML Parsing
    conf.set(XmlInputFormat.START_TAG_KEY, "<page>")
    conf.set(XmlInputFormat.END_TAG_KEY, "</page>")
    conf.set(XmlInputFormat.LANG,lang)

    val rawXmls = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable],
      classOf[Text], conf)

    rawXmls.map(p => p._2.toString)
  }


  /*
     Method to Get the list of Surface forms from the wiki
   */

  def getSfs(dfWikiRDD:DataFrame) : List[String] = {

    dfWikiRDD.select("wid","links.description")
             .rdd
             .map(artRow => (artRow.getList[String](1)))
             .flatMap(sf => sf)
             .collect()
             .toList
  }

  /*
  Method to get the wid and article text from the wiki dump
   */
  //TODO Need to improve the filter condition of blank articles. we can include in the where clause of dataframe
  def getArticleText(dfWikiRDD:DataFrame): RDD[(Long,String)] = {

    dfWikiRDD.select("wid","wikiText")
             .rdd
             .map(artRow => {
                 (artRow.getLong(0),artRow.getString(1))
             })
             .filter(artRow => artRow._2.length > 0)
  }
  /*

  Logic to Create Memory Token Store
   */
  def createTokenTypeStore(tokenTypes:List[TokenType]): MemoryTokenTypeStore =  {

    val tokenTypeStore = new MemoryTokenTypeStore()
    val tokens = new Array[String](tokenTypes.size + 1)
    val counts = new Array[Int](tokenTypes.size + 1)

    tokenTypes.foreach(token => {
      tokens(token.id) = token.tokenType
      counts(token.id) = token.count

    })

    tokenTypeStore.tokenForId  = tokens.array
    tokenTypeStore.counts = counts.array
    tokenTypeStore.loaded()

    return tokenTypeStore
  }

  /*
  Logic for building the memory type tokens
   */
  def getTokens(allSfs:List[String],lang:String): List[TokenType] ={

    //Below Logic is for creating Token Store from the Surface forms
    val stemmer = new Stemmer()
    val locale = new Locale(lang)
    val lst = new LanguageIndependentStringTokenizer(locale, stemmer)
    val token = allSfs.flatMap( sf => lst.tokenizeUnstemmed(sf) ).toSet

    val tokenTypes = new ListBuffer[TokenType]()
    var i= 1
    token.foreach(x =>{
      val token = new TokenType(i,x,0)
      tokenTypes += token
      i += 1

    } )

    tokenTypes.toList
  }


}
