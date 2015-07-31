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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.dbpedia.spotlight.db.memory.MemoryTokenTypeStore
import org.dbpedia.spotlight.db.model.Stemmer
import org.dbpedia.spotlight.db.tokenize.LanguageIndependentStringTokenizer
import org.dbpedia.spotlight.model.TokenType
import org.dbpedia.spotlight.wikistats.util.DBpediaUriEncode
import org.dbpedia.spotlight.wikistats.utils.RedirectUtil
import org.dbpedia.spotlight.wikistats.wikiformat.XmlInputFormat
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

/*
Class to Parse the Raw WikiPedia dump into individual JSON format articles
Member variables -  1. Input Wikipedia Dump Path
                    2. Language of the Wikipedia dump
 */
class JsonPediaParser(inputWikiDump: String, lang: String)
                     (implicit val sc: SparkContext,implicit val sqlContext: SQLContext)
  extends WikiPediaParser{


  val pageRDDs = parse(inputWikiDump).persist(StorageLevel.MEMORY_AND_DISK)
  val dfWikiRDD = parseJSON(pageRDDs)

  /*
    Method to Begin the Parsing Logic
    Input:  - RDD of Individual article in JSON format
    Output: - Dataframe of the input RDD
   */
  def parseJSON(pageRDDs: RDD[String]): DataFrame ={

    //Create Initial DataFrame by Parsing using JSONRDD. This is from Spark 1.3 onwards
    sqlContext.read.json(pageRDDs)

  }

  /*
    Method to parse the XML dump into JSON
    Input:  - Path of the Wikipedia dump
    Output: - RDD of Individual article in JSON format
 */
  def parse(path: String): RDD[String] = {

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
    Get Redirects from the wiki dump
    Input:   - Base dataframe consiting of wiki data
    Output:  - RDD with Redirect source and target
   */
  def redirectsWikiArticles(): RDD[(String, String)] = {

    dfWikiRDD.select("wikiTitle","type","redirect")
      .rdd
      .filter(row => row.getString(1)== "REDIRECT")
      .map(row => (row.getString(0),row.getString(2)))

  }

  /*
    Method to resolve transitive dependencies for the redirects.
    Input:  - None
    Output: - RDD with the resolved wiki uri and surface form
   */
  def getResolveRedirects(): RDD[(String, String)] = {

    val rddRedirects = redirectsWikiArticles()
    var linkMap = sc.accumulableCollection(HashMap[String,String]())

    rddRedirects.foreach(row => {linkMap += (row._1 -> row._2)})

    val mapBc = sc.broadcast(linkMap.value)


    rddRedirects.mapPartitions(rows => {
      val redirectUtil = new RedirectUtil(mapBc.value)
      rows.map { row => (redirectUtil.getEndOfChainURI(row._1),row._1)
      }})

  }

  /*
     Method to Get the list of Surface forms from the wiki
    Input:  - None
    Output: - RDD of all Surface forms from the wikipedia dump
   */
  def getSfs() : RDD[String] = {

    dfWikiRDD.select("wid","links.description","type")
      .rdd
      .filter(row => row.getString(2)== "ARTICLE")
      .map(artRow => artRow.getList[String](1))
      .flatMap(sf => sf)
  }


  /*
  Method to get the wid and article text from the wiki dump
    Input:  - None
    Output: - RDD of all wikiId and the article text
   */
  def getArticleText(): RDD[(Long, String)] = {

    dfWikiRDD.select("wid","wikiText","type")
      .rdd
      .filter(row => row.getString(2)== "ARTICLE")
      .map(artRow => {
      (artRow.getLong(0),artRow.getString(1))
    })
      .filter(artRow => artRow._2.length > 0)
  }

  /*
   Logic to Get Surface Forms and URIs from the wikiDump
    Input:  - None
    Output: - RDD of wiki-id, surface forms and uri
   */
  def getSfURI(): RDD[(Long,String, String)]= {

    dfWikiRDD.select(new Column("wid"),new Column("type"),explode( new Column("links")).as("link"))
      .select("type","wid","link.description","link.end","link.id","link.start")
      .rdd
      .filter(row => row.getString(0)== "ARTICLE")
      .filter(row => !(row.getLong(3)==0 && row.getLong(5)==0))
      .map(row => (row.getLong(1),row.getString(2),row.getString(4)))

  }

  /*
   Logic to get Links and paragraph text as RDD
    Input:  - None
    Output: - RDD with the paragraph links and the text
   */
  def getUriParagraphs(): RDD[(String,String)] = {

    import org.apache.spark.sql.functions._

    dfWikiRDD.select(explode( new Column("paragraphsLink")).as("paraLink"))
      .select(explode(new Column("paraLink.links.id")).as("id"),new Column("paraLink.paraText"))
      .map(row => (row.getString(0),row.getString(1)))

  }

  def getRawWikiText(): Unit ={

    val language = lang
    dfWikiRDD.select(explode( new Column("paragraphsLink")).as("paraLink"),new Column ("wid"),new Column("type"))
    .select("type","wid","paraLink.links","paraLink.paraText")
    .rdd
    .filter(row => row.getString(0)== "ARTICLE")
    .map(row => (row.getLong(1),row.getList[String](2),row.getString(3)))
    .map(row => {

      val dbpediaEncode = new DBpediaUriEncode(language)
      row._2.map(sf => {
        val sfDet = sf.split(",")
        //row._3.

      })
    })

  }

  /*
   Logic to get the list of all the tokens in the Surface forms
    Input:  - None
    Output: - List of different token types
   */

  def getTokensInSfs(): List[TokenType] ={

    val stemmer = new Stemmer()
    val locale = new Locale(lang)
    val lst = new LanguageIndependentStringTokenizer(locale, stemmer)
    //Below Logic is for creating Token Store from the Surface forms
    //TODO Getting Searlization error Hence Using Collect and ToList. May need to change in Future
    val token = getSfs().collect().toList.flatMap( sf => lst.tokenizeUnstemmed(sf) )


    val tokenTypes=token.zip(Stream from 1).map{case (x,i) => new TokenType(i,x,0)}

    //tokenTypes.foreach(x => println (x.toString()))
    //tokenTypes.foreach(x => println (x.id,x.tokenType,x.count))
    println("Printing Tokens")
    println(tokenTypes.size)

    tokenTypes
  }


}