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


package org.dbpedia.spotlight

import com.sun.xml.internal.bind.v2.TODO
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.dbpedia.util.DBpediaUriEncode
import org.dbpedia.wiki.format.XmlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._


class WikipediaParser(inputWikiDump:String,lang:String)(implicit val sc: SparkContext){

  /*
    Method to Begin the Parsing Logic
   */
  private def startProcess(): Unit ={

    //Read the Wikipedia XML Dump and store each page in JSON format as an element of RDD
    val pageRDDs = readFile(inputWikiDump,sc)

    //Initializing SqlContext for Use in Operating on DataFrames
    val sqlContext = new SQLContext(sc)

    //Create Initial DataFrame by Parsing using JSONRDD. This is from Spark 1.3 onwards
    val dfWikiRDD = sqlContext.jsonRDD(pageRDDs)

    //Method to JsonParsing
    val dfSurfaceForms = parseJson(sqlContext,dfWikiRDD)
  }


  /*
    Method to parse the XML dump into JSON
 */
  private def readFile(path: String, sc: SparkContext): RDD[String] = {

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
  Method to Create Dataframe and parse the WikiIds from the JSON text
 */
  private def parseJson(sqlContext:SQLContext, dfWikiRDD:DataFrame): Unit= {

    //Print the JSON Schema
    //TODO - This is just for printing the Input JSON Schema. Will be removed at the end
    //dfWikiRDD.printSchema()

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

}


object WikipediaParser {

  def main(args: Array[String]): Unit ={

    //TODO - Change the input file
    val inputWikiDump = "E:\\enwiki-pages-articles-latest.xml"

    //TODO - Initialize with Proper Spark Settings
    implicit val sc = new SparkContext("local","FirstTestApp","E:\\ApacheSpark\\spark-1.4.0-bin-hadoop2.6\\bin")

    //Wikipedia Dump Language
    val lang = "en"

    /*
    Parsing Starts from here
     */
    val wikipediaParser = new WikipediaParser(inputWikiDump,lang)
    wikipediaParser.startProcess()
  }





}
