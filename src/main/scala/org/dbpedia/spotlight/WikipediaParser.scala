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
import org.dbpedia.wiki.format.XmlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer

object WikipediaParser {

  def main(args: Array[String]): Unit ={

    //TODO - Change the input file
    val inputWikiDump = "E:\\My_Masters_Data_Science\\Google Summer of Code 2015\\enwiki-20090902-pages-articles-sample.xml"

    //TODO - Initialize with Proper Spark Settings
    val sc = new SparkContext("local","FirstTestApp","E:\\ApacheSpark\\spark-1.4.0-bin-hadoop2.6\\bin")

    //Read the Wikipedia XML Dump and store each page in JSON format as an element of RDD
    val pageRDDs = readFile(inputWikiDump,sc)

    //Initializing SqlContext for Use in Operating on DataFrames
    val sqlContext = new SQLContext(sc)

    //Create Initial DataFrame by Parsing the JSONRDD
    val dfWikiRDD = sqlContext.jsonRDD(pageRDDs)

    //Method to JsonParsing
    val dfSurfaceForms = parseJson(sqlContext,dfWikiRDD)

  }

  /*
  Function to parse the XML dump into JSON
   */
  def readFile(path: String, sc: SparkContext): RDD[String] = {
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, "<page>")
    conf.set(XmlInputFormat.END_TAG_KEY, "</page>")
    val rawXmls = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable],
                                      classOf[Text], conf)
    rawXmls.map(p => p._2.toString)
  }

  /*

   */
  def parseJson(sqlContext:SQLContext, dfWikiRDD:DataFrame): Unit= {

    //Print the JSON Schema
    dfWikiRDD.printSchema()

    //Parse the individual Anchors
    val dfSurfaceForms = dfWikiRDD.select("links.description")
                         .map(artRow => artRow.getSeq[Row](0))
                         .map(row => row.toList)
                         .flatMap(sf => sf)

    dfSurfaceForms.foreach(println)
    println(dfSurfaceForms)
  }

  def sfParse(row:Row): String ={
      row.toString()
  }
}
