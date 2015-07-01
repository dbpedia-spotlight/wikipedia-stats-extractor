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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.dbpedia.spotlight.wikiformat.XmlInputFormat


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



}
