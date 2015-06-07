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

import org.dbpedia.wiki.format.XmlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WikipediaParser {

  def main(args: Array[String]): Unit ={
    val inputWikiDump = "E:\\ApacheSpark\\enwiki-pages-articles-sample.xml"

    val sc = new SparkContext("local","FirstTestApp","E:\\ApacheSpark\\spark-1.3.1-bin-hadoop2.4\\spark-1.3.1-bin-hadoop2.4\\bin")

    //Read the Wikipedia XML Dump and store each page as XML as element of RDD
    val pageRDDs = readFile(inputWikiDump,sc)







  }
  def readFile(path: String, sc: SparkContext): RDD[String] = {
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, "<page>")
    conf.set(XmlInputFormat.END_TAG_KEY, "</page>")
    val rawXmls = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable],
                                      classOf[Text], conf)
    rawXmls.map(p => p._2.toString)
  }

}
