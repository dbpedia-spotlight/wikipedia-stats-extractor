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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.dbpedia.spotlight.model.TokenType

/*
WikiPedia Parser trait which can be extended to include any parsers
 */

trait WikiPediaParser {

  //Method to parse the RawWikiPedia dump and return RDD of wiki Article details
  def parse(path: String): RDD[String]

  //Method to get the list of Surface forms from the wikiDump
  def getSfs() : RDD[String]

  //Method to build tokens from the list of Surface forms
  def getTokensInSfs(allSfs: List[String]): List[TokenType]

  //Method to get the wid and article text from dataframe
  def getArticleText(): RDD[(Long, String, List[(String,Long)])]

  //Method to parse XML to JSON and return a Dataframe
  def parseJSON(pageRDDs:RDD[String]): DataFrame

  //Method to compute redirects
  def redirectsWikiArticles(): RDD[(String, String)]

  //Method to resolve redirects
  def getResolveRedirects(): RDD[(String, String)]

  //Method to URI and paragraph text
  def getUriParagraphs(): RDD[(String, String)]
}
