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

/*
Class to serialize the contents used for creating language tokenizer
 */
package org.dbpedia.spotlight.wikistats.utils

import java.util.Locale
import java.io.File
import org.dbpedia.spotlight.db.model.Stemmer
import org.dbpedia.spotlight.db.tokenize.LanguageIndependentTokenizer
import collection.mutable
import org.dbpedia.spotlight.model.TokenType


object SpotlightUtils extends Serializable{

  def createLanguageIndependentTokenzier(lang: String,
                                         tokenTypeStore: MemoryTokenTypeStore,
                                         stopWordLoc: String,
                                         stemmer: Stemmer): LanguageIndependentTokenizer ={

    val locale = new Locale(lang)

    val stopWords = Set.empty[String]
    new LanguageIndependentTokenizer(stopWords,
                                     stemmer,
                                     locale,
                                     tokenTypeStore)
  }

  def createStopWordsSet(stopWordLoc:String): Set[String] = {
    scala.io.Source.fromFile(new File(stopWordLoc)).getLines().map(_.trim()).toSet

  }

  /*
 Logic to create the Memory Token Store
  Input:  - List of all Token types
  Output: - Memory Store with the Token information
 */
  def createTokenTypeStore(tokenTypes: List[TokenType]): MemoryTokenTypeStore =  {

    val tokenTypeStore = new MemoryTokenTypeStore()
    val tokens = new Array[String](tokenTypes.size + 1)
    val counts = new Array[Int](tokenTypes.size + 1)

    tokenTypes.map(token => {
      tokens(token.id) = token.tokenType
      counts(token.id) = token.count

    })

    tokenTypeStore.tokenForId  = tokens.array
    tokenTypeStore.counts = counts.array
    tokenTypeStore.loaded()

    tokenTypeStore
  }

  /*
      Logic to concatnate two strings with the space. This function is used the reduceByKey operation
      to contact two paragraphs.
      Input:  - Two Strings to concatenate
      Output: - Concatenated String with Space
  */


  def stringConcat (str1: String, str2: String): String = {

    new mutable.StringBuilder(str1).append(" ").append(str2).toString()

  }

  /*
    Logic to counts the Tokens in a List
    to contact two paragraphs.
    Input:  - List of tokens
    Output: - List of tokens, count
  */

  def countTokens(tokens: List[String]): List[(String,Int)] = {

    val tokenMap = new mutable.HashMap[String, Int]()
    tokens.map( token => {
      if (tokenMap.contains(token)) {
         val value = tokenMap.getOrElse(token,0)
         tokenMap.put(token, value + 1)
      }
      else
         tokenMap.put(token,1)
    })

    tokenMap.toList
  }

  /*
  Below logic is to merge two lists to be used by reduceByKey operation
   */
  def addingTuples(t1: List[(String, String, Int)], t2: List[(String, String, Int)]): List[(String, String, Int)] = {

      t1 ::: t2
  }

}


/*
Case classes for reading from the Dataframe Row
 */
case class span(desc : String, end: Long, id: String, start: Long)

case class artRow(wid: Long, wikiText: String, artType: String, spans: Seq[span])


