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
import org.dbpedia.spotlight.db.model.Stemmer
import org.dbpedia.spotlight.db.tokenize.LanguageIndependentTokenizer
import scala.io.Source
import org.dbpedia.spotlight.model.TokenType

object SpotlightUtils extends Serializable{

  def createLanguageIndependentTokenzier(lang: String,
                                         tokenTypeStore: MemoryTokenTypeStore,
                                         stopWordLoc: String,
                                         stemmer: Stemmer): LanguageIndependentTokenizer ={

    val locale = new Locale(lang)

    val stopWords = createStopWordsSet(stopWordLoc)

    new LanguageIndependentTokenizer(stopWords,
                                     stemmer,
                                     locale,
                                     tokenTypeStore)
  }

  def createStopWordsSet(stopWordLoc:String): Set[String] = {

    Source.fromFile(stopWordLoc).getLines().toSet
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

    println ("Naveen in tokentypes:")

    tokenTypes.map(token => {
      println (token.id)
      tokens(token.id) = token.tokenType
      counts(token.id) = token.count

    })

    tokenTypeStore.tokenForId  = tokens.array
    tokenTypeStore.counts = counts.array
    tokenTypeStore.loaded()

    tokenTypeStore
  }

}


