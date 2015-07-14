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

import org.dbpedia.spotlight.db.memory.MemoryTokenTypeStore
import org.dbpedia.spotlight.db.model.Stemmer
import org.dbpedia.spotlight.db.tokenize.LanguageIndependentTokenizer
import scala.io.Source


object SpotlightUtils extends Serializable{

  def createLanguageIndependentTokenzier(lang:String,
                                         tokenTypeStore:MemoryTokenTypeStore,
                                         stopWordLoc:String): LanguageIndependentTokenizer ={
    val stemmer = new Stemmer()
    val locale = new Locale(lang)

    val stopWords = createStopWordsSet(stopWordLoc)

    new LanguageIndependentTokenizer(stopWords,
                                     stemmer,
                                     locale,
                                     tokenTypeStore)
  }

  def createStopWordsSet(stopWordLoc:String): Set[String] = {

    val stopWords = Source.fromFile(stopWordLoc).getLines().toSet
    stopWords
  }
}


