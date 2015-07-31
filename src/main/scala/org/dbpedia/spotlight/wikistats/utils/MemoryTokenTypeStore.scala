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


package org.dbpedia.spotlight.wikistats.utils

import org.dbpedia.spotlight.db.memory.{MemoryStore, util}
import org.dbpedia.spotlight.db.model.TokenTypeStore
import java.lang.String
import org.dbpedia.spotlight.model.TokenType
import scala.transient
import org.dbpedia.spotlight.log.SpotlightLog
import org.dbpedia.spotlight.db.memory.util.StringToIDMapFactory

/**
 * A memory-based store for
 *
 * @author Joachim Daiber
 */

@SerialVersionUID(1006001)
class MemoryTokenTypeStore
  extends MemoryStore
  with TokenTypeStore
{

  var tokenForId: Array[String] = null
  var counts: Array[Int] = null

  var idFromToken: java.util.Map[String, java.lang.Integer] = null

  @transient
  var totalTokenCount: Double = 0.0

  @transient
  var vocabularySize: Int = 0


  override def loaded() {
    counts.foreach( c =>
      totalTokenCount += c
    )
    vocabularySize = counts.size
    createReverseLookup()
  }

  def size = tokenForId.size

  def createReverseLookup() {
    if (tokenForId != null) {
      SpotlightLog.info(this.getClass, "Creating reverse-lookup for Tokens.")
      idFromToken = StringToIDMapFactory.createDefault(tokenForId.size)

      var id = 0
      tokenForId foreach { token => {
        idFromToken.put(token, id)
        id += 1
      }
      }
    }
  }

  def getTokenType(token: String): TokenType = {

    val id = idFromToken.get(token)

    if (id == null)
      TokenType.UNKNOWN
    else
      new TokenType(id, token, counts(id))
  }

  def getTokenTypeByID(id: Int): TokenType = {
    val token = tokenForId(id)
    val count = counts(id)
    new TokenType(id, token, count)
  }

  def getTotalTokenCount: Double = totalTokenCount

  def getVocabularySize: Int = vocabularySize

}