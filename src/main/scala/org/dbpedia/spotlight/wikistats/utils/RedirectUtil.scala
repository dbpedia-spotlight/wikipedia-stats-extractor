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


class RedirectUtil(map:collection.mutable.Map[String,String]) extends Serializable{

  //Get the resolved Redirect
  def getEndOfChainURI(uri: String): String = getEndOfChainURI(uri, Set())

  private def getEndOfChainURI(uri: String, alreadyTraversed:Set[String]): String = map.get(uri) match {
    case Some(s: String) => if (alreadyTraversed.contains(s)) s else getEndOfChainURI(s, alreadyTraversed + s)
    case None => uri
  }


}
