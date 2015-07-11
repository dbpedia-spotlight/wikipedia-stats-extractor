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

package scala.org.dbpedia.spotlight.wikistats

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.dbpedia.spotlight.wikistats.JsonPediaParser
import org.scalatest.{Filter, FunSuite}
import org.scalatest.Assertions._

/*
Test Suite for testing the Spark Application funtionality
 */

class TestSuite extends FunSuite with SharedSparkContext{

  test("Testing Empty Surface forms"){
    val inputWikiDump = "enwiki-pages-articles-latest.xml"

    val stopWordLoc = "stopwords.en.list"
    val lang = "en"
    implicit val sqlContext = new SQLContext(sc)

    val wikipediaParser = new JsonPediaParser(inputWikiDump,lang)

    wikipediaParser.getSfs().collect().toList.foreach{sf => assert(sf.isEmpty)}
  }


  override def expectedTestCount(filter:Filter): Int ={
    return 1
  }

}
