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


import org.apache.spark.sql.SQLContext
import org.dbpedia.spotlight.wikistats.{ComputeStats, SharedSparkContext, JsonPediaParser}
import org.scalatest.{BeforeAndAfter, FunSuite}


/*
Test Suite for testing the Spark Application funtionality
 */

class TestSuite extends FunSuite with SharedSparkContext with BeforeAndAfter{

  /*
  Declaring common variables used across all the test cases
   */
  var inputWikiDump:String = _
  var stopWordLoc:String = _
  var lang:String = _

  before {
    inputWikiDump = "src/test/resources/enwiki-pages-articles-latest.xml"
    stopWordLoc = "src/test/resources/stopwords.en.list"
    lang = "en"

  }

  //Test case for verifying empty surface forms
  test("Testing Empty Surface forms"){

    implicit val sc = sc_implicit
    implicit val sqlContext = new SQLContext(sc)

    val wikipediaParser = new JsonPediaParser(inputWikiDump,lang)

    wikipediaParser.getSfs().collect().toList.foreach(sf => assert(!sf.isEmpty))

  }

  //Test case for verifying Redirects
  test("Testing Redirects"){
    implicit val sc = sc_implicit
    implicit val sqlContext = new SQLContext(sc)

    val wikipediaParser = new JsonPediaParser(inputWikiDump,lang)

    assert(wikipediaParser.redirectsWikiArticles().map(row => row._1).count()==10)
  }

  //Test case for verifying Redirects
  test("Counting Redirects"){
    implicit val sc = sc_implicit
    implicit val sqlContext = new SQLContext(sc)

    val wikipediaParser = new JsonPediaParser(inputWikiDump,lang)

    assert(wikipediaParser.redirectsWikiArticles().map(row => row._1).count()==10)
  }

  //Test case for resolving redirects
  test("Validating Redirects to Source Article"){

    implicit val sc = sc_implicit
    implicit val sqlContext = new SQLContext(sc)
    inputWikiDump = "src/test/resources/enwiki-pages-redirects.xml"
    val wikipediaParser = new JsonPediaParser(inputWikiDump,lang)

    assert(wikipediaParser.redirectsWikiArticles().map(row => row._1).count()==2)

    //Test for transitive dependencies
    wikipediaParser.getResolveRedirects()
      .map(row=>row._1)
      .collect()
      .toList
      .foreach(redirect => assert(redirect=="Computer_accessibility"))

  }

  //Test case for varifying annotated counts in the Total Surface form counts
  test("Verifying Annotated Counts from the WikiDump"){
    implicit val sc = sc_implicit
    implicit val sqlContext = new SQLContext(sc)

    val wikipediaParser = new JsonPediaParser(inputWikiDump,lang)

    val totalLinks = wikipediaParser.getSfURI().map(row => row._3).collect().toList.size

    val computeStats = new ComputeStats(lang)

    val spotterSfsRDD = computeStats.spotSfsInWiki(wikipediaParser)
    val sfDfs = computeStats.setupJoinDfs(wikipediaParser, spotterSfsRDD)

    var annotatedCounts = 0l
    computeStats.computeTotalSfs(sfDfs._1,sfDfs._2)
      .map(row => row._2)
      .collect()
      .toList
      .foreach(count => {
      if (count == -1) annotatedCounts += 0
      else annotatedCounts +=count
    })

    assert(totalLinks==annotatedCounts)
  }


  //Test case for verifying the spotter sfs
  test("Spotter Sfs"){
    implicit val sc = sc_implicit
    implicit val sqlContext = new SQLContext(sc)

    val wikipediaParser = new JsonPediaParser("src/test/resources/enwiki-pages-anarchism.xml",lang)


    val wikiText = wikipediaParser.getArticleText().map(r => r._2).first().toString

    val computeStats = new ComputeStats(lang)

    val spotterSfsRDD = computeStats.spotSfsInWiki(wikipediaParser).collect()
    spotterSfsRDD.foreach(sf => {
      assert(wikiText.substring(sf._3,sf._3 + sf._2.length) == sf._2)
    })

  }

}
