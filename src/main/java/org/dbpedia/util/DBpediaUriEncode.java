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


package org.dbpedia.util;

import org.dbpedia.spotlight.util.WikiUtil;

import java.io.Serializable;
import java.util.regex.Pattern;
/*
Class used to Encode wiki Uri
 */
public class DBpediaUriEncode {

    private Pattern percentEncodedPattern = Pattern.compile("%\\d\\d");

    private String wikipediaUrlPrefix;
    private String dbpediaUriPrefix;
    private String dbpediaCanonicalUriPrefix;

    /*
    Creating Wiki and DBPedia URLs
     */
    public DBpediaUriEncode(String lang){

        this.wikipediaUrlPrefix = "http://" + lang + ".wikipedia.org/wiki/";
        this.dbpediaUriPrefix = "http://" + lang + ".dbpedia.org/resource/";
        this.dbpediaCanonicalUriPrefix = "http://.dbpedia.org/resource/";
    }

    /*
    Method to Check the WikiID and create the corresponding DBpedia url
     */
    public String uriEncode(String id){

        id = id.replace(this.wikipediaUrlPrefix, "");
        id = id.replace(this.dbpediaUriPrefix, "");
        id = id.replace(this.dbpediaCanonicalUriPrefix, "");
/*
        if (percentEncodedPattern.matcher(id).find()) {
            id = WikiUtil.wikiDecode(id);
        }
*/
        String encoded = WikiUtil.wikiEncode(id);
        return this.dbpediaUriPrefix + encoded;
    }
}
