# Wikipedia Stats Extractor

Tool to extract wikipedia counts/statistics for Name Entity Linking.

## Entity Counts

Extracts counts about how many times a topic URI was linked on the wikipedia corpus.
i.e:

```
DBpedia URI                             Count
--------------------------------------------------------------
http://en.dbpedia.org/resource/1        21
http://en.dbpedia.org/resource/7        7
http://en.dbpedia.org/resource/C        20
```

## Surface Form Counts

Extracts counts about how many times a Surface Form was seen in the corpus, and how many times it was an anchor of an URI

```
Surface form         Count annotated    Count total
--------------------------------------------------------------
Berlin               49068              105915
Berloz               2                  6
9z                   -1                 1
```


## Pair Counts

URIs and Surface forms co-occurrence counts:

```
Surface form     DBpedia URI                                         Count
----------------------------------------------------------------------------
Berlin           http://en.dbpedia.org/resource/Brent_Berlin         2
Berlin           http://en.dbpedia.org/resource/Trams_in_Berlin      9
Berlin           http://en.dbpedia.org/resource/Berlin_(Seedorf)     1
```

## Token Counts

Context Vectors for Entities. Stemmed Tokens.

```
Wikipedia URI                   Stemmed token counts
----------------------------------------------------------------------------
http://en.wikipedia.org/wiki/!  {(renam,76),(intel,14),...,(plai,2),(auf,2)}
<<<<<<< HEAD
```
=======
```
>>>>>>> upstream/master
