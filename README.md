# wikipedia-article-recommender

This is a hobby project, a spin-off idea from my Master thesis. The aim is to look at user edits of Wikipedia and recommend artciles to users that they are most likely to edit. People who edit certain articles on Wikipedia are likely to have more knowledge on certain domains or certain artcles (hence presumably their decision to edit those articles). We could use this information then recommend them similar or related articles to edit.

The two major steps in this project are first, parsing the Wikipedia user history dumps (~90GBs) and then experiment with recommendation algorithms like the Alternating Least Squares (preferrably With SparkMlLib) to generate recommendations



## Status

COMPLEDTED: XML Parsing and Hadoop Map-Reduce job to prepare training data from Wikipedia dumps.
PENDING: Implement the ML part.

I haven't had the time to work on this project hopefully i finish it someday (soon!)
