# wikipedia-recommender
A recommender engine that parses Wikipedia user edit logs and uses the ALS algorithm to suggest similar articles to edit. the Wikipedia XML dump is processed using the SAX parser and Hadoop. The prepared data is used by the Alternating Least Squares algorithm (from MLlib package of Apache Spark library) to generate a Rating model to predict similar articles for users to edit.

COMPLEDTED: XML Parsing and Hadoop Map-Reduce job to prepare data from Wikipedia dumps.
To-Do: Implement the ML part.
