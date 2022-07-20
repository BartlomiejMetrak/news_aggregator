# news_aggregator
working example of the entire script can be viewed here: https://tuinwestor.pl/newsy

News aggregation repository to parse newspapers with a given list of RSS FEED. 
Articels are analyzed using NLP techniques to detect similar topics, estimate text readability and reading time.
Then, articles are analyzed in terms of the occurrence of word variations of given companies in MySQL DB table.
Based on the text analysis, the algorithm selects the most important topics of the day and groups similar articles concerning the same company.

Used libraries:

1.  mysql.connector
2.  dotenv
3.  nltk
4.  pandas
5.  numpy
6.  sqlalchemy
7.  sklearn
8.  spacy
9.  re
10. time
11. os
12. random
13. scrappy framework

Prior to running the algorithm, the tables must be imported to the MYSQL database.
