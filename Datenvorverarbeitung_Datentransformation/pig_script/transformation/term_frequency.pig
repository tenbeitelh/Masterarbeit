REGISTER hdfs:///lib/elephantbird/elephant-bird-core-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-pig-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-hadoop-compat-4.10.jar;
REGISTER hdfs:///lib/elephantbird/google-collections-1.0.jar;
REGISTER hdfs:///lib/elephantbird/json-simple-1.1.jar;
REGISTER hdfs:///lib/tenbeitel/CustomPigUDFs-0.0.1-SNAPSHOT.jar;

twitter_files_of_month = LOAD '/user/flume/keyword/tweets/2015/08/02' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

non_empty_tweets = FILTER twitter_files_of_month BY (json#'text' IS NOT NULL);
non_empty_tweets2 = FILTER non_empty_tweets BY SIZE((chararray)json#'text')>0;

de_tweets = FILTER non_empty_tweets2 BY (json#'lang' == 'de');

distinct_de_tweets = DISTINCT de_tweets;

stopwords = FOREACH de_tweets GENERATE de.hs.osnabrueck.tenbeitel.pig.udf.StopWordUDF((chararray)json#'text');
stopword = LIMIT stopwords 2;
DUMP stopword;

stemmings = FOREACH de_tweets GENERATE de.hs.osnabrueck.tenbeitel.pig.udf.StemmingUDF((chararray)json#'text');
stemming = LIMIT stemmings 2;
DUMP stemming;

tokens = FOREACH  de_tweets GENERATE de.hs.osnabrueck.tenbeitel.pig.udf.TokenizationUDF((chararray)json#'text');
token = LIMIT tokens 2;
DUMP token;

tfs = FOREACH  de_tweets GENERATE de.hs.osnabrueck.tenbeitel.pig.udf.TermFrequencyUDF((chararray)json#'text');
tf = LIMIT tfs 2;
DUMP tf;

ntfs = FOREACH  de_tweets GENERATE de.hs.osnabrueck.tenbeitel.pig.udf.TermFrequencyUDF((chararray)json#'text');
ntf = LIMIT ntfs 2;
DUMP ntf;

text = FOREACH de_tweets GENERATE json#'text';
IDF = de.hs.osnabrueck.tenbeitel.pig.udf.TermFrequencyUDF(text);
DUMP IDF;