<<<<<<< HEAD:Datenvorverarbeitung_Datentransformation/pig_script/preprocessing/pig_preprocessing_elephantbird.pig
REGISTER hdfs:///lib/elephantbird/elephant-bird-core-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-pig-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-hadoop-compat-4.10.jar;
REGISTER hdfs:///lib/elephantbird/google-collections-1.0.jar;
REGISTER hdfs:///lib/elephantbird/json-simple-1.1.jar;

twitter_files_of_month = LOAD '$input' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

non_empty_tweets = FILTER twitter_files_of_month BY (json#'text' IS NOT NULL);
non_empty_tweets2 = FILTER non_empty_tweets BY SIZE((chararray)json#'text')>0;

de_tweets = FILTER non_empty_tweets2 BY (json#'lang' == '$lang');

distinct_de_tweets = DISTINCT de_tweets;

STORE distinct_de_tweets INTO '/project/preprocessing/$output_folder_name' USING com.twitter.elephantbird.pig.store.LzoJsonStorage();
=======
REGISTER hdfs:///lib/elephantbird/elephant-bird-core-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-pig-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-hadoop-compat-4.10.jar;
REGISTER hdfs:///lib/elephantbird/google-collections-1.0.jar;
REGISTER hdfs:///lib/elephantbird/json-simple-1.1.jar;

twitter_files_of_month = LOAD '$input' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

non_empty_tweets = FILTER twitter_files_of_month BY (json#'text' IS NOT NULL);
non_empty_tweets2 = FILTER non_empty_tweets BY SIZE((chararray)json#'text')>0;

de_tweets = FILTER non_empty_tweets2 BY (json#'lang' == '$lang');

distinct_de_tweets = DISTINCT de_tweets;

STORE distinct_de_tweets INTO '/project/preprocessing/$output_folder_name' USING com.twitter.elephantbird.pig.store.LzoJsonStorage();


>>>>>>> fdfee6bca5ef6de15c2654e6fc072da1f0681430:Datenvorverarbeitung/pig_script/preprocessing/pig_preprocessing_elephantbird.pig
