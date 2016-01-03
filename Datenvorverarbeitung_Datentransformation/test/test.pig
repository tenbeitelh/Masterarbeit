REGISTER hdfs:///lib/elephantbird/elephant-bird-core-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-pig-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-hadoop-compat-4.10.jar;
REGISTER hdfs:///lib/elephantbird/google-collections-1.0.jar;
REGISTER hdfs:///lib/elephantbird/json-simple-1.1.jar;

twitter_files_of_month = LOAD '/user/flume/keyword/tweets/2015/12/31' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

non_empty_tweets = FILTER twitter_files_of_month BY (json#'text' IS NOT NULL);
non_empty_tweets2 = FILTER non_empty_tweets BY SIZE((chararray)json#'text')>0;

de_tweets = FILTER non_empty_tweets2 BY (json#'lang' == 'de');

distinct_de_tweets = DISTINCT de_tweets;

features_selected = FOREACH distinct_de_tweets GENERATE json#'id', json#'id_str', json#'in_reply_to_screen_name', json#'in_preply_to_status_id_str', json#'in_reply_to_status_id', json#'in_reply_to_user_id_str', json#'in_reply_to_user_id', json#'created_at', json#'lang', json#'text', json#'entities', json#'user';

clustering_features = FOREACH  features_selected GENERATE $0, $7, REPLACE($9, '\n', ' '); 

STORE clustering_features INTO 'output/test' USING PigStorage(';');