
REGISTER hdfs:///lib/elephantbird/elephant-bird-core-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-pig-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-hadoop-compat-4.10.jar;
REGISTER hdfs:///lib/elephantbird/google-collections-1.0.jar;
REGISTER hdfs:///lib/elephantbird/json-simple-1.1.jar;


twitter_files_of_month = LOAD '$input' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

non_empty_tweets = FILTER twitter_files_of_month BY (json#'text' IS NOT NULL);
non_empty_tweets2 = FILTER non_empty_tweets BY SIZE((chararray)json#'text')>0;

de_tweets = FILTER non_empty_tweets2 BY (json#'lang' == 'de');

distinct_de_tweets = DISTINCT de_tweets;

features_selected = FOREACH distinct_de_tweets GENERATE json#'id', json#'id_str', json#'in_reply_to_screen_name', json#'in_preply_to_status_id_str', json#'in_reply_to_status_id', json#'in_reply_to_user_id_str', json#'in_reply_to_user_id', json#'created_at', json#'lang', json#'text', json#'entities', json#'user';

initial_informationflow = FOREACH features_selected GENERATE $0, $4;
initial_informationflow_filtered = FILTER initial_informationflow BY ($1 IS NOT NULL);

replace_urls = FOREACH  features_selected GENERATE $0, REPLACE($9, '(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]', ''); 

clustering_features = FOREACH  replace_urls GENERATE $0, REPLACE($1, '\n', ' '); 

STORE initial_informationflow_filtered INTO '$output/initial_informationflow' USING PigStorage('\t');
STORE clustering_features INTO '$output/sequence_files' USING com.twitter.elephantbird.pig.store.SequenceFileStorage('-c com.twitter.elephantbird.pig.util.TextConverter', '-c com.twitter.elephantbird.pig.util.TextConverter');

