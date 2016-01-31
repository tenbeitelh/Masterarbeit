
REGISTER hdfs:///lib/elephantbird/elephant-bird-core-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-pig-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-hadoop-compat-4.10.jar;
REGISTER hdfs:///lib/elephantbird/google-collections-1.0.jar;
REGISTER hdfs:///lib/elephantbird/json-simple-1.1.jar;
REGISTER hdfs:///lib/tenbeitel/StringUDF-0.0.1-SNAPSHOT.jar


twitter_files_of_month = LOAD '/user/flume/keyword/tweets/2015/08/{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

non_empty_tweets = FILTER twitter_files_of_month BY (((chararray)json#'text') IS NOT NULL);
non_empty_tweets2 = FILTER non_empty_tweets BY SIZE((chararray)json#'text')>0;

de_tweets = FILTER non_empty_tweets2 BY (((chararray)json#'lang') == 'de');

distinct_de_tweets = DISTINCT de_tweets;

features_selected = FOREACH distinct_de_tweets GENERATE json#'id', json#'id_str', json#'in_reply_to_screen_name', json#'in_preply_to_status_id_str', json#'in_reply_to_status_id', json#'in_reply_to_user_id_str', json#'in_reply_to_user_id', json#'created_at', json#'lang', json#'text', json#'entities', json#'user';


initial_informationflow = FOREACH features_selected GENERATE $0, $4;
initial_informationflow_filtered = FILTER initial_informationflow BY ($1 IS NOT NULL);

replace_linebreaks = FOREACH features_selected GENERATE $0, de.hs.osnabrueck.pig.string.ReplaceLinebreakUDF($9);
--replace_linebreaks = FOREACH features_selected GENERATE $0, REPLACE(((chararray)$9), '\\r?\\n', ' ');
replace_urls = FOREACH  replace_linebreaks GENERATE $0, de.hs.osnabrueck.pig.string.ReplaceUrlUDF($1); 
--replace_urls = FOREACH  replace_linebreaks GENERATE $0, REPLACE(((chararray)$1), '(https?|ftp|file):/{0,2}[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]', ''); 
replace_retweet = FOREACH  replace_urls GENERATE $0, REPLACE(((chararray)$1), 'RT', ''); 
clustering_features = FOREACH  replace_retweet GENERATE  $0, $1;

visualize_features = FOREACH features_selected GENERATE $0, $7, $11, de.hs.osnabrueck.pig.string.ReplaceControlCharUDF($9);
visualize_features2 = FOREACH visualize_features GENERATE $0, $1, $2#'id_str', $2#'screen_name', $3;
--visualize_features = FOREACH features_selected GENERATE $0, $7, $11, REPLACE(((chararray)$9), '\\r?\\n', ' ');;

twitter_id_to_date = FOREACH features_selected GENERATE $0, $7;

STORE initial_informationflow_filtered INTO '/project/201508/initial_informationflow' USING PigStorage('\t');
STORE clustering_features INTO '/project/201508/sequence_files' USING com.twitter.elephantbird.pig.store.SequenceFileStorage('-c com.twitter.elephantbird.pig.util.TextConverter', '-c com.twitter.elephantbird.pig.util.TextConverter');
STORE visualize_features2 INTO '/project/201508/tweet_detailed' USING PigStorage('\t');
STORE twitter_id_to_date INTO '/project/201508/twitter_id_date' USING com.twitter.elephantbird.pig.store.SequenceFileStorage('-c com.twitter.elephantbird.pig.util.TextConverter', '-c com.twitter.elephantbird.pig.util.TextConverter');
