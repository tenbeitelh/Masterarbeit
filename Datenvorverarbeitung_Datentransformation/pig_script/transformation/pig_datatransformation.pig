REGISTER hdfs:///lib/elephantbird/elephant-bird-core-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-pig-4.10.jar;
REGISTER hdfs:///lib/elephantbird/elephant-bird-hadoop-compat-4.10.jar;
REGISTER hdfs:///lib/elephantbird/google-collections-1.0.jar;
REGISTER hdfs:///lib/elephantbird/json-simple-1.1.jar;

REGISTER hdfs:///lib/piggybank/piggybank-0.14.0.jar;

preprocessed_twitter_files = LOAD '/project/preprocessing/$input_folder' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

-- Feature selection: only get the necessary features for the classification and clustering algorithms,
features_selected = FOREACH preprocessed_twitter_files GENERATE json#'id', json#'id_str', json#'in_reply_to_screen_name', json#'in_preply_to_status_id_str', json#'in_reply_to_status_id', json#'in_reply_to_user_id_str', json#'in_reply_to_user_id', json#'created_at', json#'lang', json#'text', json#'entities', json#'user';

clustering_features = FOREACH preprocessed_twitter_files GENERATE json#'id', json#'text';

STORE features_selected INTO '/project/transformation/complete/$output_folder_name' USING org.apache.pig.piggybank.storage.MultiStorage('/project/transformation/complete/$output_folder_name','0', 'none', '\\t');

STORE clustering_features INTO '/project/transformation/clustering' USING org.apache.pig.piggybank.storage.MultiStorage('/project/transformation/clustering/$output_folder_name','0', 'none', '\\t');