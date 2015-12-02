-- $input parameter contains the directories which are to process
twitter_files_of_month = LOAD '$input' USING JsonLoader('filter_level: chararray, retweeted: chararray, in_reply_to_screen_name: chararray, truncated: chararray,
lang: chararray, in_reply_to_status_id_str: chararray, id: chararray, in_reply_to_user_id_str: chararray,
timestamp_ms: chararray, in_reply_to_status_id: chararray, created_at: chararray, favorite_count: chararray,
place: chararray, coordinates: chararray, text: chararray, contributors: chararray,
geo: chararray, entities: {( symbols: {(symbol:chararray)}, urls: {(url: chararray)}, hashtags: {(text: chararray, indices: {(index:chararray)})})},
user_mentions: {(id: chararray, name: chararray, indices: {(index:chararray)}, screen_name: chararray, id_str: chararray)},
is_quote_status: chararray, source: chararray, favorited: chararray, in_reply_to_user_id: chararray, retweet_count: chararray, id_str: chararray,
user: { (location: chararray, default_profile: chararray, profile_background_tile: chararray, statuses_count: chararray, lang: chararray,
profile_link_color: chararray, profile_banner_url: chararray, id: chararray, following: chararray, protected: chararray, favourites_count: chararray,
profile_text_color: chararray, verified: chararray, description: chararray, contributors_enabled: chararray, profile_sidebar_border_color: chararray,
name: chararray, profile_background_color: chararray, created_at: chararray, default_profile_image: chararray, followers_count: chararray,
profile_image_url_https: chararray, geo_enabled: chararray, profile_background_image_url: chararray, profile_background_image_url_https: chararray,
follow_request_sent: chararray, url: chararray, utc_offset: chararray, time_zone: chararray, notifications: chararray, profile_use_background_image: chararray,
friends_count: chararray, profile_sidebar_fill_color: chararray, screen_name: chararray, id_str: chararray, profile_image_url: chararray, listed_count: chararray, 
is_translator: chararray) }');

non_empty_tweets=FILTER twitter_files_of_month BY (text IS NOT NULL);
non_empty_tweets2=FILTER non_empty_tweets BY SIZE(text)>0;

de_tweets=FILTER non_empty_tweets2 BY (lang == 'de');

distinct_de_tweets=DISTINCT de_tweets;

--STORE de_tweets INTO '/preprocessing/de_tweets/test' USING PigStorage();
STORE de_tweets INTO '$output' USING JsonStorage();




