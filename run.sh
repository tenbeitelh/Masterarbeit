
hadoop jar Transformation-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs://master.hs.osnabrueck.de:8020/project/$month/sequence_files

hadoop jar KMeansDriver-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs://master.hs.osnabrueck.de:8020/project/$month/

hadoop jar CreateInitialInformationFlow-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs://master.hs.osnabrueck.de:8020/project/$month/initial_informationflow/ hdfs://master.hs.osnabrueck.de:8020/project/$month/graph_data/

hadoop jar Twitter
 