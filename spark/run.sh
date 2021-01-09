mvn clean && mvn compile && mvn package;
# spark-submit --num-executors 4 --executor-cores 2 --driver-memory 512M target/Project-0.0.1.jar 1 --jars hbase classpath

# spark-submit --num-executors 4 --executor-cores 2 --driver-memory 512M target/Project-0.0.1.jar /raw_data/tweet_01_03_2020_first10000.nljson --jars hbase classpath

spark-submit --num-executors 4 --executor-cores 2 --driver-memory 512M target/Project-0.0.1.jar /raw_data/ --jars hbase classpath

# spark-submit --num-executors 4 --executor-cores 2 --driver-memory 512M target/Project-0.0.1.jar --jars hbase classpath