mvn clean && mvn compile && mvn package;
spark-submit --master yarn --num-executors 50 target/Project-0.0.1.jar /raw_data/ 21 --jars hbase classpath;

# Run without details in terminal
# spark-submit --master yarn --num-executors 38 --deploy-mode cluster target/Project-0.0.1.jar /raw_data/ --jars hbase classpath