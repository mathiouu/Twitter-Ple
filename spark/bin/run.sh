mvn clean && mvn compile && mvn package;
spark-submit --num-executors 4 --executor-cores 2 --driver-memory 512M target/Project-0.0.1.jar 21 --jars hbase classpath