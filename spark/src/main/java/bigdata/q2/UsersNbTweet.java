package bigdata.q2;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.utils.Utils;
import scala.Tuple2;

public class UsersNbTweet {
    
    public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TP Spark");
        JavaSparkContext context = new JavaSparkContext(conf);

		List<JavaPairRDD<String, Integer>> listOfRdd = new ArrayList<JavaPairRDD<String, Integer>>();
		int nbDaySelected = 1;
		for(int i = 1; i <= nbDaySelected; i++){

			String tweetFile = Utils.getTweetFile(args[0], Integer.toString(i));
			
			JavaRDD<String> lines = context.textFile(tweetFile, 4);
            JavaRDD<JsonObject> tweets = Utils.convertLinesToTweets(lines);
			JavaPairRDD<String, Integer> usersNbTweet = getUsersNbTweet(tweets);

			listOfRdd.add(usersNbTweet);
		}

		JavaPairRDD<String, Integer> rdd = listOfRdd.get(0);
		for(int i = 1; i < listOfRdd.size() ; i++){
			rdd = rdd.union(listOfRdd.get(i)).distinct();
		}

		ArrayList<String> columns = new ArrayList<String>();
		columns.add("user");
        columns.add("times");
		Utils.fillHBaseTable(rdd, context, "seb-mat-userNbTweet", Bytes.toBytes("userNbTweet"), columns);
		
		context.stop();
    }

    public static JavaPairRDD<String, Integer> getUsersNbTweet(JavaRDD<JsonObject> tweets){

		JavaPairRDD<String, Integer> usersObj = tweets.mapToPair((tweet) -> {
			try{
				String userName = tweet.getAsJsonObject("user").get("name").getAsString();
				String twitterName = tweet.getAsJsonObject("user").get("screen_name").getAsString();

				String key = userName.toLowerCase() + " (@" + twitterName + ")";
				Integer value = 1;

				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(key, value);
				return tuple;
			} catch(Exception e){
				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>("unvalid user", 0);
				return tuple;
			}
		});

		JavaPairRDD<String, Integer> freq = usersObj.reduceByKey((a, b) -> a + b);		
		return freq;
	}

}