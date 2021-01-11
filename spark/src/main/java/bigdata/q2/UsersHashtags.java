package bigdata.q2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.comparators.CountComparator;
import bigdata.utils.Utils;
import scala.Tuple2;

public class UsersHashtags {

    public static void main(String[] args) {

		// SparkConf conf = new SparkConf().setAppName("TP Spark");
        // JavaSparkContext context = new JavaSparkContext(conf);

		// List<JavaPairRDD<String, Integer>> listOfRdd = new ArrayList<JavaPairRDD<String, Integer>>();
		// int nbDaySelected = 1;
		// for(int i = 1; i <= nbDaySelected; i++){

		// 	String tweetFile = Utils.getTweetFile(args[0], Integer.toString(i));
			
		// 	JavaRDD<String> lines = context.textFile(tweetFile, 4);
        //     JavaRDD<JsonObject> tweets = Utils.convertLinesToTweets(lines);
		// 	JavaPairRDD<String, Integer> usersHashtags = getUsersHashtags(tweets);

		// 	listOfRdd.add(usersHashtags);
		// }

		// JavaPairRDD<String, Integer> rdd = listOfRdd.get(0);
		// for(int i = 1; i < listOfRdd.size() ; i++){
		// 	rdd = rdd.union(listOfRdd.get(i)).distinct();
		// }

		// ArrayList<String> columns = new ArrayList<String>();
		// columns.add("userName");
        // columns.add("times");
		// Utils.fillHBaseTable(rdd, context, "seb-mat-userHastags", Bytes.toBytes("userHashtags"), columns);
		
		// context.stop();
    }

    public static JavaPairRDD<String, String> getUsersHashtags(JavaRDD<JsonObject> tweets) {
		JavaPairRDD<String, String> userNameHashtagsObj = tweets.mapToPair((tweet) -> {
			try{
				JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
				StringBuilder str = new StringBuilder();
				for(int i = 0 ; i < hashtags.size(); i++){

					String hashtagsText = hashtags.get(i).getAsJsonObject().get("text").getAsString();	
					str.append("#");
					// str.append(hashtagsText.toLowerCase());
					str.append(hashtagsText);
					if(!(i + 1 == hashtags.size())){
						str.append(", ");
					}
				} 

				String userName = tweet.getAsJsonObject("user").get("name").getAsString();
				String twitterName = tweet.getAsJsonObject("user").get("screen_name").getAsString();

				String key = userName.toLowerCase() + " (@" + twitterName + ")";
				String value = str.toString();

				Tuple2<String, String> tuple = new Tuple2<String, String>(key, value);
				return tuple;
			}
			catch(Exception e){
				Tuple2<String, String> tuple = new Tuple2<String, String>("", "");
				return tuple;
			}
		});

		JavaPairRDD<String, String> withoutDuplicates = userNameHashtagsObj.reduceByKey((a, b) -> a + b);
		return withoutDuplicates;
	}


}