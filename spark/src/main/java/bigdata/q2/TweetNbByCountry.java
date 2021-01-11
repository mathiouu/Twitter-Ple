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

public class TweetNbByCountry {

    public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TP Spark");
        JavaSparkContext context = new JavaSparkContext(conf);
        Utils utils = new Utils();

		List<JavaPairRDD<String, Integer>> listOfRdd = new ArrayList<JavaPairRDD<String, Integer>>();
		int nbDaySelected = 1;
		for(int i = 1; i <= nbDaySelected; i++){

			String tweetFile = utils.getTweetFile(args[0], Integer.toString(i));
			
			JavaRDD<String> lines = context.textFile(tweetFile, 4);
			JavaRDD<JsonObject> tweets = utils.convertLinesToTweets(lines);
			JavaPairRDD<String, Integer> countriesTweet = getTweetsByCountry(tweets);

			listOfRdd.add(countriesTweet);
		}

		JavaPairRDD<String, Integer> rdd = listOfRdd.get(0);
		for(int i = 1; i < listOfRdd.size() ; i++){
			rdd = rdd.union(listOfRdd.get(i)).distinct();
		}

		ArrayList<String> columns = new ArrayList<String>();
		columns.add("country");
		columns.add("times");
		utils.fillHBaseTable(rdd, context, "seb-mat-tweetByCountry", Bytes.toBytes("tweetByCountry"), columns);
		
		context.stop();
    }

    public static JavaPairRDD<String, Integer> getTweetsByCountry(JavaRDD<JsonObject> tweets){

		JavaPairRDD<String, Integer> countriesObj = tweets.mapToPair((tweet) -> {
			try{
				String country = tweet.getAsJsonObject("place").get("country").getAsString();
				String key = country.toLowerCase();

				Integer value = 1;

				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(key, value);
				return tuple;
			} catch(Exception e){
				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>("", 0);
				return tuple;
			}
		});

		JavaPairRDD<String, Integer> freq = countriesObj.reduceByKey((a, b) -> a + b);		
		return freq;
    }
    
}
