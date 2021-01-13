package bigdata.q2;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.Lang;
import bigdata.utils.Utils;
import scala.Tuple2;

public class TweetNbByLang {
    
    public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TP Spark");
        JavaSparkContext context = new JavaSparkContext(conf);

		List<JavaPairRDD<String, Integer>> listOfRdd = new ArrayList<JavaPairRDD<String, Integer>>();
		int nbDaySelected = 1;
		for(int i = 1; i <= nbDaySelected; i++){

			String tweetFile = Utils.getTweetFile(args[0], Integer.toString(i));
			
			JavaRDD<String> lines = context.textFile(tweetFile, 4);
			JavaRDD<JsonObject> tweets = Utils.convertLinesToTweets(lines);
			JavaPairRDD<String, Integer> langsTweet = getTweetsByLang(tweets);

			listOfRdd.add(langsTweet);
		}

		JavaPairRDD<String, Integer> rdd = listOfRdd.get(0);
		for(int i = 1; i < listOfRdd.size() ; i++){
			rdd = rdd.union(listOfRdd.get(i)).distinct();
		}

		ArrayList<String> columns = new ArrayList<String>();
		columns.add("lang");
		columns.add("times");
		Utils.fillHBaseTable(rdd, context, "seb-mat-tweetByLang", Bytes.toBytes("tweetByLang"), columns);
		
		context.stop();
    }

    public static JavaPairRDD<String, Integer> getTweetsByLang(JavaRDD<JsonObject> tweets){

		JavaPairRDD<String, Integer> langsObj = tweets.mapToPair((tweet) -> {
			try{
				String lang = tweet.get("lang").getAsString();

				Lang langParser = new Lang(lang);
				String key = langParser.getConvertedLang();

				Integer value = 1;

				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(key, value);
				return tuple;
			} catch(Exception e){
				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>("unvalid country", 1);
				return tuple;
			}
		});

		JavaPairRDD<String, Integer> freq = langsObj.reduceByKey((a, b) -> a + b);
		return freq;
	}
    
}