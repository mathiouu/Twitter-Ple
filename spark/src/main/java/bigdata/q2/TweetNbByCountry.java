package bigdata.q2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import bigdata.utils.Utils;
import bigdata.utils.Lang;
import scala.Tuple2;
public class TweetNbByCountry {

	public static FlatMapFunction<String, Tuple2<String, Integer>> getNbTweetByCountry = new FlatMapFunction<String, Tuple2<String, Integer>>(){

		private static final long serialVersionUID = 1L;
	
		@Override
		public Iterator<Tuple2<String, Integer>> call(String line) {

			List<Tuple2<String, Integer>> res = new ArrayList<Tuple2<String, Integer>>();
			Gson gson = new Gson();

			try{
				JsonObject tweet = gson.fromJson(line, JsonElement.class).getAsJsonObject();

				String country = tweet.getAsJsonObject("place").get("country").getAsString();
				String key = country.toLowerCase();

				Integer value = 1;

				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(key, value);

				res.add(tuple);
				return res.iterator();
			} catch (Exception e){
				return res.iterator();
			}
		}
	};

    public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		// String filePath = "/raw_data/tweet_01_03_2020_first10000.nljson";
		// String filePath = "/raw_data/tweet_01_03_2020.nljson";

		List<JavaPairRDD<String, Integer>> listOfRdd = new ArrayList<JavaPairRDD<String, Integer>>();

		int nbDaySelected = 5;
		for(int i = 1; i <= nbDaySelected; i++){

			String filePath = Utils.getTweetFile(args[0], Integer.toString(i));
			
			// JavaRDD<String> lines = context.textFile(filePath, 4);
			JavaRDD<String> lines = context.textFile(filePath, 4);

			JavaRDD<Tuple2<String, Integer>> tweets = lines.flatMap(getNbTweetByCountry);
			JavaPairRDD<String, Integer> pairRddNbTweetByCountry = tweets.mapToPair(tweet-> tweet);
			
			JavaPairRDD<String, Integer> resRDDNbTweetByCountry = pairRddNbTweetByCountry.reduceByKey((a,b) -> a + b);

			listOfRdd.add(resRDDNbTweetByCountry);
		}

		JavaPairRDD<String, Integer> rdd = listOfRdd.get(0);
		for(int i = 1; i < listOfRdd.size() ; i++){
			rdd = rdd.union(listOfRdd.get(i)).distinct();
		}

		ArrayList<String> columns = new ArrayList<String>();
		columns.add("country");
		columns.add("times");
		Utils.fillHBaseTable(rdd, context, "seb-mat-tweetByCountry", Bytes.toBytes("tweetByCountry"), columns);
		
		context.stop();
    }
    
}
