package bigdata.q2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import bigdata.utils.Utils;
import scala.Tuple2;

public class UsersNbTweet {

	public static FlatMapFunction<String, Tuple2<Long, Tuple2<String, Integer>>> getUserNbTweet = new FlatMapFunction<String, Tuple2<Long, Tuple2<String, Integer>>>(){

		private static final long serialVersionUID = 1L;
	
		@Override
		public Iterator<Tuple2<Long, Tuple2<String, Integer>>> call(String line) {

			List<Tuple2<Long, Tuple2<String, Integer>>> res = new ArrayList<Tuple2<Long, Tuple2<String, Integer>>>();
			Gson gson = new Gson();

			try{
				JsonObject tweet = gson.fromJson(line, JsonElement.class).getAsJsonObject();
				Long key = tweet.getAsJsonObject("user").get("id_str").getAsLong();
				
				String userName = tweet.getAsJsonObject("user").get("screen_name").getAsString();

				Tuple2<String, Integer> value = new Tuple2<String, Integer>(userName, 1);
				Tuple2<Long, Tuple2<String, Integer>> tuple = new Tuple2<Long, Tuple2<String, Integer>>(key, value);

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
		String filePath = "/raw_data/tweet_01_03_2020.nljson";

		List<JavaPairRDD<String, Integer>> listOfRdd = new ArrayList<JavaPairRDD<String, Integer>>();

		int nbDaySelected = 1;
		for(int i = 1; i <= nbDaySelected; i++){

			// String tweetFile = Utils.getTweetFile(args[0], Integer.toString(i));

			JavaRDD<String> lines = context.textFile(filePath, 4);
			
            JavaRDD<Tuple2<Long, Tuple2<String, Integer>>> tweets = lines.flatMap(getUserNbTweet);
			JavaPairRDD<Long, Tuple2<String, Integer>> pairRddUserNbTweet = tweets.mapToPair(tweet-> tweet);

			JavaPairRDD<String, Integer> usersNbTweet = getUsersNbTweet(pairRddUserNbTweet);

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

    
	public static JavaPairRDD<String, Integer> getUsersNbTweet(JavaPairRDD<Long, Tuple2<String, Integer>> tweets){
	
		JavaPairRDD<Long, Tuple2<String, Integer>> reducer = tweets.reduceByKey((a,b) -> {
			StringBuilder str = new StringBuilder();
			str.append(a._1());
			str.append(",");
			str.append(b._1());
			String tuple_1 = str.toString();
			Integer tuple_2 = a._2() + b._2();

			Tuple2<String, Integer> res = new Tuple2<String, Integer>(tuple_1, tuple_2);
			return res;
		});

		JavaPairRDD<String, Integer> pairRDDRes = reducer.mapToPair((tuple) -> {
			try{
				String userNameList = tuple._2()._1();
				String splitted[] = userNameList.split(",");

				String key = '@' + splitted[splitted.length - 1];
				Integer value = tuple._2()._2();

				Tuple2<String, Integer> res = new Tuple2<String, Integer>(key, value);
				return res;
			} catch (Exception e){
				String key = "unvalid userName";
				Integer value = 1;
				Tuple2<String, Integer> res = new Tuple2<String, Integer>(key, value);
				return res;
			}
		});

		return pairRDDRes;
	}


}