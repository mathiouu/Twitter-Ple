package bigdata.q2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.spark.api.java.function.FlatMapFunction;

import bigdata.utils.Utils;
import scala.Tuple2;

public class UsersHashtags {

	public static FlatMapFunction<String, Tuple2<Long, Tuple2<String, List<String>>>> getUserHashtags = new FlatMapFunction<String, Tuple2<Long, Tuple2<String, List<String>>>>(){
		private static final long serialVersionUID = 1L;
	
		@Override
		public Iterator<Tuple2<Long, Tuple2<String, List<String>>>> call(String line) {

			List<Tuple2<Long, Tuple2<String, List<String>>>> res = new ArrayList<Tuple2<Long, Tuple2<String, List<String>>>>();
			Gson gson = new Gson();

			try{
				JsonObject tweet = gson.fromJson(line, JsonElement.class).getAsJsonObject();
				Long key = tweet.getAsJsonObject("user").get("id_str").getAsLong();
				String userName = tweet.getAsJsonObject("user").get("screen_name").getAsString();

				List<String> hashtagsList = new ArrayList<String>();
				JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
				if(hashtags.size() < 1){
					throw new Exception("test exception");
				}
				for(int i = 0 ; i < hashtags.size(); i++){

					String hashtagsText = hashtags.get(i).getAsJsonObject().get("text").getAsString();
					hashtagsList.add(hashtagsText.toLowerCase());
				}
				
				Tuple2<String, List<String>> value = new Tuple2<String, List<String>>(userName, hashtagsList);
				Tuple2<Long, Tuple2<String, List<String>>> tuple = new Tuple2<Long, Tuple2<String, List<String>>>(key, value);

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

		String filePath = "/raw_data/tweet_01_03_2020_first10000.nljson";
		// String filePath = "/raw_data/tweet_01_03_2020.nljson";

		List<JavaPairRDD<String, String>> listOfRdd = new ArrayList<JavaPairRDD<String, String>>();

		int nbDaySelected = 1;
		for(int i = 1; i <= nbDaySelected; i++){

			// String tweetFile = Utils.getTweetFile(args[0], Integer.toString(i));
			
			JavaRDD<String> lines = context.textFile(filePath, 4);

			JavaRDD<Tuple2<Long, Tuple2<String, List<String>>>> tweets = lines.flatMap(getUserHashtags);
			JavaPairRDD<Long, Tuple2<String, List<String>>> pairRddUserHashtags = tweets.mapToPair(tweet-> tweet);

			JavaPairRDD<String, String> userHashTags = getUsersHashtags(pairRddUserHashtags);

			listOfRdd.add(userHashTags);
		}

		JavaPairRDD<String, String> rdd = listOfRdd.get(0);
		for(int i = 1; i < listOfRdd.size() ; i++){
			rdd = rdd.union(listOfRdd.get(i)).distinct();
		}

		ArrayList<String> columns = new ArrayList<String>();
		columns.add("user");
		columns.add("hashTags");
		Utils.fillHBaseTable1(rdd, context, "seb-mat-userHashtags", Bytes.toBytes("userHashtags"), columns);
	
		context.stop();
    }

	public static JavaPairRDD<String, String> getUsersHashtags(JavaPairRDD<Long, Tuple2<String, List<String>>> userHashtags) {

		// Reducer (withou duplicates)
		JavaPairRDD<Long, Tuple2<String, List<String>>> reducer = userHashtags.reduceByKey((a,b) -> {
			List<String> hashTagsListA = a._2();
			List<String> hashTagsListB = b._2();

			hashTagsListA.addAll(hashTagsListB);
			
			StringBuilder str = new StringBuilder();
			str.append(a._1());
			str.append(",");
			str.append(b._1());
			String tuple_1 = str.toString();
			List<String> tuple_2 = new ArrayList<>(new HashSet<>(hashTagsListA));

			Tuple2<String, List<String>> res = new Tuple2<String, List<String>>(tuple_1, tuple_2);
			return res;
		});

		// Map to Pair
		JavaPairRDD<String, String> pairRDDRes = reducer.mapToPair((tuple) -> {
			try{
				String userNameList = tuple._2()._1();
				String splitted[] = userNameList.split(",");

				String key = '@' + splitted[splitted.length - 1];

				List<String> hashtagsWithoutDuplicates = tuple._2()._2();
				StringBuilder value = new StringBuilder();

				for(String hashtag : hashtagsWithoutDuplicates){
					value.append(hashtag);
					value.append(",");
				}

				Tuple2<String, String> res = new Tuple2<String, String>(key, value.toString());
				return res;
			} catch (Exception e){
				String key = "unvalid userName";
				String value = "unvalid #";
				Tuple2<String, String> res = new Tuple2<String, String>(key, value);
				return res;
			}
		});

		return pairRDDRes;
	}
}