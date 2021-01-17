package bigdata;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import java.util.regex.*;
import scala.Tuple2;
import scala.Tuple3;
import bigdata.comparators.*;

public class TopKHashtags {
    public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <directory path> <day selected> ");
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		List<JavaPairRDD<String, Integer>> listOfRdd = new ArrayList<JavaPairRDD<String, Integer>>();
		int nbDaySelected= 5;
	
		//String tweetFile = getTweetFile(args[0], Integer.toString(i));

		// Pattern p = Pattern.compile("(.*tweet_)(.+)(.nljson)");

		// Matcher m = p.matcher(tweetFile);
		// String date = "row";
		// if(m.find()){
		// 	date = m.group(2);
		// }

		JavaRDD<String> lines = context.textFile(args[0], 12);
		JavaRDD<Tuple2<String,Integer>> hashtags = convertLinesToHashtags(lines);
		JavaPairRDD<String,Integer> hashtagsPairRDD= hashtags.mapToPair(line->line).reduceByKey((a,b)-> a+b);
		List<Tuple2<String, Integer>> topK = getTopK(hashtagsPairRDD, 10000);
			
		createHBaseTable(topK,context);
		
		context.stop();
	}

	

	public static JavaRDD<Tuple2<String,Integer>> convertLinesToHashtags(JavaRDD<String> lines) {
		JavaRDD<Tuple2<String,Integer>> tweets = lines.flatMap(line-> {
			ArrayList<Tuple2<String,Integer>> res = new ArrayList<Tuple2<String,Integer>>();
			Gson gson = new Gson();
			try{
				JsonElement tweet = gson.fromJson(line, JsonElement.class).getAsJsonObject();
				JsonArray hashtags = tweet.getAsJsonObject().getAsJsonObject("entities").getAsJsonArray("hashtags");
				for(int i = 0 ; i< hashtags.size(); i++ ){
					String text = hashtags.get(i).getAsJsonObject().get("text").getAsString();	
					res.add(new Tuple2<String,Integer>(text.toLowerCase(),1));
				} 
				return res.iterator();
			}
			catch(Exception e){
				return res.iterator();
			}
		});
		return tweets;
	}

	public static JavaPairRDD<String, Integer> getHashtags(JavaRDD<JsonObject> tweets, String date) {
		
		JavaRDD<String> hashtagsObj = tweets.flatMap(tweet -> {
			ArrayList<String> res = new ArrayList<String>();
			try{
				JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
				for(int i = 0 ; i< hashtags.size(); i++ ){
					String text = hashtags.get(i).getAsJsonObject().get("text").getAsString();	
					res.add(text.toLowerCase());
				} 
				return res.iterator();
			}
			catch(Exception e){
				return res.iterator();
			}
			
		});
		JavaPairRDD<String,Integer> counts = hashtagsObj.mapToPair(hashtag -> new Tuple2<String, Integer>(hashtag, 1));
		JavaPairRDD<String, Integer> freq = counts.reduceByKey((a, b) -> a + b);
		
		
		return freq;
	}
	public static List<Tuple2<String, Integer>> getTopK(JavaPairRDD<String, Integer> hashtags, int k){
		List<Tuple2<String, Integer>> data = hashtags.takeOrdered(k, new NbHashtagComparator());
		return data;
	}

	public static void createHBaseTable(List<Tuple2<String, Integer>> topK, JavaSparkContext context) {
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		String tableName = "testSmelezan"; 
		byte[] familyName = Bytes.toBytes("hashtags");
		Connection connection = null;
		try {
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object.
			table = connection.getTable(TableName.valueOf(tableName));
			
			Integer i = 0;

			for (Tuple2<String, Integer> line : topK) {
				Put put = new Put(Bytes.toBytes("row" + i));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(String.format("%s", line._2)));
				put.addColumn(familyName, Bytes.toBytes("hashtag"), Bytes.toBytes(line._1));
				i += 1;
				table.put(put);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					// Close the table object.
					table.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (connection != null) {
				try {
					// Close the HBase connection.
					connection.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}
