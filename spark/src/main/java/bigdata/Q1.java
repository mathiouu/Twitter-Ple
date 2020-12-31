  
package bigdata;

import java.util.List;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
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

import bigdata.tweetTest.*;
import scala.Tuple2;

public class Q1 {

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <file>");
			System.exit(1);
		}

		// int daySelected = Integer.parseInt(args[0]);

		// if(daySelected < 1 || daySelected > 21){
		// 	System.err.println("Day are incluted between 1 and 21");
		// 	System.exit(1);
		// }

		// String tweetStartFilePath = "/raw_data/tweet_";
		// String tweetEndFilePath = "_03_2020.nljson";
		// StringBuilder day = new StringBuilder();
		// if(daySelected < 10){
		// 	day.append(tweetStartFilePath);
		// 	day.append("0");
		// }
		// else{
		// 	day.append(tweetStartFilePath);
		// }
		// String tweetFile = day.toString() + daySelected + tweetEndFilePath;
	
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> lines = context.textFile(args[0], 4);

		JavaRDD<JsonObject> tweets = convertLinesToTweets(lines);
		
		
		JavaPairRDD<String,Integer> hashtags = getTopKHashtags(tweets);
		createHBaseTable(hashtags, context);
		
		context.stop();
	}

	public static JavaRDD<JsonObject> convertLinesToTweets(JavaRDD<String> lines) {
		JavaRDD<JsonObject> tweets = lines.map(line-> {
			Gson gson = new Gson();
			return gson.fromJson(line, JsonElement.class).getAsJsonObject();
		});
		return tweets;
	}

	public static JavaPairRDD<String, Integer> getTopKHashtags(JavaRDD<JsonObject> tweets) {
		
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
		JavaPairRDD<String,Integer> counts = hashtagsObj
		.mapToPair(hashtag -> new Tuple2<String, Integer>(hashtag,1));
		JavaPairRDD<String, Integer> freq = counts.reduceByKey((a, b) -> a + b);
		
		
		return freq;
	}

	public static void createHBaseTable(JavaPairRDD<String,Integer> hashtags,JavaSparkContext context) {
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
			
			List<Tuple2<String,Integer>> data = hashtags.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(100);
			Integer i = 0;
			for (Tuple2<String,Integer> line : data) {
				Put put = new Put(Bytes.toBytes("row" + i));
				System.out.println(String.format("(%s,%s)", line._1,line._2));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(String.format("%s",line._2)));
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