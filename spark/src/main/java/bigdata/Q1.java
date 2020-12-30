package bigdata;

import java.util.List;
import java.util.ArrayList;

import com.google.gson.Gson;
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

import bigdata.Tweet;
import bigdata.HashTags;
import scala.Tuple2;

public class Q1 {

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <file>");
			System.exit(1);
		}

		int daySelected = Integer.parseInt(args[0]);

		if(daySelected < 1 || daySelected > 21){
			System.err.println("Day are incluted between 1 and 21");
			System.exit(1);
		}

		String tweetStartFilePath = "/raw_data/tweet_";
		String tweetEndFilePath = "_03_2020.nljson";
		StringBuilder day = new StringBuilder();
		if(daySelected < 10){
			day.append(tweetStartFilePath);
			day.append("0");
		}
		else{
			day.append(tweetStartFilePath);
		}
		String tweetFile = day.toString() + daySelected + tweetEndFilePath;

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		// JavaRDD<String> lines = context.textFile(args[0], 4);
		JavaRDD<String> lines = context.textFile(tweetFile, 4);
		
		JavaRDD<Tweet> tweets = convertLinesToTweets(lines);

		JavaPairRDD<String,Integer> hashtags = getTopKHashtags(tweets);
		createHBaseTable(hashtags, context);
		
		context.stop();
	}

	public static JavaRDD<Tweet> convertLinesToTweets(JavaRDD<String> lines) {
		JavaRDD<Tweet> tweets = lines.flatMap(line -> {
			ArrayList<Tweet> res = new ArrayList<Tweet>();
			try {
				Gson gson = new Gson();
				Tweet t = gson.fromJson(line, Tweet.class);
				res.add(t);
				return res.iterator();
			} catch (Exception e) {
				return res.iterator();
			}
		});
		return tweets;
	}

	public static JavaPairRDD<String, Integer> getTopKHashtags(JavaRDD<Tweet> tweets) {

		// JavaRDD<TweetHashtags> hashtagsObj = tweets.flatMap(tweet -> {
		JavaRDD<HashTags> hashtagsObj = tweets.flatMap(tweet -> {
			ArrayList<HashTags> res = new ArrayList<HashTags>();
			try {
				for (HashTags hashtag : tweet.entities.hashtags) {
					res.add(hashtag);
				}
				return res.iterator();
			} catch (Exception e) {
				return res.iterator();
			}
		});

		JavaRDD<String> hashtags = hashtagsObj.map(hashtag -> hashtag.text.toLowerCase());
		// JavaRDD<String> lowerHastags = hashtags.map(hashtag -> hashtag.toLowerCase());
		
		JavaPairRDD<String, Integer> counts = hashtags.mapToPair(hashtag -> new Tuple2<String, Integer>(hashtag, 1));
		JavaPairRDD<String, Integer> freq = counts.reduceByKey((a, b) -> a + b);
		// for(Tuple2<String,Integer> hashtag : freq.collect()){
		// 	System.out.println(String.format("(%s,%s)", hashtag._1,hashtag._2));
		// }
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
			// Obtain the HBase connection
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object
			table = connection.getTable(TableName.valueOf(tableName));
			//List<Tuple2<String,Integer>> topHashtags = hashtags.top(100);
			//List<Tuple2<String,Integer>> data = hashtags.top(100);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			//System.out.println(data.get(0));
			
			//List<Tuple2<String,Integer>> data = topHashtags;
			
			List<Tuple2<String,Integer>> data = hashtags.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(100);
			Integer i = 0;
			for (Tuple2<String,Integer> line : data) {
				Put put = new Put(Bytes.toBytes("row" + i));
				//System.out.println(String.format("(%s,%s)", line._1,line._2));
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