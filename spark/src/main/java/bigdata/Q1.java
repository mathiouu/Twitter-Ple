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

import bigdata.Tweet.TweetEntity.TweetHashtags;
import scala.Tuple2;

public class Q1 {

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <file>");
			System.exit(1);
		}
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> lines = context.textFile(args[0], 4);

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
		JavaRDD<TweetHashtags> hashtagsObj = tweets.flatMap(tweet -> {
			ArrayList<TweetHashtags> res = new ArrayList<TweetHashtags>();
			try {
				for (TweetHashtags hashtag : tweet.entities.hashtags) {
					res.add(hashtag);
				}
				return res.iterator();
			} catch (Exception e) {
				return res.iterator();
			}
		});

		
		JavaRDD<String> hashtags = hashtagsObj.map(hashtag -> hashtag.text);
		JavaRDD<String> lowerHastags = hashtags.map(hashtag -> hashtag.toLowerCase());
		
		JavaPairRDD<String,Integer> counts = lowerHastags
		.mapToPair(hashtag -> new Tuple2<String, Integer>(hashtag,1));
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
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object.
			table = connection.getTable(TableName.valueOf(tableName));
			List<Tuple2<String,Integer>> data = hashtags.collect();
			
			Integer i = 0;
			for (Tuple2<String,Integer> line : data) {
				Put put = new Put(Bytes.toBytes("row" + i));
				//System.out.println(String.format("(%s,%s)", line._1,line._2));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(line._2));
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
// Country,City,AccentCity,RegionCode,Population,Latitude,Longitude

// Nombre de villes valides : 47980
// Nombre de villes :3173959
