  
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
import java.util.regex.*;
import bigdata.tweetTest.*;
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
		for(int i = 1 ; i <=nbDaySelected; i++){
			
			String tweetFile = getTweetFile(args[0], Integer.toString(i));
	
			Pattern p = Pattern.compile("(.*tweet_)(.+)(.nljson)");
	
			Matcher m = p.matcher(tweetFile);
			String date = "row";
			if(m.find()){
				date = m.group(2);
			}
			JavaRDD<String> lines = context.textFile(tweetFile, 4);
			JavaRDD<JsonObject> tweets = convertLinesToTweets(lines);
			JavaPairRDD<String, Integer> hashtags = getHashtags(tweets,date);
			List<Tuple2<String,Integer>> topK = getTopK(hashtags, 1000);
			JavaPairRDD<String, Integer> pairs = context.parallelizePairs(topK);

			listOfRdd.add(pairs);
		}
		
		createHBaseTable(listOfRdd,context);
		
		context.stop();
		///raw_data/tweet_01_03_2020.nljson
		///raw_data/tweet_01_03_2020.nljson
	}



	public static String getTweetFile(String directory, String dayInArg){
		
		String dataDirectory = directory;
		int daySelected = Integer.parseInt(dayInArg);

		if(daySelected < 1 || daySelected > 21){
			System.err.println("Day are included between 1 and 21");
			System.exit(1);
		}
		
		String tweetStartFilePath = dataDirectory+"/tweet_";
		String tweetEndFilePath = "_03_2020.nljson";
		StringBuilder day = new StringBuilder();
		if(daySelected < 10){
			day.append(tweetStartFilePath);
			day.append("0");
		}
		else{
			day.append(tweetStartFilePath);
		}
		return day.toString() + daySelected + tweetEndFilePath;
	}

	public static JavaRDD<JsonObject> convertLinesToTweets(JavaRDD<String> lines) {
		JavaRDD<JsonObject> tweets = lines.flatMap(line-> {
			ArrayList<JsonObject> res = new ArrayList<JsonObject>();
			Gson gson = new Gson();
			try{
				res.add(gson.fromJson(line, JsonElement.class).getAsJsonObject());
				return res.iterator();
			}
			catch(Exception e){
				return res.iterator();
			}
		});
		return tweets;
	}

	public static JavaPairRDD<String, Integer> getHashtags(JavaRDD<JsonObject> tweets,String date) {
		
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
	public static List<Tuple2<String,Integer>> getTopK(JavaPairRDD<String,Integer> hashtags, int k){
		List<Tuple2<String,Integer>> data = hashtags.takeOrdered(k, new NbHashtagComparator());
		return data;
	}

	public static void createHBaseTable(List<JavaPairRDD<String, Integer>> listOfRdd,JavaSparkContext context) {
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		String tableName = "seb-mat-topkhashtags"; 
		byte[] familyName = Bytes.toBytes("hashtags");
		Connection connection = null;
		try {
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object.
			table = connection.getTable(TableName.valueOf(tableName));
			JavaPairRDD<String, Integer> rdd = listOfRdd.get(0);
			for(int i = 1; i < listOfRdd.size() ; i++){
				rdd = rdd.union(listOfRdd.get(i));
            }
            JavaPairRDD<String, Integer> topK = rdd.reduceByKey((a, b) -> a + b);
			JavaPairRDD<String, Integer> orderedTopK = topK.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap());
            
			List<Tuple2<String,Integer>> topKMap = orderedTopK.collect();
			Integer i = 0;
			for (Tuple2<String,Integer> line : topKMap) {
				Put put = new Put(Bytes.toBytes("row"+i));
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
