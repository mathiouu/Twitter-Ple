package bigdata;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.io.IOException;
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
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

import org.apache.spark.api.java.function.Function2;
import java.util.regex.*;
import bigdata.utils.Utils;
import scala.Tuple2;
import scala.Tuple3;
import bigdata.comparators.*;

public class UsersByHashtags {
    
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <directory path> <day selected> ");
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		int nbSelectedDays = 2;
		JavaRDD<Tuple2<String,Tuple2<Integer, String>>> emptyRdd = context.emptyRDD();
		JavaPairRDD<String,Tuple2<Integer, String>> allData = JavaPairRDD.fromJavaRDD(emptyRdd);

        for(int i = 1; i <= nbSelectedDays; i++){
			String tweetFile = getTweetFile(args[0], Integer.toString(i));
			JavaRDD<String> lines = context.textFile(tweetFile, 4);
			JavaRDD<JsonObject> tweets = convertLinesToTweets(lines);
			JavaRDD<Tuple2<String, Tuple2<Integer, String>>> hashtagsRdd = getHashtags(tweets);
			
			JavaPairRDD<String,Tuple2<Integer, String>> pairs = hashtagsRdd.mapToPair(hashtag-> hashtag);
			allData = allData.union(pairs);
			
		}
		JavaPairRDD<String, Tuple2<ArrayList<String>, Integer>> aggr = getHashtagsByUsers(allData);
			
		createHBaseTable(aggr, context);
        
		context.stop();
	}

	public static JavaRDD<Tuple2<String, Tuple2<Integer, String>>> getHashtags(JavaRDD<JsonObject> tweets){
		JavaRDD<Tuple2<String, Tuple2<Integer, String>>> hashtagsRdd = tweets.flatMap(tweet->{
			ArrayList<Tuple2<String, Tuple2<Integer, String>>> res = new ArrayList<Tuple2<String, Tuple2<Integer, String>>>();
			try{
				JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
				String user = tweet.getAsJsonObject("user").get("name").getAsString();
				for(int i = 0; i < hashtags.size(); i++){
					String text = hashtags.get(i).getAsJsonObject().get("text").getAsString();	
					Tuple2<Integer, String> values = new Tuple2<Integer, String>(1, user);
					res.add(new Tuple2<String, Tuple2<Integer, String>>(text.toLowerCase(), values));
				} 
				return res.iterator();
			}
			catch(Exception e){
				return res.iterator();
			}
		});
		return hashtagsRdd;
	}
   
	public static String getTweetFile(String directory, String dayInArg){
		
		String dataDirectory = directory;
		int daySelected = Integer.parseInt(dayInArg);
        if(daySelected==0){
            return "/raw_data/tweet_01_03_2020_first10000.nljson";
        }
		if(daySelected < 1 || daySelected > 21){
			System.err.println("Day are included between 1 and 21");
			System.exit(1);
		}
		
		String tweetStartFilePath = dataDirectory + "/tweet_";
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

	public static JavaPairRDD<String, Tuple2<ArrayList<String>, Integer>> getHashtagsByUsers(JavaPairRDD<String, Tuple2<Integer, String>> pairs){
		Tuple2<ArrayList<String>, Integer> zeroValue = new Tuple2<ArrayList<String>, Integer>(new ArrayList<String>(), 0);
		
		
		Function2<Tuple2<ArrayList<String>, Integer>,Tuple2<Integer, String>, Tuple2<ArrayList<String>, Integer> > seqOp = new Function2<Tuple2<ArrayList<String>, Integer>,Tuple2<Integer, String>,Tuple2<ArrayList<String>, Integer>>(){
			
			@Override
			public Tuple2<ArrayList<String>, Integer> call(Tuple2<ArrayList<String>, Integer> accumulator,Tuple2<Integer, String> element){
				ArrayList<String> userList = new ArrayList<String>(accumulator._1);
				if(!userList.contains(element._2)){
					userList.add(element._2);
				}
				Tuple2<ArrayList<String>, Integer> newAccumulator = new Tuple2<ArrayList<String>, Integer>(userList, accumulator._2 + 1); 
				return newAccumulator;
			}
        };
    
        Function2<Tuple2<ArrayList<String>, Integer>, Tuple2<ArrayList<String>, Integer>, Tuple2<ArrayList<String>, Integer>> combOp = new Function2<Tuple2<ArrayList<String>, Integer>, Tuple2<ArrayList<String>, Integer>, Tuple2<ArrayList<String>,Integer>>(){
			@Override
			public Tuple2<ArrayList<String>, Integer> call(Tuple2<ArrayList<String>, Integer> accumulator1, Tuple2<ArrayList<String>, Integer> accumulator2){
				ArrayList<String> userList = new ArrayList<String>();
				for(int i = 0; i < accumulator1._1.size(); i++){
					if(!userList.contains(accumulator1._1.get(i))){
						userList.add(accumulator1._1.get(i));
					}
				}
				
				for(int i = 0; i < accumulator2._1.size(); i++){
					if(!userList.contains(accumulator2._1.get(i))){
						userList.add(accumulator2._1.get(i));
					}
				}
				Tuple2<ArrayList<String>, Integer> result = new Tuple2<ArrayList<String>, Integer>(userList, accumulator1._2 + accumulator2._2);
				return result;
			}
        };


		JavaPairRDD<String, Tuple2<ArrayList<String>, Integer>> aggr = pairs.aggregateByKey(zeroValue, seqOp, combOp);
		return aggr;
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

	public static void createHBaseTable(JavaPairRDD<String, Tuple2<ArrayList<String>, Integer>> rdd,JavaSparkContext context) {
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		String tableName = "seb-mat-hashtagsbyusers"; 
		byte[] familyName = Bytes.toBytes("hashtags");
		Connection connection = null;
		try {
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object.
			table = connection.getTable(TableName.valueOf(tableName));
			
			JavaPairRDD<Integer,Tuple2<ArrayList<String>, String>> tmpRdd =  rdd.mapToPair(t-> new Tuple2<Integer, Tuple2<ArrayList<String>, String>>(t._2._2, new Tuple2<ArrayList<String>, String>(t._2._1,t._1))).sortByKey(false);

			JavaPairRDD<String, Tuple2<ArrayList<String>, Integer>> sortedRdd = tmpRdd.mapToPair(t-> new Tuple2<String, Tuple2<ArrayList<String>, Integer>>(t._2._2, new Tuple2<ArrayList<String>, Integer>(t._2._1,t._1) ));

			List<Tuple2<String, Tuple2<ArrayList<String>, Integer>>> rddToList = sortedRdd.collect();
			int i = 0;

			for (Tuple2<String, Tuple2<ArrayList<String>, Integer>> line : rddToList) {
				Put put = new Put(Bytes.toBytes("row" + i));
				put.addColumn(familyName, Bytes.toBytes("hashtag"), Bytes.toBytes(line._1));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(String.format("%s", line._2._2)));
				Gson g = new Gson();
				String userJson = g.toJson(line._2._1);
				put.addColumn(familyName, Bytes.toBytes("users"), Bytes.toBytes(userJson));
				table.put(put);
				i++;
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


