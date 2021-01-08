package bigdata;

import java.util.List;
import java.io.File;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Q2 {
    public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

        String file1Path = "/raw_data/tweet_01_03_2020_first10000.nljson";
		JavaRDD<String> lines = context.textFile(file1Path, 4);

		JavaRDD<JsonObject> tweets = convertLinesToTweets(lines);
		
		// JavaPairRDD<String, String> usersHashtags = getUsersHashtags(tweets);
		// createHBaseTable(usersHashtags, context);

		// JavaPairRDD<String, Integer> usersNbTweet = getUsersNbTweet(tweets);
		// createHBaseTable1(usersNbTweet, context);

		// JavaPairRDD<String, Integer> countriesTweet = getTweetByCountry(tweets);
		// createHBaseTableCountries(countriesTweet, context);

		JavaPairRDD<String, Integer> langsTweet = getTweetByLang(tweets);
		createHBaseTableLangs(langsTweet, context);
		
		context.stop();
	}

	public static JavaRDD<JsonObject> convertLinesToTweets(JavaRDD<String> lines) {
		JavaRDD<JsonObject> tweets = lines.map(line-> {
			Gson gson = new Gson();
			return gson.fromJson(line, JsonElement.class).getAsJsonObject();
		});
		return tweets;
	}

	public static JavaPairRDD<String, Integer> getTweetByLang(JavaRDD<JsonObject> tweets){

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

		// JavaRDD<String> langsObj = tweets.flatMap((tweet) -> {
		// 	ArrayList<String> langs = new ArrayList<String>();
		// 	try{

		// 		String lang = tweet.get("lang").getAsString();
		// 		Lang langParser = new Lang(lang);

		// 		String key = langParser.getConvertedLang();

		// 		langs.add(key);

		// 		return langs.iterator();
		// 	}
		// 	catch(Exception e){
		// 		return langs.iterator();
		// 	}
		// });

		// JavaPairRDD<String, Integer> counts = langsObj.mapToPair(country -> new Tuple2<String, Integer>(country, 1));
		JavaPairRDD<String, Integer> freq = langsObj.reduceByKey((a, b) -> a + b);
		return freq;
	}

	public static JavaPairRDD<String, Integer> getTweetByCountry(JavaRDD<JsonObject> tweets){

		JavaPairRDD<String, Integer> countriesObj = tweets.mapToPair((tweet) -> {
			try{
				String country = tweet.getAsJsonObject("place").get("country").getAsString();
				String key = country.toLowerCase();

				Integer value = 1;

				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(key, value);
				return tuple;
			} catch(Exception e){
				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>("", 0);
				return tuple;
			}
		});

		// JavaRDD<String> countriesObj = tweets.flatMap((tweet) -> {
		// 	ArrayList<String> users = new ArrayList<String>();
		// 	try{

		// 		String country = tweet.getAsJsonObject("place").get("country").getAsString();
		// 		String key = country.toLowerCase();

		// 		users.add(key);

		// 		return users.iterator();
		// 	}
		// 	catch(Exception e){
		// 		return users.iterator();
		// 	}
		// });

		// JavaPairRDD<String, Integer> counts = countriesObj.mapToPair(country -> new Tuple2<String, Integer>(country, 1));
		JavaPairRDD<String, Integer> freq = countriesObj.reduceByKey((a, b) -> a + b);		
		return freq;
	}

	public static JavaPairRDD<String, Integer> getUsersNbTweet(JavaRDD<JsonObject> tweets){

		JavaPairRDD<String, Integer> usersObj = tweets.mapToPair((tweet) -> {
			try{
				String userName = tweet.getAsJsonObject("user").get("name").getAsString();
				String twitterName = tweet.getAsJsonObject("user").get("screen_name").getAsString();

				String key = userName.toLowerCase() + " (@" + twitterName + ")";
				Integer value = 1;

				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(key, value);
				return tuple;
			} catch(Exception e){
				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>("", 0);
				return tuple;
			}
		});


		// JavaRDD<String> usersObj = tweets.flatMap((tweet) -> {
		// 	ArrayList<String> users = new ArrayList<String>();
		// 	try{

		// 		String userName = tweet.getAsJsonObject("user").get("name").getAsString();
		// 		String twitterName = tweet.getAsJsonObject("user").get("screen_name").getAsString();

		// 		String key = userName.toLowerCase() + " (@" + twitterName + ")";

		// 		users.add(key);

		// 		return users.iterator();
		// 	}
		// 	catch(Exception e){
		// 		return users.iterator();
		// 	}
		// });

		// JavaPairRDD<String, Integer> counts = usersObj.mapToPair(hashtag -> new Tuple2<String, Integer>(hashtag, 1));
		JavaPairRDD<String, Integer> freq = usersObj.reduceByKey((a, b) -> a + b);
		
		return freq;
	}

	public static JavaPairRDD<String, String> getUsersHashtags(JavaRDD<JsonObject> tweets) {
		JavaPairRDD<String, String> userNameHashtagsObj = tweets.mapToPair((tweet) -> {
			try{
				JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
				StringBuilder str = new StringBuilder();
				for(int i = 0 ; i < hashtags.size(); i++){

					String hashtagsText = hashtags.get(i).getAsJsonObject().get("text").getAsString();	
					str.append("#");
					// str.append(hashtagsText.toLowerCase());
					str.append(hashtagsText);
					if(!(i + 1 == hashtags.size())){
						str.append(", ");
					}
				} 

				String userName = tweet.getAsJsonObject("user").get("name").getAsString();
				String twitterName = tweet.getAsJsonObject("user").get("screen_name").getAsString();

				String key = userName.toLowerCase() + " (@" + twitterName + ")";
				String value = str.toString();

				Tuple2<String, String> tuple = new Tuple2<String, String>(key, value);
				return tuple;
			}
			catch(Exception e){
				Tuple2<String, String> tuple = new Tuple2<String, String>("", "");
				return tuple;
			}
		});

		JavaPairRDD<String, String> withoutDuplicates = userNameHashtagsObj.reduceByKey((a, b) -> a + b);
		
		return withoutDuplicates;
	}

	// HBASE
	public static void createHBaseTableLangs(JavaPairRDD<String, Integer> langs, JavaSparkContext context) {
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		String tableName = "testDubanLangs";
		byte[] familyName = Bytes.toBytes("tweetByLang");
		Connection connection = null;
		try {
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object.
			table = connection.getTable(TableName.valueOf(tableName));
			
			List<Tuple2<String,Integer>> data = langs.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(100);
			Integer i = 1;

			for (Tuple2<String, Integer> line : data) {
				Put put = new Put(Bytes.toBytes("row" + i));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(String.format("%s", line._2)));
				put.addColumn(familyName, Bytes.toBytes("lang"), Bytes.toBytes(line._1));
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

	public static void createHBaseTableCountries(JavaPairRDD<String, Integer> users, JavaSparkContext context) {
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		String tableName = "testDubanCountries";
		byte[] familyName = Bytes.toBytes("tweetByCountry");
		Connection connection = null;
		try {
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object.
			table = connection.getTable(TableName.valueOf(tableName));
			
			List<Tuple2<String,Integer>> data = users.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(100);
			Integer i = 0;
			for (Tuple2<String,Integer> line : data) {
				Put put = new Put(Bytes.toBytes("row" + i));
				System.out.println(String.format("(%s,%s)", line._1, line._2));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(String.format("%s", line._2)));
				put.addColumn(familyName, Bytes.toBytes("country"), Bytes.toBytes(line._1));
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

	public static void createHBaseTable1(JavaPairRDD<String, Integer> users, JavaSparkContext context) {
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		String tableName = "testDuban1";
		byte[] familyName = Bytes.toBytes("userNbTweet");
		Connection connection = null;
		try {
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object.
			table = connection.getTable(TableName.valueOf(tableName));
			
			List<Tuple2<String,Integer>> data = users.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(100);
			Integer i = 0;
			for (Tuple2<String,Integer> line : data) {
				Put put = new Put(Bytes.toBytes("row" + i));
				System.out.println(String.format("(%s,%s)", line._1, line._2));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(String.format("%s", line._2)));
				put.addColumn(familyName, Bytes.toBytes("user"), Bytes.toBytes(line._1));
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

	public static void createHBaseTable(JavaPairRDD<String, String> userHashtags, JavaSparkContext context) {
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		String tableName = "testDuban";
		byte[] familyName = Bytes.toBytes("userHashtags");
		Connection connection = null;
		try {
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object. 
			table = connection.getTable(TableName.valueOf(tableName));
			
			Integer i = 0;				
			List<Tuple2<String, String>> data = userHashtags.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(100);

			for (Tuple2<String, String> line : data) {
				Put put = new Put(Bytes.toBytes("row" + i));
				// System.out.println(String.format("(%s,%s)", line._1, line._2));
				put.addColumn(familyName, Bytes.toBytes("userName"), Bytes.toBytes(String.format("%s", line._1)));
				put.addColumn(familyName, Bytes.toBytes("hashtag"), Bytes.toBytes(String.format("%s", line._2)));
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
