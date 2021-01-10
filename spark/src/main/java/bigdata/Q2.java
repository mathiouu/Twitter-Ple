package bigdata;

import java.io.IOException;
import java.util.ArrayList;
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

import bigdata.comparators.CountComparator;
import scala.Tuple2;

public class Q2 {
    public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		// String file1Path = "/raw_data/tweet_01_03_2020_first10000.nljson";
		String file1Path = "/raw_data/tweet_01_03_2020.nljson";
		JavaRDD<String> lines = context.textFile(file1Path, 4);

		JavaRDD<JsonObject> tweets = convertLinesToTweets(lines);
		
		// JavaPairRDD<String, String> usersHashtags = getUsersHashtags(tweets);
		// createHBaseTable(usersHashtags, context);

		JavaPairRDD<String, Integer> usersNbTweet = getUsersNbTweet(tweets);
		// createHBaseTable1(usersNbTweet, context);

		// 8103 de longueur


		// JavaPairRDD<String, Integer> countriesTweet = getTweetByCountry(tweets);
		// createHBaseTableCountries(countriesTweet, context);

		// JavaPairRDD<String, Integer> langsTweet = getTweetByLang(tweets);
		// createHBaseTableLangs(langsTweet, context);


		// TEST

		// List<JavaPairRDD<String, Integer>> listOfRdd = new ArrayList<JavaPairRDD<String, Integer>>();
		// int nbDaySelected = 1;
		// for(int i = 1; i <= nbDaySelected; i++){

		// 	String tweetFile = getTweetFile(args[0], Integer.toString(i));
			
		// 	JavaRDD<String> lines1 = context.textFile(tweetFile, 4);
		// 	JavaRDD<JsonObject> tweets1 = convertLinesToTweets(lines1);
		// 	JavaPairRDD<String, Integer> usersNbTweet1 = getUsersNbTweet(tweets1);

		// 	listOfRdd.add(usersNbTweet1);
		// }

		// JavaPairRDD<String, Integer> rdd = listOfRdd.get(0);
		// for(int i = 1; i < listOfRdd.size() ; i++){
		// 	rdd = rdd.union(listOfRdd.get(i)).distinct();
		// }

		ArrayList<String> columns = new ArrayList<String>();
		columns.add("user");
		columns.add("times");
		// fillHBaseTable (rdd, context, "testDuban1", Bytes.toBytes("userNbTweet"), columns);
		// fillHBaseTable (usersNbTweet, context, "testDuban1", Bytes.toBytes("userNbTweet"), columns);
		
		context.stop();
	}

	public static String getTweetFile(String directory, String dayInArg){
		
		int daySelected = Integer.parseInt(dayInArg);

		if(daySelected < 1 || daySelected > 21){
			System.err.println("Day are included between 1 and 21");
			System.exit(1);
		}
		
		String tweetStartFilePath = directory + "/tweet_";
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
				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>("unvalid user", 0);
				return tuple;
			}
		});

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

	public static void createHBaseTableCountries(JavaPairRDD<String, Integer> countries, JavaSparkContext context) {
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
			
			List<Tuple2<String,Integer>> data = countries.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(100);
			Integer i = 0;
			for (Tuple2<String,Integer> line : data) {
				Put put = new Put(Bytes.toBytes("row" + i));
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

	// public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
	// 	if (admin.tableExists(table.getTableName())) {
	// 		admin.disableTable(table.getTableName());
	// 		admin.deleteTable(table.getTableName());
	// 	}
	// 	admin.createTable(table);
	// }

	public static void fillHBaseTable (JavaPairRDD<String, Integer> rdd, JavaSparkContext context, String tableName, byte[] familyName, ArrayList<String> columns){
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		Connection connection = null;
		try {

			connection = ConnectionFactory.createConnection(hbConf);

			// final Admin admin = connection.getAdmin(); 
			// HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

			// HColumnDescriptor famLoc = new HColumnDescriptor(familyName); 
			// final byte[] TEST = Bytes.toBytes("0");
			// famLoc.setValue(TEST, familyName);
			// tableDescriptor.addFamily(famLoc);			
			// createOrOverwrite(admin, tableDescriptor);

			table = connection.getTable(TableName.valueOf(tableName));

			Long sizeRdd = rdd.count();
			int minSizeRdd = sizeRdd.intValue();

			List<Tuple2<String, Integer>> data = rdd.takeOrdered(minSizeRdd, new CountComparator());

			// List<Tuple2<String,Integer>> data = rdd.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(100);
			Integer i = 0;

			for (Tuple2<String, Integer> line : data) {

				Put put = new Put(Bytes.toBytes(String.valueOf(i)));
				put.addColumn(familyName, Bytes.toBytes(columns.get(0)), Bytes.toBytes(line._1));
				put.addColumn(familyName, Bytes.toBytes(columns.get(1)), Bytes.toBytes(String.format("%s", line._2)));
				i += 1;
				table.put(put);
			}
			// admin.close();
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
			
			Long sizeRdd = users.count();
			int minSizeRdd = sizeRdd.intValue();
			// List<Tuple2<String, Integer>> data = users.takeOrdered(minSizeRdd, new CountComparator());


			List<Tuple2<String,Integer>> data = users.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(100);
			Integer i = 0;

			for (Tuple2<String, Integer> line : data) {
				Put put = new Put(Bytes.toBytes("row" + i));
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
