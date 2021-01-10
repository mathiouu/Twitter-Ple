  
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

public class TopKHashtagsByDay {
    public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <directory path> <day selected> ");
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
        String tweetFile = getTweetFile(args[0], args[1]);

        Pattern p = Pattern.compile("(.*tweet_)(.+)(.nljson)");

        Matcher m = p.matcher(tweetFile);
        String date ;
        if(m.find()){
            date = m.group(2);
        }
        else{
            date = "row";
        }
        JavaRDD<String> lines = context.textFile(tweetFile, 4);
        JavaRDD<JsonObject> tweets = convertLinesToTweets(lines);
        JavaPairRDD<Tuple2<String,String>, Integer> hashtags = getHashtags(tweets,date);
        List<Tuple2<Tuple2<String,String>,Integer>> topK = getTopK(hashtags, 100);

		createHBaseTable(topK,context);
		
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

	public static JavaPairRDD<Tuple2<String,String>, Integer> getHashtags(JavaRDD<JsonObject> tweets,String date) {
		
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
		JavaPairRDD<Tuple2<String,String>,Integer> counts = hashtagsObj
		.mapToPair(hashtag -> new Tuple2<Tuple2<String,String>, Integer>(new Tuple2<String,String>(hashtag,date),1));
		JavaPairRDD<Tuple2<String,String>, Integer> freq = counts.reduceByKey((a, b) -> a + b);
		
		
		return freq;
	}
	public static List<Tuple2<Tuple2<String,String>,Integer>> getTopK(JavaPairRDD<Tuple2<String,String>,Integer> hashtags, int k){
		List<Tuple2<Tuple2<String,String>,Integer>> data = hashtags.takeOrdered(k, new NbHashtagComparator());
		return data;
	}

	public static void createHBaseTable( List<Tuple2<Tuple2<String,String>,Integer>> topK,JavaSparkContext context) {
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		String tableName = "seb-mat-tophashtagsbyday"; 
		byte[] familyName = Bytes.toBytes("hashtags");
		Connection connection = null;
		try {
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object.
			table = connection.getTable(TableName.valueOf(tableName));
			Integer i = 0;
			for (Tuple2<Tuple2<String,String>,Integer> line : topK) {
				Put put = new Put(Bytes.toBytes(line._1._2+"-"+i));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(String.format("%s",line._2)));
				put.addColumn(familyName, Bytes.toBytes("hashtag"), Bytes.toBytes(line._1._1));
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
