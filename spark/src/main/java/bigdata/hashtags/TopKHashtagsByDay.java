package bigdata.hashtags;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class TopKHashtagsByDay {

	public static FlatMapFunction<String,Tuple2<String,Integer>> getHashtags = new FlatMapFunction<String,Tuple2<String,Integer>>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<Tuple2<String,Integer>> call(String line) {
			List<Tuple2<String,Integer>> res = new ArrayList<Tuple2<String,Integer>>();
			Gson gson = new Gson();
			try {
				JsonObject tweet = gson.fromJson(line, JsonElement.class).getAsJsonObject();
				
				JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
				for (int i = 0; i < hashtags.size(); i++) {
					res.add(new Tuple2<String,Integer>(hashtags.get(i).getAsJsonObject().get("text").getAsString().toLowerCase(),1));
				}
				return res.iterator();
			}

			catch (Exception e) {
				return res.iterator();
			}
		}

		
	};
    public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <directory path> <day selected> ");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		Pattern p = Pattern.compile("(.*tweet_)(.+)(.nljson)");

		Matcher m = p.matcher(args[0]);
		String date;
		if (m.find()) {
			date = m.group(2);
		} else {
			date = "row";
		}
		JavaRDD<String> lines = context.textFile(args[0]);
		JavaRDD<Tuple2<String,Integer>> tweets = lines.flatMap(getHashtags) ;
		JavaPairRDD<String,Integer> hashtags = tweets.mapToPair(tweet->tweet).partitionBy(new HashPartitioner(30));
		hashtags = hashtags.reduceByKey((a,b)-> a+b,12);
		List<Tuple2<String,Integer>> sortedList = hashtags.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(1000);

		// hashtags.saveAsNewAPIHadoopFile("/users/smelezan/testSpark",Text.class,
		// Text.class,TextOutputFormat.class);
		// JavaRDD<String> example =
		// context.textFile("/users/smelezan/testSpark/part-r-00000", 4);
		createHBaseTable(sortedList, date, context);

		context.stop();
	}
	public static void createHBaseTable(List<Tuple2<String,Integer>> sortedRdd,String date ,JavaSparkContext context) {
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
			for (Tuple2<String,Integer> line : sortedRdd) {
				Put put = new Put(Bytes.toBytes(String.format("%s-%s",date,i)));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(String.format("%s",line._2())));
				put.addColumn(familyName, Bytes.toBytes("hashtags"), Bytes.toBytes(line._1()));
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
