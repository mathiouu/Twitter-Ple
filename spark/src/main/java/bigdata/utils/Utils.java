package bigdata.utils;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.comparators.CountComparator;
import scala.Tuple2;

public class Utils {

    public static Writable toWritable(ArrayList<String> list){
        Writable[] content = new Writable[list.size()];
        for (int i = 0; i < content.length; i++) {
            content[i] = new Text(list.get(i));
        }
        return new ArrayWritable(Text.class, content);
    }
    
    public static ArrayList<String> fromWritable(ArrayWritable writable) {
        Writable[] writables = ((ArrayWritable) writable).get();
        ArrayList<String> list = new ArrayList<String>(writables.length);
        for (Writable wrt : writables) {
            list.add(((Text)wrt).toString());
        }
        return list;
    }

    public static void fillHBaseTable(JavaPairRDD<String, Integer> rdd, JavaSparkContext context, String tableName, byte[] familyName, ArrayList<String> columns){
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
			// List<Tuple2<String, Integer>> data = rdd.collect();

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
		// JavaRDD<JsonObject> tweets = lines.map(line-> {
		// 	Gson gson = new Gson();
		// 	return gson.fromJson(line, JsonElement.class).getAsJsonObject();
		// });
		// return tweets;
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
    
}
