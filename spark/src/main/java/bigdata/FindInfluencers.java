package bigdata;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;
import java.util.regex.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import bigdata.comparators.*;
import bigdata.tweet.*;

public class FindInfluencers {
    public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <directory path> <day selected> ");
			System.exit(1);
		}
		
		// SparkConf conf = new SparkConf().setAppName("TP Spark");
		// JavaSparkContext context = new JavaSparkContext(conf);
        // Configuration conf = HBaseConfiguration.create();
        // HTable hTable = new HTable(conf);
        // Get get = new Get(toBytes("row1"));
        // Result result = hTable.get(get);
        // byte [] value = result.getValue(Bytes.toBytes("hashtags"),Bytes.toBytes("hashtag"));
        // byte [] value1 = result.getValue(Bytes.toBytes("hashtags"),Bytes.toBytes("users"));
        // System.out.println(Bytes.toString(value));
		// context.stop();
	}

	

	
}
