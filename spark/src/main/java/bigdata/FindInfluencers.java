package bigdata;

import java.util.List;
import java.util.ArrayList;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import bigdata.tweet.*;

public class FindInfluencers {
    public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <directory path> <day selected> ");
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
        Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		
		Table table = null;
		String tableName = "testSmelezan"; 
        byte[] familyName = Bytes.toBytes("hashtags");
        try{
            Connection connection = null;
            connection = ConnectionFactory.createConnection(hbConf);
            table = connection.getTable(TableName.valueOf(tableName));
            List<Get> rowList = new ArrayList<Get>();
            for(int i = 0 ; i< 1000; i+=1){
                Get get = new Get(Bytes.toBytes("row"+i));
                rowList.add(get);
            }
            Result[] result = table.get(rowList);
            List<String> userList = new ArrayList<String>();

            for(Result r : result){
                byte[] usersByte = r.getValue(Bytes.toBytes("hashtags"),Bytes.toBytes("users"));
                userList.add(Bytes.toString(usersByte));
            }

            JavaRDD<String> userRdd = context.parallelize(userList);
            JavaRDD<Tuple2<String,Integer>> newUserRDD = userRdd.flatMap(user-> {
                ArrayList<Tuple2<String,Integer>> res = new ArrayList<Tuple2<String,Integer>>();
                Gson gson = new Gson();
                User[] userObjList = gson.fromJson(user, User[].class);
                for(int i = 0 ; i< userObjList.length; i++){
                    res.add(new Tuple2<String,Integer>(userObjList[i].screen_name,userObjList[i].followers_count));
                }
                return res.iterator();
            });
            
            JavaPairRDD<String,Integer> userPairRDD = newUserRDD.mapToPair(user->user).reduceByKey((a,b)->{
                return (a>b)? a:b;
            });
            System.out.println(userPairRDD.count());
        }
        catch(Exception e){
            System.out.println(e);
        }
		context.stop();
	}

	

	
}
