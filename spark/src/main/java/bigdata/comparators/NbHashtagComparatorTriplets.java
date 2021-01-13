package bigdata.comparators;

import java.io.Serializable;
import java.util.Comparator;
import scala.Tuple2;
import scala.Tuple3;

public class NbHashtagComparatorTriplets implements Comparator<Tuple2<Tuple3<String,String,String>,Tuple2<ArrayList<String>,Integer>>>, Serializable{

    @Override 
    public int compare(Tuple2<Tuple3<String,String,String>,Tuple2<ArrayList<String>,Integer>> hashtag1, Tuple2<Tuple3<String,String,String>,Tuple2<ArrayList<String>,Integer>> hashtag2){
        return hashtag2._2._2 - hashtag1._2._2;
    }
}