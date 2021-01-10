package bigdata.comparators;

import java.io.Serializable;
import java.util.Comparator;
import scala.Tuple2;

public class NbHashtagComparatorByDay implements Comparator<Tuple2<Tuple2<String,String>,Integer>>, Serializable{

    @Override 
    public int compare(Tuple2<Tuple2<String,String>,Integer> hashtag1, Tuple2<Tuple2<String,String>,Integer> hashtag2){
        return hashtag2._2 - hashtag1._2;
    }
}