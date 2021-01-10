package bigdata.comparators;

import java.io.Serializable;
import java.util.Comparator;
import scala.Tuple2;

public class NbHashtagComparator implements Comparator<Tuple2<String,Integer>>, Serializable{

    @Override 
    public int compare(Tuple2<String,Integer> hashtag1, Tuple2<String,Integer> hashtag2){
        return hashtag2._2 - hashtag1._2;
    }
}