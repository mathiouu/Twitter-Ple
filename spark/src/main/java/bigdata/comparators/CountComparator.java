package bigdata.comparators;

import java.io.Serializable;
import java.util.Comparator;
import scala.Tuple2;

public class CountComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
    
    @Override
    public int compare(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2){
        return tuple2._2() - tuple1._2();
    }
}
