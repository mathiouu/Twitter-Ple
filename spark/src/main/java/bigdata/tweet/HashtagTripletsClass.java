package bigdata.tweet;
import java.io.Serializable;

public class HashtagTripletsClass implements Serializable {
    /**
	 *
	 */
	private static final long serialVersionUID = 5833895230384936558L;
	public String hashtag1;
    public String hashtag2;
    public String hashtag3;

    public HashtagTripletsClass(String hashtag1,String hashtag2,String hashtag3){
        this.hashtag1 = hashtag1;
        this.hashtag2 = hashtag2;
        this.hashtag3 = hashtag3;
    }

    @Override
    public String toString(){
        return String.format("{%s,%s,%s}",hashtag1,hashtag2,hashtag3);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this){
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) { 
            return false; 
        }
        HashtagTripletsClass hashtagObj = (HashtagTripletsClass) obj;
        return ((hashtagObj.hashtag1 == this.hashtag1) && (hashtagObj.hashtag2 == this.hashtag2)&& (hashtagObj.hashtag3 == this.hashtag3));

    }
    
}
