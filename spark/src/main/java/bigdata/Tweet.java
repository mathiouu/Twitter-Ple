package bigdata;

import java.util.ArrayList;

public class Tweet {

    public class TweetEntity {
        public class TweetHashtags{
            public String text;
        }
        public ArrayList<TweetHashtags> hashtags;
        public ArrayList<String> urls;
        public ArrayList<String> user_mentions;
        public ArrayList<String> symbols;


    }

    public class TweetUser {
        public String id_str;
        public int id;
        public String name;

    }

    public String created_at;
    public double id;
    public String text;
    public TweetEntity entities;
    public TweetUser user;


    public String toString(){
        return String.format("Tweet is %s with id %s",this.text, this.id);
    }

}
