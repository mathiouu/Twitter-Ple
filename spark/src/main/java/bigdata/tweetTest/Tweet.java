package bigdata.tweetTest;


public class Tweet {
    public String created_at;
    public double id;
    public String text;
    public Entity entities;
    public User user;

    public String toString(){
        return String.format("Tweet is %s with id %s", text, id);
    }
}
