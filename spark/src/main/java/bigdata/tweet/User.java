package bigdata.tweet;

import java.io.Serializable;

public class User implements Serializable{
    public String screen_name;
    public int followers_count;
    public int friends_count;
    
    public User(String screen_name, int followers_count, int friends_count){
        this.screen_name = screen_name;
        this.followers_count = followers_count;
        this.friends_count = friends_count;
    }
    @Override
    public String toString(){
        return this.screen_name;
    }

    @Override
    public boolean equals(Object obj){
        if(obj == this){
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) { 
            return false; 
        }
        User user = (User) obj;
        return user.screen_name == this.screen_name;

    }

}
