package bigdata.influencers;

import java.io.Serializable;

public class EngagementClass implements Serializable{
    public int nbRetweets;
    public int nbFavs;
    public int nbUrls;
    public int mentionned;


    public EngagementClass(int nbRetweets, int nbFavs, int nbUrls, int mentionned){
        this.mentionned= mentionned;
        this.nbUrls = nbUrls;
        this.nbFavs = nbFavs;
        this.nbRetweets = nbRetweets;
    }

    public EngagementClass add(EngagementClass obj){
        return new EngagementClass(
            mentionned + obj.mentionned, 
            nbUrls+ obj.nbUrls,
            Math.max(nbFavs, obj.nbFavs),
            nbRetweets + obj.nbRetweets);
    }
    
    public EngagementClass average(int count){
        return new EngagementClass(
            mentionned/count, 
            nbUrls/count,
            nbFavs/count,
            nbRetweets /count);
    }

    @Override
    public String toString(){
        return String.format("mentionned: %s , nbUrls: %s , nbFavs: %s, nbRetweets: %s", mentionned, nbUrls, nbFavs, nbRetweets);
    }
}