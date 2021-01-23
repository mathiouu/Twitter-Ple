package bigdata.influencers;

import java.io.Serializable;

public class InfluencerClass implements Serializable {
    public int nbFollowers;
    public int nbRetweets;

    public InfluencerClass(int nbFollowers, int nbRetweets) {
        this.nbFollowers = nbFollowers;
        this.nbRetweets = nbRetweets;
    }

    public InfluencerClass add(InfluencerClass obj) {
        return new InfluencerClass(Math.max(nbFollowers, obj.nbFollowers), nbRetweets + obj.nbRetweets);
    }

    public InfluencerClass reduce(int count) {
        return new InfluencerClass(nbFollowers, nbRetweets / count);
    }

    @Override
    public String toString() {
        return String.format("nbFollowers : %s , nbRetweets: %s ", nbFollowers, nbRetweets);
    }
}
