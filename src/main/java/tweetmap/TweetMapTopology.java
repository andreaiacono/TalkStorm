package tweetmap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import tweetmap.bolt.ToRedisTweetBolt;
import tweetmap.spout.GeoTweetSpout;

public class TweetMapTopology {

    private static final String name = TweetMapTopology.class.getSimpleName();

    public static String GEO_TWEET_SPOUT = "geo-tweet-spout";
    public static String TO_REDIS_TWEET_BOLT = "to-redis-tweet-bolt";

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        GeoTweetSpout geoTweetSpout = new GeoTweetSpout("xxx", "xxx", "xxx", "xxx");
        builder.setSpout(GEO_TWEET_SPOUT, geoTweetSpout, 4);

        builder.setBolt(TO_REDIS_TWEET_BOLT, new ToRedisTweetBolt(), 4)
                .shuffleGrouping(GEO_TWEET_SPOUT);

        Config conf = new Config();
        conf.setDebug(false);
//        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(name, conf, builder.createTopology());
        Utils.sleep(30000000);
        cluster.killTopology(name);
        cluster.shutdown();
    }
}
