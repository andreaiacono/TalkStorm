package javamap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import javamap.bolt.ToRedisTweetBolt;
import javamap.spout.JavaGeoTweetSpout;

public class JavaMapTopology {

    private static final String name = JavaMapTopology.class.getSimpleName();

    public static String JAVA_GEO_TWEET_SPOUT = "geo-tweet-spout";
    public static String TO_REDIS_TWEET_BOLT = "to-redis-tweet-bolt";

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        JavaGeoTweetSpout javaGeoTweetSpout = new JavaGeoTweetSpout("xxx", "xxx", "xxx", "xxx");
        builder.setSpout(JAVA_GEO_TWEET_SPOUT, javaGeoTweetSpout, 4);

        builder.setBolt(TO_REDIS_TWEET_BOLT, new ToRedisTweetBolt(), 4)
                .shuffleGrouping(JAVA_GEO_TWEET_SPOUT);

        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(name, conf, builder.createTopology());
        Utils.sleep(30000000);
        cluster.killTopology(name);
        cluster.shutdown();
    }
}
