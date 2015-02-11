package tophashtagsmap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import tophashtagsmap.bolt.*;
import tophashtagsmap.spout.GeoTweetSpout;

public class TopHashtagMapTopology {

    private static final String name = TopHashtagMapTopology.class.getSimpleName();
    private static int TOP_NUMBER = 20;

    public static String GEO_TWEET_SPOUT = "geo-tweet-spout";
    public static String NO_HASHTAG_DROPPER_BOLT = "no-ht-dropper-bolt";
    public static String COUNT_HASHTAGS_BOLT = "count-ht-bolt";
    public static String PARSE_HASHTAGS_BOLT = "parse-ht-bolt";
    public static String INTERMEDIATE_RANKING_BOLT = "intermediate-ranking-bolt";
    public static String TOTAL_RANKING_BOLT = "totale-ranking-bolt";
    public static String TO_REDIS_TOP_HASHTAGS_BOLT = "to-redis-top-ht-bolt";
    public static String GEO_HASHTAG_FILTER_BOLT = "geo-ht-filter-bolt";
    public static String TO_REDIS_TWEET_BOLT = "to-redis-tweet-bolt";

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        GeoTweetSpout geoTweetSpout = new GeoTweetSpout("xxx", "xxx", "xxx", "xxx");
        builder.setSpout(GEO_TWEET_SPOUT, geoTweetSpout, 4);

        builder.setBolt(NO_HASHTAG_DROPPER_BOLT, new NoHashtagDropperBolt(), 4)
                .shuffleGrouping(GEO_TWEET_SPOUT);

        builder.setBolt(PARSE_HASHTAGS_BOLT, new ParseTweetBolt(), 4)
                .shuffleGrouping(NO_HASHTAG_DROPPER_BOLT);

        builder.setBolt(COUNT_HASHTAGS_BOLT, new CountHashtagsBolt(), 4)
                .fieldsGrouping(PARSE_HASHTAGS_BOLT, new Fields("hashtag"));

        builder.setBolt(INTERMEDIATE_RANKING_BOLT, new IntermediateRankingsBolt(TOP_NUMBER), 4)
                .fieldsGrouping(COUNT_HASHTAGS_BOLT, new Fields("hashtag"));

        builder.setBolt(TOTAL_RANKING_BOLT, new TotalRankingsBolt(TOP_NUMBER), 1)
                .globalGrouping(INTERMEDIATE_RANKING_BOLT);

        builder.setBolt(TO_REDIS_TOP_HASHTAGS_BOLT, new ToRedisTopHashtagsBolt(), 1)
                .shuffleGrouping(TOTAL_RANKING_BOLT);

        builder.setBolt(GEO_HASHTAG_FILTER_BOLT, new GeoHashtagsFilterBolt(), 4)
                .shuffleGrouping(NO_HASHTAG_DROPPER_BOLT)
                .allGrouping(TOTAL_RANKING_BOLT);

        builder.setBolt(TO_REDIS_TWEET_BOLT, new ToRedisTweetBolt(), 4)
                .shuffleGrouping(GEO_HASHTAG_FILTER_BOLT);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(name, conf, builder.createTopology());
        Utils.sleep(30000000);
        cluster.killTopology(name);
        cluster.shutdown();
    }
}
