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
    private static int n = 10;

    public enum Spout {
        GEO_TWEET_SPOUT
    }

    public enum Bolt {
        NO_HASHTAG_DROPPER_BOLT, PARSE_COUNT_HASHTAGS_BOLT, INTERMEDIATE_RANKING_BOLT, TOTAL_RANKING_BOLT, GEO_HASHTAG_FILTER_BOLT, TO_REDIS_TWEET_BOLT, TO_REDIS_TOP_HASHTAGS_BOLT
    }

    public static void main(String[] args) {

        GeoTweetSpout geoTweetSpout = new GeoTweetSpout("xxx", "xxx", "xxx", "xxx");

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(Spout.GEO_TWEET_SPOUT.name(), geoTweetSpout, 1);

        builder.setBolt(Bolt.NO_HASHTAG_DROPPER_BOLT.name(), new NoHashtagDropperBolt(), 15).shuffleGrouping(Spout.GEO_TWEET_SPOUT.name());
        builder.setBolt(Bolt.PARSE_COUNT_HASHTAGS_BOLT.name(), new ParseCountBolt(), 15).fieldsGrouping(Bolt.NO_HASHTAG_DROPPER_BOLT.name(), new Fields("tweet"));
        builder.setBolt(Bolt.INTERMEDIATE_RANKING_BOLT.name(), new IntermediateRankingsBolt(n), 4).fieldsGrouping(Bolt.PARSE_COUNT_HASHTAGS_BOLT.name(), new Fields("hashtag"));
        builder.setBolt(Bolt.TOTAL_RANKING_BOLT.name(), new TotalRankingsBolt(n)).globalGrouping(Bolt.INTERMEDIATE_RANKING_BOLT.name());
        builder.setBolt(Bolt.TO_REDIS_TOP_HASHTAGS_BOLT.name(), new ToRedisTopHashtagsBolt(), 1).globalGrouping(Bolt.TOTAL_RANKING_BOLT.name());

        builder.setBolt(Bolt.GEO_HASHTAG_FILTER_BOLT.name(), new GeoHashtagsFilterBolt(), 1).shuffleGrouping(Bolt.NO_HASHTAG_DROPPER_BOLT.name()).shuffleGrouping(Bolt.TOTAL_RANKING_BOLT.name());
        builder.setBolt(Bolt.TO_REDIS_TWEET_BOLT.name(), new ToRedisTweetBolt(), 1).globalGrouping(Bolt.GEO_HASHTAG_FILTER_BOLT.name());

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
