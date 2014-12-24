package tophashtags;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import tophashtags.bolt.*;
import tophashtags.spout.ItalianTweetSpout;

class TopHashtagTopology {

    private static final String name = TopHashtagTopology.class.getSimpleName();
    private static int n = 10;

    public enum Spout {
        ITALIAN_TWEET_SPOUT
    }

    public enum Bolt {
        PARSE_TWEET_BOLT, COUNT_HASHTAGS_BOLT, INTERMEDIATE_RANKING_BOLT, TOTAL_RANKING_BOLT, TO_REDIS_BOLT
    }

    public static void main(String[] args) {

        ItalianTweetSpout italianTweetSpout = new ItalianTweetSpout("xxx", "xxx", "xxx", "xxx");

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(Spout.ITALIAN_TWEET_SPOUT.name(), italianTweetSpout, 1);
        builder.setBolt(Bolt.PARSE_TWEET_BOLT.name(), new ParseTweetBolt(), 10).shuffleGrouping(Spout.ITALIAN_TWEET_SPOUT.name());
        builder.setBolt(Bolt.COUNT_HASHTAGS_BOLT.name(), new CountBolt(), 15).fieldsGrouping(Bolt.PARSE_TWEET_BOLT.name(), new Fields("hashtag"));
        builder.setBolt(Bolt.INTERMEDIATE_RANKING_BOLT.name(), new IntermediateRankingsBolt(n), 4).fieldsGrouping(Bolt.COUNT_HASHTAGS_BOLT.name(), new Fields("word"));
        builder.setBolt(Bolt.TOTAL_RANKING_BOLT.name(), new TotalRankingsBolt(n)).globalGrouping(Bolt.INTERMEDIATE_RANKING_BOLT.name());
        builder.setBolt(Bolt.TO_REDIS_BOLT.name(), new ToRedisBolt(), 1).globalGrouping(Bolt.TOTAL_RANKING_BOLT.name());

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(name, conf, builder.createTopology());
        Utils.sleep(30000000);
        cluster.killTopology(name);
        cluster.shutdown();
    }
}
