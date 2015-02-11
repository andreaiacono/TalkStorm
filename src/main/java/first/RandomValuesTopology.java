package first;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import first.bolt.FileWriteBolt;
import first.spout.RandomSpout;

/**
 * Created with IntelliJ IDEA.
 * User: andrea
 * Date: 22/12/14
 * Time: 18.12
 */
public class RandomValuesTopology {

    private static final String name = RandomValuesTopology.class.getName();

    public enum Spout {
        RANDOM_SPOUT
    }

    public enum Bolt {
        FILE_WRITER_BOLT
    }

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(Spout.RANDOM_SPOUT.name(), new RandomSpout(), 1);
        builder.setBolt(Bolt.FILE_WRITER_BOLT.name(), new FileWriteBolt(), 10).shuffleGrouping(Spout.RANDOM_SPOUT.name());

        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(name, conf, builder.createTopology());
        Utils.sleep(30000000);
        cluster.killTopology(name);
        cluster.shutdown();

//        // to run it on a live cluster
//        conf.setNumWorkers(3);
//        StormSubmitter.submitTopology("topology-name", conf, builder.createTopology());
    }
}
