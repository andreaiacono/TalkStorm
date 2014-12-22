package first.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * A spout that emits a random value in [0,99] range.
 */
public class RandomSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private Random random;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("val"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        random = new Random();
    }

    @Override
    public void nextTuple() {
        spoutOutputCollector.emit(new Values(random.nextInt() % 100));
    }
}
