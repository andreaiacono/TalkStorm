package tophashtagsmap.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tophashtagsmap.utils.MiscUtils;


import java.util.Map;
import java.util.Set;

/**
 * A bolt that drops tweets not containing hashtags
 */
public class NoHashtagDropperBolt extends BaseRichBolt {

    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        Set<String> hashtags = MiscUtils.getHashtags(tuple.getString(0));
        if (hashtags.size() == 0) {
            return;
        }
        collector.emit(new Values(tuple.getString(0), tuple.getString(1), tuple.getString(2)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "lat", "lon"));
    }

}