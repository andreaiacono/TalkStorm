package tophashtagsmap.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tophashtagsmap.utils.MiscUtils;

/**
 * A bolt that parses the tweet and emits its hashtags
 */
public class ParseBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String tweet = tuple.getString(0);
        MiscUtils.getHashtags(tweet).stream().forEach(ht -> collector.emit(new Values(ht)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hashtag"));
    }
}
