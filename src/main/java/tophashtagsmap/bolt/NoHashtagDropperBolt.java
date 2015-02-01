package tophashtagsmap.bolt;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tophashtagsmap.utils.MiscUtils;

import java.util.Set;

/**
 * A bolt that drops tweets not containing hashtags
 */
public class NoHashtagDropperBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "lat", "lon"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Set<String> hashtags = MiscUtils.getHashtags(tuple.getString(0));
        if (hashtags.size() == 0) {
            return;
        }
        String tweet = tuple.getString(0);
        String lat = tuple.getString(1);
        String lon = tuple.getString(2);

        collector.emit(new Values(tweet, lat, lon));
    }
}