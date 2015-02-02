package tophashtagsmap.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * A bolt that counts the hashtags it receives
 */
public class CountHashtagsBolt extends BaseBasicBolt {

    private Map<String, Long> countMap = new HashMap<>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        String word = tuple.getString(0);
        Long val = countMap.get(word);
        if (val == null) {
            countMap.put(word, 1L);
        }
        else {
            countMap.put(word, ++val);
        }
        collector.emit(new Values(word, countMap.get(word)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hashtag", "count"));
    }

}
