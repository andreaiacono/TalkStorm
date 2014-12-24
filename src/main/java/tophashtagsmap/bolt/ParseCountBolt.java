package tophashtagsmap.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tophashtagsmap.utils.MiscUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * A bolt that parses and counts the hashtags it receives
 */
public class ParseCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private Map<String, Long> countMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        countMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {

        String tweet = tuple.getString(0);
        MiscUtils.getHashtags(tweet).stream().forEach(h -> updateAndEmitHashtagCount(h));
    }

    /**
     * updates the map with counts of every hashtag; after doing that,
     * it emits the hashtag and its count to the next bolt
     *
     * @param hashtag
     */
    private void updateAndEmitHashtagCount(String hashtag) {

        // check if the hashtag is present in the map
        if (countMap.get(hashtag) == null) {

            // not present, add the word with a count of 1
            countMap.put(hashtag, 1L);
        }
        else {

            // already there, hence get the count
            Long val = countMap.get(hashtag);

            // increment the count and save it to the map
            countMap.put(hashtag, ++val);
        }

        // emit the word and count
        collector.emit(new Values(hashtag, countMap.get(hashtag)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hashtag", "count"));
    }
}
