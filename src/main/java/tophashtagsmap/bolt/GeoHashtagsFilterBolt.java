package tophashtagsmap.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tophashtagsmap.TopHashtagMapTopology;
import tophashtagsmap.tools.Rankable;
import tophashtagsmap.tools.Rankings;
import tophashtagsmap.utils.MiscUtils;

import java.util.Set;


public class GeoHashtagsFilterBolt extends BaseBasicBolt {

    private Rankings rankings;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet", "lat", "lon", "hashtag"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String componentId = tuple.getSourceComponent();

        // if the message comes from the NoHashtagDropper
        if (TopHashtagMapTopology.NO_HASHTAG_DROPPER_BOLT.equals(componentId)) {
            String tweet = tuple.getString(0);
            Set<String> hashtags = MiscUtils.getHashtags(tweet);
            for (String hashtag : hashtags) {
                for (Rankable r : rankings.getRankings()) {
                    String rankedHashtag = r.getObject().toString();
                    if (hashtag.equals(rankedHashtag)) {
                        String lat = tuple.getString(1);
                        String lon = tuple.getString(2);
                        collector.emit(new Values(lat, lon, hashtag, tweet));
                        return;
                    }
                }
            }
        }
        // if the message comes from the TotalRanker
        else if (TopHashtagMapTopology.TOTAL_RANKING_BOLT.equals(componentId)) {

            // TODO: send only if it is changed
            rankings = (Rankings) tuple.getValue(0);
        }
    }
}