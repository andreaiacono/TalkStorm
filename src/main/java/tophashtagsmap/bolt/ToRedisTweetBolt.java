package tophashtagsmap.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: andrea
 * Date: 24/12/14
 * Time: 15.13
 */
public class ToRedisTweetBolt  extends BaseRichBolt {

    transient RedisConnection<String, String> redis;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        RedisClient client = new RedisClient("localhost", 6379);
        redis = client.connect();
    }

    @Override
    public void execute(Tuple tuple) {

        // gets the tweet and its rank (based on hashtags contained in the tweet)
        String lat = tuple.getString(0);
        String lon = tuple.getString(1);
        String hashtag  = tuple.getString(2);
        String tweet = tuple.getString(3);
        String message = "1|" + lat + "|" + lon + "|" + hashtag + "|" + tweet;
        redis.publish("tophashtagsmap", message);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
