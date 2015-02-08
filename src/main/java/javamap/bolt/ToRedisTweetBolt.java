package javamap.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
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
public class ToRedisTweetBolt  extends BaseBasicBolt {

    private transient RedisConnection<String, String> redis;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        RedisClient client = new RedisClient("localhost", 6379);
        redis = client.connect();
    }


    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // gets the tweet and its rank (based on hashtags contained in the tweet)
        String lat = tuple.getString(0);
        String lon = tuple.getString(1);
        String tweet = tuple.getString(2);
        String message = lat + "|" + lon + "|" + tweet;
        redis.publish("java", message);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
