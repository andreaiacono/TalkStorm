package tophashtagsmap.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import tophashtagsmap.tools.Rankings;
import tophashtagsmap.tools.Rankable;

import java.util.Map;

/**
 * A bolt that publishes the top hashtags to redis
 */
public class ToRedisTopHashtagsBolt extends BaseBasicBolt {

    private transient RedisConnection<String, String> redis;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        RedisClient client = new RedisClient("localhost", 6379);
        redis = client.connect();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Rankings rankableList = (Rankings) tuple.getValue(0);
        StringBuilder builder = new StringBuilder("0");
        for (Rankable r : rankableList.getRankings()) {
            String hashtag = r.getObject().toString();
            Long count = r.getCount();
            builder.append("|").append(hashtag).append("|").append(count);
        }
        redis.publish("tophashtagsmap", builder.toString());
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nothing to declare: this is the final bolt
    }
}
