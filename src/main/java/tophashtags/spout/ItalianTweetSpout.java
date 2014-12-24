package tophashtags.spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import tophashtags.utils.TwitterListener;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A spout that uses Twitter streaming API for continuously
 * getting tweets
 */
public class ItalianTweetSpout extends BaseRichSpout {

    // Twitter API authentication credentials
    String custkey, custsecret, accesstoken, accesssecret;

    // To output tuples from spout to the next stage bolt
    SpoutOutputCollector spoutOutputCollector;

    // Twitter4j - twitter stream to get tweets
    TwitterStream twitterStream;

    // Shared queue for getting buffering tweets received
    LinkedBlockingQueue<String> queue = null;

    /**
     * Constructor for tweet spout that accepts the credentials
     */
    public ItalianTweetSpout(String key, String secret, String token, String tokenSecret) {
        custkey = key;
        custsecret = secret;
        accesstoken = token;
        accesssecret = tokenSecret;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.spoutOutputCollector = spoutOutputCollector;

        queue = new LinkedBlockingQueue<>(1000);

        // sets Twitter4j auth info
        ConfigurationBuilder config = new ConfigurationBuilder().setOAuthConsumerKey(custkey).setOAuthConsumerSecret(custsecret).setOAuthAccessToken(accesstoken).setOAuthAccessTokenSecret(accesssecret);
        TwitterStreamFactory streamFactory = new TwitterStreamFactory(config.build());

        // gets the twitter stream
        twitterStream = streamFactory.getInstance();
        twitterStream.addListener(new TwitterListener(queue));
        FilterQuery filterQuery = new FilterQuery();
        filterQuery.language(new String[] {"it"});
        double[][] boundingBox = {{-179, -89}, {179, 89}};
        filterQuery.locations(boundingBox);
        twitterStream.filter(filterQuery);
    }

    @Override
    public void nextTuple() {

        // gets the tweet from the queue (must be non-blocking!)
        String ret = queue.poll();

        // if no tweet is available, wait for 50 ms and return
        if (ret == null) {
            Utils.sleep(50);
            return;
        }

        // now emit the tweet to next stage bolt
        spoutOutputCollector.emit(new Values(ret));
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {

        // set the parallelism for this spout to be 1
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }
}
