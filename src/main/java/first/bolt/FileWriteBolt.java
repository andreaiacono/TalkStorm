package first.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * A bolt that write the received values on a file.
 */
public class FileWriteBolt extends BaseBasicBolt {

    private final String filename = "output.txt";
    private BufferedWriter writer = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        try {
            writer = new BufferedWriter(new FileWriter(filename, true));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            writer.write(tuple.getInteger(0) + "\n");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
        try {
            writer.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
