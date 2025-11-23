package twitch.sentiment;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class PrintBolt extends BaseRichBolt {

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
        // no-op
    }

    @Override
    public void execute(Tuple input) {
        String message = input.getStringByField("message");
        String sentiment = input.getStringByField("twitch/sentiment");
        int score = input.getIntegerByField("score");

        System.out.printf("Twitch message: \"%s\" | sentiment=%s | score=%d%n",
                message, sentiment, score);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // no further output
    }
}