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
        @SuppressWarnings("unchecked")
        Map<String, Double> scores = (Map<String, Double>) input.getValueByField("scores");
        String topLabel = input.getStringByField("topLabel");
        double topScore = input.getDoubleByField("topScore");

        System.out.printf(">>> \"%s\" -> top=%s (%.3f) all=%s%n",
                message, topLabel, topScore, scores.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // no further output
    }
}