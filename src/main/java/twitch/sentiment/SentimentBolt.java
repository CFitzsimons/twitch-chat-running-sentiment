package twitch.sentiment;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SentimentBolt extends BaseRichBolt {

    private OutputCollector collector;

    private Set<String> positiveWords;
    private Set<String> negativeWords;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

        positiveWords = new HashSet<String>(Arrays.asList(
                "love", "awesome", "good", "great", "wholesome", "fun", "amazing"
        ));

        negativeWords = new HashSet<String>(Arrays.asList(
                "hate", "bad", "terrible", "boring", "awful"
        ));
    }

    @Override
    public void execute(Tuple input) {
        String message = input.getStringByField("message");
        String lower = message.toLowerCase();

        int score = 0;

        for (String word : positiveWords) {
            if (lower.contains(word)) {
                score++;
            }
        }

        for (String word : negativeWords) {
            if (lower.contains(word)) {
                score--;
            }
        }

        String label;
        if (score > 0) {
            label = "positive";
        } else if (score < 0) {
            label = "negative";
        } else {
            label = "neutral";
        }

        collector.emit(new Values(message, label, score));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message", "twitch/sentiment", "score"));
    }
}