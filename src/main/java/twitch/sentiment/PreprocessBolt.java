package twitch.sentiment;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.Normalizer;
import java.util.Map;

public class PreprocessBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String raw = input.getStringByField("message");

        // 1) Unicode normalize (e.g. fancy quotes, accents composed)
        String normalized = Normalizer.normalize(raw, Normalizer.Form.NFKC);

        // 2) Remove control chars + zero-width stuff
        normalized = normalized.replaceAll("\\p{C}", "");

        // 3) Replace weird whitespace with a normal space
        normalized = normalized.replaceAll("\\s+", " ").trim();

        // 4) Optionally strip other “odd” chars – keep letters, digits, punctuation, spaces
        //    (tune this to taste; you might want to preserve emoji for future use)
        String cleaned = normalized.replaceAll("[^\\p{L}\\p{N}\\p{P}\\p{Z}]", "");

        // Emit with the SAME field name that SentimentBolt expects: "message"
        collector.emit(new Values(cleaned));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // keep the field name "message" so downstream bolts don't need to change
        declarer.declare(new Fields("message"));
    }
}