package twitch.sentiment;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

public class SentimentBolt extends BaseRichBolt {

    private OutputCollector collector;
    private transient HttpClient httpClient;
    private transient ObjectMapper objectMapper;

    // Change to localhost if running in local mode for testing, should probably
    // be in an env file.
    private static final String SENTIMENT_URL = "http://localhost:5500/classify";

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void execute(Tuple input) {
        String channel = input.getStringByField("channel");
        String originalMessage = input.getStringByField("original_message");
        String message = input.getStringByField("cleaned_message");

        try {
            Map<String, String> body = new HashMap<>();
            body.put("text", message);
            String jsonBody = objectMapper.writeValueAsString(body);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(SENTIMENT_URL))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                // Log and fail the tuple so Storm can retry/backpressure
                collector.reportError(new RuntimeException("Sentiment API returned status " + response.statusCode()
                        + " body: " + response.body()));
                collector.fail(input);
                return;
            }
            JsonNode root = objectMapper.readTree(response.body());
            JsonNode resultNode = root.path("result");
            if (resultNode.isMissingNode() || !resultNode.isObject()) {
                collector.reportError(new RuntimeException("Invalid sentiment response: " + response.body()));
                collector.fail(input);
                return;
            }

            Map<String, Double> scores = new HashMap<>();
            String topLabel = null;
            double topScore = Double.NEGATIVE_INFINITY;

            Iterator<Map.Entry<String, JsonNode>> fields = resultNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String label = entry.getKey();
                double score = entry.getValue().asDouble();

                scores.put(label, score);

                if (score > topScore) {
                    topScore = score;
                    topLabel = label;
                }
            }

            // Emit:
            //  - twitch channel
            //  - original message
            //  - cleaned message
            //  - full map of label->score
            //  - top label according to the model
            //  - top score
            collector.emit(new Values(
                    channel,
                    originalMessage,
                    message,
                    scores,
                    topLabel,
                    topScore
            ));
            collector.ack(input);

        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("channel", "original_message", "cleaned_message", "scores", "topLabel", "topScore"));
    }
}