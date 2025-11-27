package twitch.sentiment;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class SentimentBolt extends BaseRichBolt {

    private OutputCollector collector;
    private transient HttpClient httpClient;
    private transient Gson gson;

    private static final String SENTIMENT_URL = "http://flask-service:5500/classify";

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map topoConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.gson = new Gson();
    }

    @Override
    public void execute(Tuple input) {
        String channel = input.getStringByField("channel");
        String originalMessage = input.getStringByField("original_message");
        String message = input.getStringByField("cleaned_message");

        try {
            // Build JSON request body: { "text": "<message>" }
            Map<String, String> body = new HashMap<>();
            body.put("text", message);
            String jsonBody = gson.toJson(body);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(SENTIMENT_URL))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                collector.reportError(new RuntimeException(
                        "Sentiment API returned status " + response.statusCode()
                                + " body: " + response.body()));
                collector.fail(input);
                return;
            }

            // Expecting: { "result": { "neutral": 0.94, "approval": 0.03, ... } }
            JsonObject root = JsonParser.parseString(response.body()).getAsJsonObject();
            JsonObject resultObj = root.getAsJsonObject("result");
            if (resultObj == null || !resultObj.isJsonObject()) {
                collector.reportError(new RuntimeException("Invalid sentiment response: " + response.body()));
                collector.fail(input);
                return;
            }

            Map<String, Double> scores = new HashMap<>();
            String topLabel = null;
            double topScore = Double.NEGATIVE_INFINITY;

            for (Map.Entry<String, JsonElement> entry : resultObj.entrySet()) {
                String label = entry.getKey();
                double score = entry.getValue().getAsDouble();

                scores.put(label, score);

                if (score > topScore) {
                    topScore = score;
                    topLabel = label;
                }
            }

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
        declarer.declare(new Fields(
                "channel",
                "original_message",
                "cleaned_message",
                "scores",
                "topLabel",
                "topScore"
        ));
    }
}