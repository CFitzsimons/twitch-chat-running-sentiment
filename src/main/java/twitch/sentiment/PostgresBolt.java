package twitch.sentiment;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class PostgresBolt extends BaseRichBolt {

    private OutputCollector collector;
    private Connection connection;
    private PreparedStatement insertStmt;
    private ObjectMapper objectMapper;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.objectMapper = new ObjectMapper();

        try {
            Class.forName("org.postgresql.Driver");

            // You can override these via topology config if you like
            String url = (String) stormConf.getOrDefault(
                    "pg.url",
                    // Update to localhost if running in local mode
                    "jdbc:postgresql://localhost:5432/twitch"
            );
            String user = (String) stormConf.getOrDefault("pg.user", "twitch");
            String password = (String) stormConf.getOrDefault("pg.password", "twitch");

            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(true);

            // matches message_sentiment schema
            String sql =
                    "INSERT INTO message_sentiment " +
                            "(channel, original_message, cleaned_message, top_label, top_score, scores) " +
                            "VALUES (?, ?, ?, ?, ?, ?::jsonb)";

            insertStmt = connection.prepareStatement(sql);

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize PostgresSinkBolt", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple input) {
        try {
            String channel = input.getStringByField("channel");
            String originalMessage = input.getStringByField("original_message");
            String cleanedMessage = input.getStringByField("cleaned_message");
            String topLabel = input.getStringByField("topLabel");
            double topScore = input.getDoubleByField("topScore");

            Map<String, Double> scores =
                    (Map<String, Double>) input.getValueByField("scores");

            String scoresJson = objectMapper.writeValueAsString(scores);

            insertStmt.setString(1, channel);
            insertStmt.setString(2, originalMessage);
            insertStmt.setString(3, cleanedMessage);
            insertStmt.setString(4, topLabel);
            insertStmt.setDouble(5, topScore);
            insertStmt.setString(6, scoresJson);

            insertStmt.executeUpdate();

            collector.ack(input);
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(input); // Storm can retry the tuple; may cause duplicates
        }
    }

    @Override
    public void cleanup() {
        try {
            if (insertStmt != null) {
                insertStmt.close();
            }
        } catch (SQLException ignored) {}

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ignored) {}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // sink bolt: no downstream fields
    }
}