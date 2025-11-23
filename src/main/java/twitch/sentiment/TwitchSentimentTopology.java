package twitch.sentiment;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class TwitchSentimentTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitch-spout", new TwitchMessageSpout());

        // NEW: preprocess before sentiment
        builder.setBolt("preprocess-bolt", new PreprocessBolt())
                .shuffleGrouping("twitch-spout");

        builder.setBolt("sentiment-bolt", new SentimentBolt())
                .shuffleGrouping("preprocess-bolt");

        builder.setBolt("print-bolt", new PrintBolt())
                .shuffleGrouping("sentiment-bolt");

        Config config = new Config();
        config.setDebug(true);

        if (args != null && args.length > 0) {
            // Cluster mode: submit to Nimbus
            config.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            // Local mode: run inside IntelliJ
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("twitch-sentiment-topology", config, builder.createTopology());

            // run for a minute then stop
            Thread.sleep(Long.MAX_VALUE);

            cluster.killTopology("twitch-sentiment-topology");
            cluster.shutdown();
        }
    }
}