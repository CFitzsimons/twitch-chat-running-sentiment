package twitch.sentiment;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class TwitchSentimentTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("channel-1", new TwitchMessageSpout("cdawg"));
        builder.setSpout("channel-2", new TwitchMessageSpout("ironmouse"));
        builder.setSpout("channel-3", new TwitchMessageSpout("piratesoftware"));
        builder.setSpout("channel-4", new TwitchMessageSpout("pestily"));

        builder.setBolt("preprocess-bolt", new PreprocessBolt())
                .shuffleGrouping("channel-1")
                .shuffleGrouping("channel-2")
                .shuffleGrouping("channel-3")
                .shuffleGrouping("channel-4");

        builder.setBolt("sentiment-bolt", new SentimentBolt())
                .shuffleGrouping("preprocess-bolt");

        builder.setBolt("db-sink-bolt", new PostgresBolt(), 1)
                .shuffleGrouping("sentiment-bolt");

        Config config = new Config();
        config.setDebug(true);

        config.put("twitch.username", "Yulfy");
        config.put("twitch.oauthToken", "oauth:REPLACE_TOKEN");
        config.setDebug(true);

        if (args != null && args.length > 0) {
            config.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("twitch-sentiment-topology", config, builder.createTopology());

            Thread.sleep(Long.MAX_VALUE);

            cluster.killTopology("twitch-sentiment-topology");
            cluster.shutdown();
        }
    }
}