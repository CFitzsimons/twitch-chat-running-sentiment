package twitch.sentiment;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class TwitchMessageSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    String[] messyMessages = {
            // Unicode variants / fancy text
            "𝖙𝖍𝖎𝖘 𝖌𝖆𝖒𝖊 𝖎𝖘 𝖆𝖜𝖊𝖘𝖔𝖒𝖊!!!",
            "ＴＨＩＳ　ＢＯＳＳ　ＩＳ　ＴＥＲＲＩＢＬＥ",
            "thís strëâm îs sô gòòd",
            "ｔｈｉｓ ｉｓ ｓｏ ｂｏｒｉｎｇ",

            // Zero-width spaces / invisible chars
            "h​e​l​l​o this message has zero width spaces",
            "you wont believe​ this boss",
            "this​is​almost​invisible",

            // Emoji / Twitch emotes (your cleaner strips these)
            "this fight is amazing 😂😂",
            "PogChamp that was good 😎",
            "LUL this is awful 🤡",

            // Non-Latin letters (will be removed with current regex)
            "これはひどい！",
            "이건 너무 좋다",
            "это просто ужасно",

            // Control characters / weird escapes
            "good luck\u0007 have fun",
            "this is so\u0000 weird",
            "awful boss fight\r\nso bad",

            // Spam characters / noise
            "this#####is#####wild!!!",
            "boring..................",
            "this is $$$crazy$$$",
            "!!!!! i hate this !!!!!",

            // Mixed cases your preprocessor should simplify
            "wholesome\t\tchat here",
            "   lots   of   weird      spacing   ",
            "weird\u200bwhitespace\u200bhere"
    };

    private int index = 0;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        String channel = "TestChannel";
        String msg = messyMessages[index];
        index = (index + 1) % messyMessages.length;

        collector.emit(new Values(channel, msg));

        // slow it down a bit
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("channel", "original_message"));
    }
}