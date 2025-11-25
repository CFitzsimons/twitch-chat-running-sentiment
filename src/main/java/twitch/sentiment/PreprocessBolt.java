package twitch.sentiment;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.Normalizer;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PreprocessBolt extends BaseRichBolt {

    private OutputCollector collector;
    private static final Map<String, String> EMOTE_MAP = new HashMap<>();
    private static final Pattern EMOTE_PATTERN;
    // This isn't going to catch everything, but it's a good start
    // realistically we probably want a more sophisticated expansion.
    static {
        // Positive / hype
        EMOTE_MAP.put("pogchamp", "hype");
        EMOTE_MAP.put("pog", "hype");
        EMOTE_MAP.put("pogu", "hype");
        EMOTE_MAP.put("poggers", "hype");
        EMOTE_MAP.put("hypers", "hype");
        EMOTE_MAP.put("hype", "hype");

        // Laugh / funny
        EMOTE_MAP.put("lul", "funny");
        EMOTE_MAP.put("lulw", "funny");
        EMOTE_MAP.put("omegalul", "funny");
        EMOTE_MAP.put("kekw", "funny");
        EMOTE_MAP.put("4head", "funny");
        EMOTE_MAP.put("kappa", "funny");
        EMOTE_MAP.put("kappapride", "funny");

        // Sad
        EMOTE_MAP.put("biblethump", "sad");
        EMOTE_MAP.put("feelsbadman", "sad");
        EMOTE_MAP.put("feelssadman", "sad");
        EMOTE_MAP.put("sadge", "sad");
        EMOTE_MAP.put("pepehands", "sad");

        // Bored / tired
        EMOTE_MAP.put("residentsleeper", "boring");

        // Angry
        EMOTE_MAP.put("madge", "angry");
        EMOTE_MAP.put("angery", "angry");

        StringBuilder sb = new StringBuilder();
        sb.append("\\b(");
        boolean first = true;
        // zip through the map and add them all to a pattern
        for (String emote : EMOTE_MAP.keySet()) {
            if (!first) {
                sb.append("|");
            }
            sb.append(Pattern.quote(emote));
            first = false;
        }
        sb.append(")\\b");
        EMOTE_PATTERN = Pattern.compile(sb.toString(), Pattern.CASE_INSENSITIVE);
    }
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String channel = input.getStringByField("channel");
        String originalMessage = input.getStringByField("original_message");
        String raw = input.getStringByField("original_message");
        // Encode emotes as simple emotions
        String emoteReplaced = replaceEmotes(raw);

        // Normalise
        String normalized = Normalizer.normalize(emoteReplaced, Normalizer.Form.NFKC);

        // Remove whitespace/contorl chars
        normalized = normalized.replaceAll("\\p{C}", "");
        normalized = normalized.replaceAll("\\s+", " ").trim();
        // Extras that tripped me up as I was testing
        String cleaned = normalized.replaceAll("[^\\p{L}\\p{N}\\p{P}\\p{Z}]", "");

        collector.emit(new Values(channel, originalMessage, cleaned));
        collector.ack(input);
    }

    private String replaceEmotes(String text) {
        Matcher m = EMOTE_PATTERN.matcher(text);
        var sb = new StringBuilder();
        while (m.find()) {
            String emoteMatched = m.group(1);
            String key = emoteMatched.toLowerCase(Locale.ENGLISH);
            String replacement = EMOTE_MAP.get(key);
            // don't jsut sub directly in, pop a space in
            m.appendReplacement(sb, " " + Matcher.quoteReplacement(replacement) + " ");
        }
        m.appendTail(sb);
        return sb.toString();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("channel", "original_message", "cleaned_message"));
    }
}