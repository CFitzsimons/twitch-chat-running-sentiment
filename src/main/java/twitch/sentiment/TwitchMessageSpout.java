package twitch.sentiment;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitchMessageSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private String twitchHost = "irc.chat.twitch.tv";
    private int twitchPort = 6667;

    private String username;
    private String oauthToken;
    private String channel;

    public TwitchMessageSpout(String channel) {
        this.channel = channel;
    }

    private transient Socket socket;
    private transient BufferedReader reader;
    private transient BufferedWriter writer;

    private final BlockingQueue<ChatMessage> messageQueue = new LinkedBlockingQueue<>(1000);
    private transient Thread readerThread;
    private volatile boolean running = true;

    private static class ChatMessage {
        final String channel;
        final String message;
        ChatMessage(String channel, String message) {
            this.channel = channel;
            this.message = message;
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        // Read configuration
        this.username = (String) conf.get("twitch.username");
        this.oauthToken = (String) conf.get("twitch.oauthToken");

        if (username == null || oauthToken == null || channel == null) {
            throw new RuntimeException("Twitch config missing: twitch.username, twitch.oauthToken, twitch.channel are all required");
        }

        // Start background reader thread
        readerThread = new Thread(this::runReaderLoop, "twitch-reader-thread");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    private void runReaderLoop() {
        while (running) {
            try {
                connectAndJoin();

                String line;
                while (running && (line = reader.readLine()) != null) {
                    if (line.startsWith("PING")) {
                        sendRaw(line.replace("PING", "PONG"));
                        continue;
                    }

                    if (line.contains(" PRIVMSG ")) {
                        ChatMessage msg = parsePrivMsg(line);
                        if (msg != null) {
                            // Non-blocking: drop oldest if full
                            if (!messageQueue.offer(msg)) {
                                messageQueue.poll();
                                messageQueue.offer(msg);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // Log and retry after a short sleep
                e.printStackTrace();
                closeConnectionQuietly();
                sleepQuietly(3000);
            }
        }

        closeConnectionQuietly();
    }

    private void connectAndJoin() throws IOException {
        closeConnectionQuietly();

        socket = new Socket(twitchHost, twitchPort);
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
        writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));

        sendRaw("PASS " + oauthToken);
        sendRaw("NICK " + username);
        sendRaw("CAP REQ :twitch.tv/tags twitch.tv/commands twitch.tv/membership");
        sendRaw("JOIN #" + channel.toLowerCase());

        // You could wait for a JOIN confirmation here if you want to be fancy.
        System.out.println("Connected to Twitch IRC channel #" + channel);
    }

    private void sendRaw(String line) throws IOException {
        writer.write(line + "\r\n");
        writer.flush();
    }

    private ChatMessage parsePrivMsg(String line) {
        int privmsgIndex = line.indexOf(" PRIVMSG ");
        if (privmsgIndex == -1) {
            return null;
        }

        int channelStart = privmsgIndex + " PRIVMSG ".length();
        int channelEnd = line.indexOf(' ', channelStart);
        if (channelEnd == -1) {
            return null;
        }
        String channelToken = line.substring(channelStart, channelEnd);
        String channelName = channelToken.startsWith("#")
                ? channelToken.substring(1)
                : channelToken;
        int firstColon = line.indexOf(':');
        if (firstColon == -1) {
            return null;
        }
        int secondColon = line.indexOf(':', firstColon + 1);
        if (secondColon == -1 || secondColon + 1 >= line.length()) {
            return null;
        }

        String messageText = line.substring(secondColon + 1);
        return new ChatMessage(channelName, messageText);
    }

    private void closeConnectionQuietly() {
        try {
            if (reader != null) reader.close();
        } catch (IOException ignored) {}
        try {
            if (writer != null) writer.close();
        } catch (IOException ignored) {}
        try {
            if (socket != null && !socket.isClosed()) socket.close();
        } catch (IOException ignored) {}
        reader = null;
        writer = null;
        socket = null;
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void nextTuple() {
        ChatMessage msg = messageQueue.poll();
        if (msg != null) {
            collector.emit(new Values(msg.channel, msg.message));
        } else {
            sleepQuietly(10);
        }
    }

    @Override
    public void close() {
        running = false;
        closeConnectionQuietly();
        if (readerThread != null) {
            readerThread.interrupt();
        }
    }

    @Override
    public void ack(Object msgId) {
        // Nop
    }

    @Override
    public void fail(Object msgId) {
        // nop
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("channel", "original_message"));
    }
}
