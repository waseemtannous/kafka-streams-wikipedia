package org.example;

import com.fasterxml.jackson.databind.node.ArrayNode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import org.apache.log4j.BasicConfigurator;
import org.example.PriorityQueue.*;

import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.time.Duration;
import java.util.*;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;

// create jar file: mvn clean compile assembly:single
//run: mvn exec:java@StreamWorker

public class StreamWorkers {

    private static final Logger logger = Logger.getLogger(StreamWorkers.class.getName());

    public static final String AppID = "Kafka-Wikipedia-Stats";

    public static String BootstrapServers;

    public static final String InputTopic = "recentChange";

    public static final String outputTopic = "output";

    /**
     * these arrays contain KTables starting from the smallest time window to the
     * larges time window
     **/

    public static final ArrayList<KTable<Windowed<String>, Long>> newPageTablesBots = new ArrayList<>();
    public static final ArrayList<KTable<Windowed<String>, Long>> newPageTables = new ArrayList<>();

    public static final ArrayList<KTable<Windowed<String>, Long>> editPageTablesBots = new ArrayList<>();
    public static final ArrayList<KTable<Windowed<String>, Long>> editPageTables = new ArrayList<>();

    public static final ArrayList<KTable<Windowed<String>, JsonNode>> newPageTablesJoined = new ArrayList<>();
    public static final ArrayList<KTable<Windowed<String>, JsonNode>> editPageTablesJoined = new ArrayList<>();

    public static final ArrayList<TimeWindows> timeWindows = new ArrayList<>();

    public static KStream<String, JsonNode> kStream;

    static int[] timeWindowsValues = { 3600, 86400, 604800, 2629743 };

    static String[] timeStamps = {"hour", "day", "week", "month"};
    static int topN = 5;

    public static Properties getProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);

        // instant commit
//        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // commit every 100ms
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        return props;
    }

    public static void initDurations() {
        for (int timeWindow : timeWindowsValues) {
            timeWindows.add(TimeWindows.of(Duration.ofSeconds(timeWindow)));
        }
    }

    public static void initTables(ArrayList<KTable<Windowed<String>, Long>> tables, String type, boolean isBot) {
        for (TimeWindows timeWindow : timeWindows) {
            KTable<Windowed<String>, Long> newPageTable = kStream
                    .filter((key, value) -> Objects.equals(value.get("eventType").asText(), type)
                            && (value.get("bot").asBoolean() == isBot))
                    .groupByKey()
                    .windowedBy(timeWindow)
                    .count();
            tables.add(newPageTable);
        }
    }

    public static void joinTablesBotsUsers(ArrayList<KTable<Windowed<String>, Long>> PageTables,
                                           ArrayList<KTable<Windowed<String>, Long>> PageTablesBots,
                                           ArrayList<KTable<Windowed<String>, JsonNode>> PageTablesJoined,
                                           String type) {
        String[] timeFrames = { "hour", "day", "week", "month" };
        for (int i = 0; i < PageTables.size(); i++) {
            KTable<Windowed<String>, Long> PageTable = PageTables.get(i);
            KTable<Windowed<String>, Long> PageTableBot = PageTablesBots.get(i);

            // left: PageTable, right: PageTableBot
            int finalI = i;
            KTable<Windowed<String>, JsonNode> joined = PageTableBot.outerJoin(PageTable, (left, right) -> {
                ObjectNode jsonNode = new ObjectMapper().createObjectNode();
                ObjectNode meta = new ObjectMapper().createObjectNode();
                meta.put("type", "stats");
                meta.put("timeFrame", timeFrames[finalI]);

                jsonNode.set("meta", meta);

                long leftValue = left == null ? 0 : left;
                long rightValue = right == null ? 0 : right;
                jsonNode.put("user", leftValue);
                jsonNode.put("bot", rightValue);

                if (leftValue <= 0 && rightValue <= 0) {
                    jsonNode.put("userRelative", 0);
                    jsonNode.put("botRelative", 0);
                }
                else {
                    jsonNode.put("userRelative", ((double) leftValue / (double) (leftValue + rightValue) * 100.0f));
                    jsonNode.put("botRelative", ((double) rightValue / (double) (leftValue + rightValue) * 100.0f));
                }

                jsonNode.put("type", type);

                return jsonNode;
            });
            joined.toStream()
                    .map((wk, v) -> new KeyValue<>(wk.key(), v))
                    .to(outputTopic, Produced.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())));
            PageTablesJoined.add(joined);
        }
    }

    /**
     * reference
     * https://github.com/confluentinc/kafka-streams-examples/blob/7.1.1-post/src/main/java/io/confluent/examples/streams/TopArticlesLambdaExample.java
     */
    public static void topFiveUsersBots(boolean isBot) {
        for (int j = 0; j < timeWindows.size(); j++) {
            int finalJ = j;
            TimeWindows timeWindow = timeWindows.get(j);

            final Comparator<JsonNode> comparator =
                    (o1, o2) -> (int) (o2.get("count").asLong() - o1.get("count").asLong());

            KTable<String, JsonNode> top5 = kStream
                    .filter((k, v) -> v.get("bot").asBoolean() == isBot)
                    .map((k, v) -> {
                        String user = v.get("user").asText();
                        String serverName = v.get("serverName").asText();
                        return new KeyValue<>(user + "---" + serverName, v);
                    })
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())))
                    .windowedBy(timeWindow)
                    .count()
                    .groupBy((windowedUser, count) -> {
                        ObjectNode jsonNode = new ObjectMapper().createObjectNode();
                        String key = windowedUser.key();
                        String user = key.split("---")[0];
                        String serverName = key.split("---")[1];
                        jsonNode.put("user", user);
                        jsonNode.put("count", count);
                        jsonNode.put("serverName", serverName);
                        return new KeyValue<>(serverName + "----" + windowedUser.window(), jsonNode);

                    }, Grouped.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())))
                    .aggregate(// the initializer
                            () -> new PriorityQueue<>(comparator),

                            // the "add" aggregator
                            (key, record, queue) -> {
                                queue.add(record);
                                return queue;
                            },

                            // the "remove" aggregator
                            (key, record, queue) -> {
                                queue.remove(record);
                                return queue;
                            },

                            Materialized.with(Serdes.String(), new PriorityQueueSerde<>(comparator, Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())))
                    )
                    .mapValues(queue -> {
                        ObjectNode jsonNode = new ObjectMapper().createObjectNode();
                        List<ObjectNode> array = new ArrayList<>();
                        String serverName = null;
                        for (int i = 0; i < topN; i++) {
                            final JsonNode record = queue.poll();
                            if (record == null) {
                                break;
                            }
                            ObjectNode userNode = new ObjectMapper().createObjectNode();

                            userNode.put("user", record.get("user").asText());
                            userNode.put("count", record.get("count").asLong());
                            array.add(userNode);

                            if (serverName == null) {
                                serverName = record.get("serverName").asText();
                            }
                        }
                        ObjectNode meta = new ObjectMapper().createObjectNode();
                        meta.put("type", "top");
                        meta.put("timeFrame", timeStamps[finalJ]);
                        meta.put("bot", isBot);
                        meta.put("serverName", serverName);

                        ArrayNode arrayNode = new ObjectMapper().valueToTree(array);

                        jsonNode.set("meta", meta);
                        jsonNode.putArray("topUser").addAll(arrayNode);
                        return jsonNode;
                    });

            top5.toStream()
                    .map((k, v) -> {
                        String newKey = k.split("----")[0];
                        return new KeyValue<>(newKey, v);
                    })
                    .to(outputTopic, Produced.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())));
        }
    }

    public static void topFivePages() {
        for (int j = 0; j < timeWindows.size(); j++) {
            int finalJ = j;
            TimeWindows timeWindow = timeWindows.get(j);

            final Comparator<JsonNode> comparator =
                    (o1, o2) -> (int) (o2.get("count").asLong() - o1.get("count").asLong());

            KTable<String, JsonNode> top5 = kStream
                    .map((k, v) -> {
                        String page = v.get("page").asText();
                        String serverName = v.get("serverName").asText();
                        return new KeyValue<>(page + "---" + serverName, v);
                    })
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())))
                    .windowedBy(timeWindow)
                    .count()
                    .groupBy((windowedPage, count) -> {
                        ObjectNode jsonNode = new ObjectMapper().createObjectNode();
                        String key = windowedPage.key();
                        String page = key.split("---")[0];
                        String serverName = key.split("---")[1];
                        jsonNode.put("page", page);
                        jsonNode.put("count", count);
                        jsonNode.put("serverName", serverName);
                        return new KeyValue<>(serverName + "----" + windowedPage.window(), jsonNode);
                    }, Grouped.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())))
                    .aggregate(// the initializer
                            () -> new PriorityQueue<>(comparator),

                            // the "add" aggregator
                            (key, record, queue) -> {
                                queue.add(record);
                                return queue;
                            },

                            // the "remove" aggregator
                            (key, record, queue) -> {
                                queue.remove(record);
                                return queue;
                            },

                            Materialized.with(Serdes.String(), new PriorityQueueSerde<>(comparator, Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())))
                    )
                    .mapValues(queue -> {
                        ObjectNode jsonNode = new ObjectMapper().createObjectNode();
                        List<ObjectNode> array = new ArrayList<>();
                        String serverName = null;
                        for (int i = 0; i < topN; i++) {
                            final JsonNode record = queue.poll();
                            if (record == null) {
                                break;
                            }
                            ObjectNode userNode = new ObjectMapper().createObjectNode();

                            userNode.put("page", record.get("page").asText());
                            userNode.put("count", record.get("count").asLong());
                            array.add(userNode);

                            if (serverName == null) {
                                serverName = record.get("serverName").asText();
                            }
                        }
                        ObjectNode meta = new ObjectMapper().createObjectNode();
                        meta.put("type", "topPage");
                        meta.put("timeFrame", timeStamps[finalJ]);
                        meta.put("serverName", serverName);

                        ArrayNode arrayNode = new ObjectMapper().valueToTree(array);

                        jsonNode.set("meta", meta);
                        jsonNode.putArray("topPage").addAll(arrayNode);
                        return jsonNode;
                    });

            top5.toStream()
                    .map((k, v) -> {
                        String newKey = k.split("----")[0];
                        return new KeyValue<>(newKey, v);
                    })
                    .to(outputTopic, Produced.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())));
        }
    }

    public static void main(String[] args) {

//        sleep 10 sec
        try {
            Thread.sleep(10000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        BootstrapServers = System.getenv("BOOTSTRAP_SERVER");

//        BasicConfigurator.configure();

        logger.info("consumer started ...");

        // create properties
        Properties props = getProps();

        // create streams builder
        Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        Serializer<JsonNode> jsonSerializer = new JsonSerializer();

        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        // create stream
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        kStream = streamsBuilder.stream(InputTopic, Consumed.with(Serdes.String(), jsonSerde));

        initDurations();

        initTables(newPageTables, "new", false);
        initTables(newPageTablesBots, "new", true);
        initTables(editPageTables, "edit", false);
        initTables(editPageTablesBots, "edit", true);

//         join tables
         joinTablesBotsUsers(newPageTables, newPageTablesBots, newPageTablesJoined, "new");
         joinTablesBotsUsers(editPageTables, editPageTablesBots, editPageTablesJoined, "edit");

        topFiveUsersBots(true);
        topFiveUsersBots(false);

        topFivePages();


        // create topology and start stream
        Topology topology = streamsBuilder.build();
        KafkaStreams stream = new KafkaStreams(topology, props);
        logger.info("started stream ...");
        stream.start();

        // wait for termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("shutting down stream ...");
            stream.close();
        }));
    }

}