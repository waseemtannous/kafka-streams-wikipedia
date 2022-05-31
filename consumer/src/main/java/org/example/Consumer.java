package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javafx.util.Pair;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import org.json.simple.JSONObject;

public class Consumer {

    private static final Logger logger = Logger.getLogger(Consumer.class.getName());

    public static final String AppID = "Kafka-Wikipedia-Stats";

    public static final String BootstrapServers = "localhost:9092";

    public static final String InputTopic = "recentChange";

    public static final String outputTopic = "output";

    /**
     these arrays contain KTables starting from the smallest time window to the larges time window
     **/

    public static final ArrayList<KTable<Windowed<String>, Long>> newPageTablesBots = new ArrayList<>();
    public static final ArrayList<KTable<Windowed<String>, Long>> newPageTables = new ArrayList<>();

    public static final ArrayList<KTable<Windowed<String>, Long>> editPageTablesBots = new ArrayList<>();
    public static final ArrayList<KTable<Windowed<String>, Long>> editPageTables = new ArrayList<>();

    public static final ArrayList<KTable<Windowed<String>, String>> newPageTablesJoined = new ArrayList<>();
    public static final ArrayList<KTable<Windowed<String>, String>> editPageTablesJoined = new ArrayList<>();

    public static final ArrayList<TimeWindows> timeWindows = new ArrayList<>();

    public static KStream<String, JsonNode> kStream;



    public static Properties getProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

//        instant commit
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

//        commit every 1000ms
//        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        return props;
    }

    public static void initDurations() {
        int[] timeWindowsValues = {5, 10, 15, 20};
        for (int timeWindow : timeWindowsValues) {
            timeWindows.add(TimeWindows.of(Duration.ofSeconds(timeWindow)));
        }
    }

    public static void initTables(ArrayList<KTable<Windowed<String>, Long>> tables, String type, boolean isBot) {
        for (TimeWindows timeWindow : timeWindows) {
            KTable<Windowed<String>, Long> newPageTable = kStream.filter((key, value) -> Objects.equals(value.get("eventType").asText(), type) && (value.get("bot").asBoolean() == isBot))
                    .groupByKey()
                    .windowedBy(timeWindow)
                    .count();
            tables.add(newPageTable);
        }
    }

    public static void joinTables(ArrayList<KTable<Windowed<String>, Long>> PageTables, ArrayList<KTable<Windowed<String>, Long>> PageTablesBots, ArrayList<KTable<Windowed<String>, String>> PageTablesJoined) {
        for (int i = 0; i < PageTables.size(); i++) {
            KTable<Windowed<String>, Long> PageTable = PageTables.get(i);
            KTable<Windowed<String>, Long> PageTableBot = PageTablesBots.get(i);

//          left: editPageTable, right: editPageTableBot
            KTable<Windowed<String>, String> joined = PageTableBot.outerJoin(PageTable, (left, right) -> {
                ObjectNode jsonNode = new ObjectMapper().createObjectNode();
                long leftValue = left == null ? 0 : left;
                long rightValue = right == null ? 0 : right;
                jsonNode.put("user", leftValue);
                jsonNode.put("bot", rightValue);
                return jsonNode.toString();
            });
            PageTablesJoined.add(joined);
        }
    }

    public static void windowsJoinTables(){
        KTable<Windowed<String>, String> joined1 = editPageTablesJoined.get(0).join(editPageTablesJoined.get(1), (left, right) -> {
            ObjectNode jsonNode = new ObjectMapper().createObjectNode();
            jsonNode.put("hour", left);
            jsonNode.put("day", right);
            return jsonNode.toString();
        });

        KTable<Windowed<String>, String> joined2 = editPageTablesJoined.get(2).join(editPageTablesJoined.get(3), (left, right) -> {
            ObjectNode jsonNode = new ObjectMapper().createObjectNode();
            jsonNode.put("week", left);
            jsonNode.put("month", right);
            return jsonNode.toString();
        });

        KTable<Windowed<String>, String> joined = joined1.join(joined2, (left, right) -> {
            ObjectNode jsonNode = new ObjectMapper().createObjectNode();

            ObjectMapper mapper = new ObjectMapper();
            JsonNode leftObj = null;
            JsonNode rightObj = null;
            try {
                leftObj = mapper.readTree(left);
                rightObj = mapper.readTree(right);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            if (leftObj == null) {
                jsonNode.put("hour", "{\"user\":0,\"bot\":0}");
                jsonNode.put("day", "{\"user\":0,\"bot\":0}");
            }
            else {
                jsonNode.put("hour", leftObj.get("hour"));
                jsonNode.put("day", leftObj.get("day"));
            }

            if (rightObj == null) {
                jsonNode.put("week", "{\"user\":0,\"bot\":0}");
                jsonNode.put("month", "{\"user\":0,\"bot\":0}");
            }
            else {
                jsonNode.put("week", rightObj.get("week"));
                jsonNode.put("month", rightObj.get("month"));
            }

            return jsonNode.toString();
        });

        joined.toStream()
                .map((wk, v) -> new KeyValue<>(wk.key(), v))
                .peek((k, v) -> logger.info("key: " + k + " value: " + v))
                .to(outputTopic);
    }

    public static void main(String[] args) {

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

//        join tables
//        joinTables(newPageTables, newPageTablesBots, newPageTablesJoined);
//        joinTables(editPageTables, editPageTablesBots, editPageTablesJoined);
//
//        windowsJoinTables();

//        newPageTablesJoined.get(0).toStream()
//                .map((wk, v) -> new KeyValue<>(wk.key(), v))
//                .peek((k, v) -> logger.info("key: " + k + " value: " + v))
//                .to(outputTopic);

//        KTable<Windowed<String>, Long> newPageTable =
//                kStream.filter((key, value) -> Objects.equals(value.get("eventType").asText(), "new") && !value.get("bot").asBoolean())
//                .groupByKey()
//                .windowedBy(timeWindows.get(0))
//                .count();
//        newPageTables.add(newPageTable);
//
//        newPageTable.toStream()
//                .map((wk, v) -> new KeyValue<>(wk.key(), v))
//                .to(outputTopic);


//        KTable<Windowed<String>, Long> count1 =
//            kStream.filter((key, value) -> Objects.equals(value.get("serverName").asText(), "en.wikipedia.org"))
//                    .peek((key, value) -> logger.info("key: " + key + " value: " + value))
//                    .groupByKey()
//                    .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
//                    .count();
//
//        count1.toStream()
//                .map((wk, v) -> new KeyValue<>(wk.key(), v))
//                .peek((key, value) -> logger.info("key: " + key + " value: " + value))
//                .to("output");
//
//        KTable<Windowed<String>, Long> count2 =
//                kStream.filter((key, value) -> Objects.equals(value.get("serverName").asText(), "en.wikipedia.org"))
//                        .groupByKey()
//                        .windowedBy(TimeWindows.of(Duration.ofSeconds(30)))
//                        .count();
//
//        count2.toStream()
//                .map((wk, v) -> new KeyValue<>(wk.key(), v))
//                .peek((key, value) -> logger.info("key: " + key + " value: " + value))
//                .to("output2");


        // create topology and start stream
        Topology topology = streamsBuilder.build();
        KafkaStreams stream = new KafkaStreams(topology, props);
        logger.info("started stream ...");
        stream.start();

        // wait for termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("shutting down stream ...");
            stream.close();
        }
        ));
    }


}