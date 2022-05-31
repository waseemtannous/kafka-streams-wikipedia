package org.example;

import javafx.application.Platform;
import javafx.scene.layout.*;
import javafx.scene.control.Label;

import javafx.scene.paint.Color;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.logging.Logger;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

public class Consumer {

    private final String topic;
    private static final Logger logger = Logger.getLogger(Producer.class.getName());

    private KafkaStreams stream;

    private Label label;

    Consumer(String topic) {
        this.topic = topic;
//        this.topic = topic.substring(1, topic.length() - 2);
        System.out.println(topic);
    }

    public void createStream() {

        logger.info(topic + " consumer started ...");

        // create properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.AppID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create stream
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<Integer, String> kStream = streamsBuilder.stream(this.topic);
        kStream.foreach((key, value) -> {
            JSONParser parser = new JSONParser();
            try {
                JSONObject jsonObject = (JSONObject) parser.parse(value);
                logger.info("key: " + key + " value: " + jsonObject.get("page_id"));

            } catch(ParseException e) {
                e.printStackTrace();
            }
//                    Platform.runLater(() -> {
//                        logger.info("key: " + key + " value: " + value.substring(1, topic.length() - 2));
//                        this.label.setText(value);
//                        logger.info(value.getClass().getName());
//                    });
         }
        );

        // create topology and start stream
        Topology topology = streamsBuilder.build();
        this.stream = new KafkaStreams(topology, props);
        logger.info("started stream ...");
        this.stream.start();

        // wait for termination
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            logger.info("shutting down stream ...");
//            this.stream.close();
//        }
//        ));
    }

    public VBox build() {
        VBox vbox = new VBox();
        this.label = new Label("0");
        Label topicLabel = new Label("Topic: " + this.topic);
        vbox.getChildren().addAll(this.label, topicLabel);
        this.createStream();
        logger.info("consumer built ...");
        Border border = new Border(new BorderStroke(Color.valueOf("#9E9E9E"), BorderStrokeStyle.SOLID, CornerRadii.EMPTY, BorderWidths.DEFAULT));
        vbox.setBorder(border);
        return vbox;
    }

    public void close() {
        logger.info("shutting down stream ...");
//        this.stream.close();
    }
}