# Kafka Streams - Wikipedia Stats

This is a Kafka Streams based project that creates statistics using the wikipedia event-stream.

We create calculate the number of new pages  edited pages using multiple filters like timeframe filtering and filtering by user type (bot / human) for each language.

## Run - Docker

To run the project, clone this repo and run these commands:
```sh
docker-compose pull
docker-compose build
docker-compose up -d
```

Then, open http://localhost:3000 in your browser.

## Kafka and Zookeeper

Kafka and Zookeeper were run using docker containers pulled from the official images from dockerhub. Kafka was given multiple environment variables including one that specifies the topics we want to create, partitions, and a replication factor. In our use case, we created two topics:
- “recentChange” topic with one partition and a replication factor of one. This is used to store all the messages received from Wikipedia. It has a string key which is the URL/language of the message and a JSON value that contains the message itself.


- “output” topic with one partition and a replication factor of one. This is used to store the statistics and all the data that the stream outputs. It has a string key which is the URL/language of the message and a JSON value that contains relevant data and metadata about the type of stats that this message contains.


## Producer

A small python program that consumes the messages from the “recentChange” topic in Wikipedia, reorders and keeps only relevant data that will be used in the processing stream.

## Stream Workers

In order to consume these messages, we used the Kafka Streams framework in java. The stream was split to be able to process and create multiple stats. We used a combination of filters, key-value maps, and grouping techniques to reorganize the data for each specific use. Also, we introduced windowing in order to create stats across multiple timeframes (last hour, day, week, month). In some cases, we joined tables of different stats that have the same timeframe to have a more rich output of the stream. In the end, all messages created had metadata about the type of stats that each message contained which included the timeframe, language, bot/user filtering, and the data itself. finally, the messages were published to the “output” topic to be saved.

## DataViewer and Server

We decided to take the project a step beyond the scope of the project’s requirements and create a website that shows the stats produced by the stream in real-time. We used React to create a client which we called DataViewer. The logic is to consume the messages from Kafka, store them in the component’s state and render the new state with the updated stats. To be able to do that, we had to create a server that consumes these messages from Kafka and sends them to the client using Server-Sent-Events. The server was implemented using Flask and contains a Kafka-consumer object that subscribes to the Wikipedia “recentChange” topic, consumes the messages, and sends them to the client. In React, we connected to the server and when a new message arrives, it is stored in the state which triggers a re-render event on the component with the new data.
