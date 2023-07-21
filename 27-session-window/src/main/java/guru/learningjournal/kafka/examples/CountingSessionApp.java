package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.UserClicks;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Properties;


public class CountingSessionApp {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreName);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, UserClicks> source = streamsBuilder.stream(AppConfigs.topicName,
            Consumed.with(AppSerdes.String(), AppSerdes.UserClicks())
                .withTimestampExtractor(new AppTimestampExtractor())
        );

       // The window can be long as 1 ms or 1 hour , it is not normalized to epoch time.
        KTable<Windowed<String>, Long> dest = source.groupByKey(Grouped.with(AppSerdes.String(), AppSerdes.UserClicks()))
                        .windowedBy(SessionWindows.with(Duration.ofMinutes(5)))// This is the inactive time
                                .count();

        dest.toStream().foreach(
                (key, value) -> logger.info (
                        "Store ID " + key.key() + " Window ID " + key.window().hashCode() +
                                "Window start " + Instant.ofEpochMilli(key.window().start()).atOffset(ZoneOffset.UTC) +
                                "Window end  "  + Instant.ofEpochMilli(key.window().end()).atOffset(ZoneOffset.UTC) +
                                "Count " + value

                 )
        );



        logger.info("Starting Stream...");
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams...");
            streams.close();
        }));

    }
}
