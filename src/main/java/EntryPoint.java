import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by rubbal on 23/1/17.
 */
public class EntryPoint {
    public static void main(String[] args) throws InterruptedException {
        // Add a uid to avoid bloating of output in subsequent runs
        String runId = UUID.randomUUID().toString();

        // Server configuration
        String zookeepers = "localhost:2181";
        String kafkaBrokers = "localhost:9092";

        // Create topics if they do not exist
        String topicVisits = "visits";
        String topicClicks = "clicks";
        String topicUserClicks = "userClicks";

        TopicCreator.createTopic(topicVisits, 3, zookeepers);
        TopicCreator.createTopic(topicClicks, 3, zookeepers);
        TopicCreator.createTopic(topicUserClicks, 3, zookeepers);

        ProducerExample.produceStrings(
                new ArrayList<Tuple2<String, String>>() {{
                    add(new Tuple2<>(runId + "visit1", runId + "uuid1"));
                    add(new Tuple2<>(runId + "visit2", runId + "uuid1"));
                    add(new Tuple2<>(runId + "visit3", runId + "uuid2"));
                    add(new Tuple2<>(runId + "visit6", runId + "uuid4"));
                }}
                , topicVisits
                , kafkaBrokers
        );

        ProducerExample.produceStrings(
                new ArrayList<Tuple2<String, String>>() {{
                    add(new Tuple2<>(runId + "visit1", "1"));
                    add(new Tuple2<>(runId + "visit1", "1"));
                    add(new Tuple2<>(runId + "visit3", "1"));
                    add(new Tuple2<>(runId + "visit4", "1"));
                }}
                , topicClicks
                , kafkaBrokers
        );

        System.out.println("Test consumption");
        ConsumerExample.consume(zookeepers, "visitConsumer", topicVisits);
        ConsumerExample.consume(zookeepers, "clickConsumer", topicClicks);
        System.out.println("Finished consumption");
        /*
          Topology
         */
        Properties streamProcessorConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamProcessorConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-click");
        // Where to find Kafka broker(s).
        streamProcessorConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamProcessorConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Specify default (de)serializers for record keys and for record values.
        streamProcessorConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamProcessorConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> visits = builder.stream("visits");
        KStream<String, String> clicks = builder.stream("clicks");

        KTable<String, String> userClicks = clicks.join(visits,
                // Join the values. (visit, user) join (visit, count) -> visit -> (user, count)
                (c, v) -> c + "," + v,
                // Window for join
                JoinWindows.of(1000000))
                // Transform visit -> user, count to user -> count
                .map((key, value) -> {
                    String[] tokens = value.split(",");
                    String userId = tokens[1];
                    String userVisits = tokens[0];
                    return new KeyValue<>(userId, userVisits);
                })
                // Group by key
                .groupByKey()
                // Specify the reduction function
                .reduce((v1, v2) -> String.valueOf(Integer.parseInt(v1) + Integer.parseInt(v2)), "mystore2");

        // Write to the topic. Usually, aggregations should not happen in streams for critical counts (such as billing
        // events as kafka provides at least once processing
        userClicks.to(topicUserClicks);

        // Build the topology and start processing
        KafkaStreams streams = new KafkaStreams(builder, streamProcessorConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
