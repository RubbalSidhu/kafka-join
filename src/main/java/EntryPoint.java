import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by rubbal on 23/1/17.
 */
public class EntryPoint {
    public static void main(String[] args) throws InterruptedException {
        String zookeepers = "localhost:2181";
        String kafkaBrokers = "localhost:9092";

        String topicName = "join_test";

        TopicCreator.createTopic(topicName, 3, zookeepers);
        ProducerExample.produce(
                new ArrayList<Tuple2<String, String>>() {{
                    add(new Tuple2<String, String>("uuid1", "10"));
                    add(new Tuple2<String, String>("uuid2", "20"));
                    add(new Tuple2<String, String>("uuid3", "30"));
                    add(new Tuple2<String, String>("uuid4", "40"));
                }}
                , topicName
                , kafkaBrokers
        );
        ConsumerExample.consume(zookeepers, "testConsumer", topicName);
    }
}
