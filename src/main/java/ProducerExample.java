import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.Tuple2;

import java.util.List;
import java.util.Properties;

/**
 * Created by rubbal on 23/1/17.
 */
public class ProducerExample {
    private ProducerExample() {

    }

    public static void produce(List<Tuple2<String, String>> messages, String topicName, String kafkaServer) {
        Properties props = new Properties();
        // Make sure kafka server is running
        props.put("metadata.broker.list", kafkaServer);
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        for (Tuple2<String, String> entry : messages) {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topicName, entry._1(), entry._2());
            producer.send(data);
        }
        producer.close();
    }
}
