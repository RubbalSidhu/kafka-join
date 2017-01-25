import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Tuple2;

import java.util.List;
import java.util.Properties;

/**
 * Created by rubbal on 23/1/17.
 */
public class ProducerExample {
    private ProducerExample() {

    }

    public static void produceStrings(List<Tuple2<String, String>> messages, String topicName, String kafkaServer) {
        Properties props = new Properties();
        // Make sure kafka server is running
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (Tuple2<String, String> entry : messages) {
            ProducerRecord<String, String> data = new ProducerRecord<>(topicName, entry._1(), entry._2());
            producer.send(data);
        }
        producer.close();
    }

    public static void produceIntegers(List<Tuple2<String, Integer>> messages, String topicName, String kafkaServer) {
        Properties props = new Properties();
        // Make sure kafka server is running
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(props);
        for (Tuple2<String, Integer> entry : messages) {
            ProducerRecord<String, Integer> data = new ProducerRecord<>(topicName, entry._1(), entry._2());
            producer.send(data);
        }
        producer.close();
    }
}
