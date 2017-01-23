/**
 * Created by rubbal on 23/1/17.
 */

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ConsumerExample {
    private final ConsumerConnector consumer;
    private final String topic;

    private ConsumerExample(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }

    private void consume() throws InterruptedException {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(5);
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executorService.submit(() -> {
                for (MessageAndMetadata<byte[], byte[]> aStream : stream) {
                    System.out.println(new String(aStream.key()) + "," + new String(aStream.message()));

                }
            });
        }

        Thread.sleep(10000);
        consumer.shutdown();

    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public static void consume(String zooKeeper, String consumerGroupId, String topic) throws InterruptedException {
        ConsumerExample consumer = new ConsumerExample(zooKeeper, consumerGroupId, topic);
        consumer.consume();
    }
}