package xxx.project;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import xxx.project.util.Constants;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private Properties props = null;
    private KafkaConsumer<String, String> consumer = null;

    public Consumer() {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
        consumer = new KafkaConsumer<>(props);
    }

    public Consumer(Properties props) {
        this.props = props;
        this.consumer = new KafkaConsumer<>(this.props);
    }

    public void consumeMessages() {
        consumer.subscribe(Arrays.asList(Constants.KAFKA_OBJECT_TOPIC));
        int numMsgs = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                ++numMsgs;
                if (numMsgs % 1000 == 0) {
                    System.out.println("Consume " + numMsgs + " messages.");
                    System.out.print("The numMsgs-th msg: " );
                    System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(), record.key(), record.value());
                }
            }
        }
    }
//
//    private Map<TopicPartition, Long> processRecords(Map<String, ConsumerRecords<String, String>> records) {
//        Map<TopicPartition, Long> processedOffsets = new HashMap<>();
//        for(Map.Entry<String, ConsumerRecords<String, String>> recordMetadata : records.entrySet()) {
//            List<ConsumerRecord<String, String>> recordsPerTopic = recordMetadata.getValue().records();
//            for(int i = 0;i < recordsPerTopic.size();i++) {
//                ConsumerRecord record = recordsPerTopic.get(i);
//                // process each record
//                try {
//                    System.out.println(record.key() + ":" + record.value());
//                    processedOffsets.put(record.topicAndPartition(), record.offset());
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        return processedOffsets;
//    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        consumer.consumeMessages();
    }
}
