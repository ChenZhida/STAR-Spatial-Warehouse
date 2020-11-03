package xxx.project.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import xxx.project.util.Constants;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

public class ObjectProducer {
    public static final String FILE_PATH_CONFIG = "producer.file.path";

    private Properties props = null;
    private KafkaProducer<String, String> producer = null;

    public ObjectProducer() {
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(FILE_PATH_CONFIG, "resources/sql_tweets.txt");
        producer = new KafkaProducer<>(props);
    }

    public ObjectProducer(Properties props) {
        this.props = props;
        this.producer = new KafkaProducer<>(this.props);
    }

    public void produceMessagesSlowly(int numObjects) throws IOException, InterruptedException {
        String fileName = (String)props.get(FILE_PATH_CONFIG);
        String topic = Constants.KAFKA_OBJECT_TOPIC;
        Reader reader;

        System.out.println("File: " + fileName);
        System.out.println("Broker: " + Constants.KAFKA_BROKER);
        System.out.println("Topic: " + topic);

        reader = new InputStreamReader(new FileInputStream(fileName));
        char[] tempChars = new char[1000];
        int index = 0, charRead = 0;
        int msgId = 0;

        while ((charRead = reader.read()) != -1) {
            if ((char)charRead != '\n')
                tempChars[index++] = (char)charRead;
            else {
                String line = new String(tempChars, 0, index);
                String[] values = line.split(String.valueOf(Constants.SEPARATOR));

                if (values.length == 14) {
                    ProducerRecord<String, String> msg = new
                            ProducerRecord<>(topic, String.valueOf(index), line);
                    producer.send(msg);
                    if (++msgId % 10 == 0) {
                        System.out.println("Finish sending " + msgId + " tuples.");
                        Thread.sleep(1000);
                    }
                    if (msgId >= numObjects)
                        break;
                }

                index = 0;
            }
        }

        reader.close();
    }

    public void produceMessages(int numObjects) throws IOException, InterruptedException {
        String fileName = (String)props.get(FILE_PATH_CONFIG);
        String topic = Constants.KAFKA_OBJECT_TOPIC;
        Reader reader;

        reader = new InputStreamReader(new FileInputStream(fileName));
        char[] tempChars = new char[1000];
        int index = 0, charRead = 0;
        int msgId = 0;

        while ((charRead = reader.read()) != -1) {
            if ((char)charRead != '\n')
                tempChars[index++] = (char)charRead;
            else {
                String line = new String(tempChars, 0, index);
                String[] values = line.split(String.valueOf(Constants.SEPARATOR));

                if (values.length == 14) {
                    ProducerRecord<String, String> msg = new
                            ProducerRecord<>(topic, String.valueOf(index), line);
                    producer.send(msg);
                    if (++msgId % 100000 == 0) {
                        System.out.println("Finish sending " + msgId + " tuples.");
//                        Thread.sleep(10);
                    }
                    if (msgId >= numObjects)
                        break;
                }

                index = 0;
            }
        }

        reader.close();
    }

    public static Properties configProperties(String broker,
                                              String producerFilePath) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ObjectProducer.FILE_PATH_CONFIG, producerFilePath);

        return props;
    }

    public static void main(String[] args) throws Exception {
//        int numObjects = Integer.valueOf(args[0]);
        String broker = Constants.KAFKA_BROKER;
        Properties props = configProperties(broker, "resources/sql_tweets.txt");

//        (new ObjectProducer(props)).produceMessages(numObjects);
        (new ObjectProducer(props)).produceMessagesSlowly(10000);
    }
}
