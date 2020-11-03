package xxx.project.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import xxx.project.util.*;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class QueryProducer {

    private KafkaProducer<String, String> producer = null;
    private Properties props = null;

    public QueryProducer(Properties props) {
        this.props = props;
        this.producer = new KafkaProducer<>(this.props);
    }

    public void produceMessages(List<WarehouseQuery> queries) throws InterruptedException {
        String topic = Constants.KAFKA_QUERY_TOPIC;
        for (WarehouseQuery query : queries) {
            HashMap<String, String> map = new HashMap<>();
            map.put(WarehouseQuery.MEASURE, query.getMeasure());
            map.put(WarehouseQuery.GROUPBY, query.getGroupBy());

            Tuple4<Double, Double, Double, Double> range = query.getRange().get();
            String rangeStr = range._1() + " " + range._2() + " " + range._3() + " " + range._4();
            map.put(WarehouseQuery.RANGE, rangeStr);

            List<String> tmpList = map.entrySet().stream()
                    .map(e -> e.getKey() + ":" + e.getValue())
                    .collect(Collectors.toList());
            String queryMsg = String.join("" + Constants.SEPARATOR, tmpList);

            ProducerRecord<String, String> msg = new
                    ProducerRecord<>(topic, String.valueOf(1), queryMsg);
            producer.send(msg);
            System.out.println(String.format("Issue query: \"%s\"", queryMsg));

            Thread.sleep(500);
        }
    }

    public static Properties configProperties(String broker) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

    public static void main(String[] args) throws Exception {
        String broker = Constants.KAFKA_BROKER;
        Properties props = configProperties(broker);

        String objectFilePath = args[0];
        int numObjects = Integer.valueOf(args[1]);
        int numQueries = Integer.valueOf(args[2]);
        double rangeRatio = Double.valueOf(args[3]);

        System.out.println("Configuration:");
        System.out.println("\tobjectFilePath = " + objectFilePath);
        System.out.println("\tnumObjects = " + numObjects);
        System.out.println("\tnumQueries = " + numQueries);
        System.out.println("\trangeRatio = " + rangeRatio);

        List<SpatioTextualObject> objects = UtilFunctions.createObjects(objectFilePath, numObjects);
        List<WarehouseQuery> queries = UtilFunctions.createQueries(
                objects, numQueries, Constants.MIN_LAT, Constants.MIN_LON,
                Constants.MAX_LAT, Constants.MAX_LON, rangeRatio);

        (new QueryProducer(props)).produceMessages(queries);
    }
}
