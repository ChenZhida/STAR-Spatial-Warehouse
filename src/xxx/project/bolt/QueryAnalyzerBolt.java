package xxx.project.bolt;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import xxx.project.util.Constants;
import xxx.project.util.QueryParser;

import java.util.Map;
import java.util.Properties;

public class QueryAnalyzerBolt extends BaseBasicBolt {
    private final String BROKER = Constants.KAFKA_BROKER;
    private KafkaProducer<String, String> producer = null;
    private Properties props = null;
    private QueryParser queryParser = new QueryParser();

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        Properties props = configProperties(BROKER);
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Boolean isValid = produceQuery(tuple);
        if (isValid)
            collector.emit(tuple.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("query"));
    }

    private boolean produceQuery(Tuple request) {
        final String topic = Constants.KAFKA_QUERY_RESULT_TOPIC;
        String requestText = request.getString(4);
        int firstLineEndIndex = requestText.indexOf('\n');
        if (firstLineEndIndex == -1)
            return false;

//        String rangeText = requestText.substring(lastReturnIndex + 1);
        String sqlText = requestText.substring(0, firstLineEndIndex);

        StringBuilder parsedSQL = new StringBuilder();
        boolean isValid = false;
        try {
            isValid = queryParser.parseSQLQuery(sqlText, parsedSQL);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (isValid == false) {
            String queryMsg = requestText;
            ProducerRecord<String, String> msg = new
                    ProducerRecord<>(topic, "error-info",
                    "Error! Query request is aborted. " + requestText);
            producer.send(msg);
        }

        return isValid;
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
}
