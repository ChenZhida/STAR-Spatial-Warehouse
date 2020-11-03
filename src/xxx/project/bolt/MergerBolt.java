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
import xxx.project.util.Tuple2;
import xxx.project.util.WarehouseQuery;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class MergerBolt extends BaseBasicBolt {
    private List<Integer> tasksOfWorkers = null;
    private HashMap<Integer, ArrayList<String>> task2Msgs = null;
    private HashMap<Integer, Boolean> task2recComp = null;
    private ArrayList<WarehouseQuery> queries = null;

    private KafkaProducer<String, String> producer = null;
    private Properties props = null;

    @Override
    public void prepare(Map conf, TopologyContext context) {
        tasksOfWorkers = context.getComponentTasks(Constants.BOLT_WORKER);
        task2Msgs = new HashMap<>();
        task2recComp = new HashMap<>();
        queries = new ArrayList<>();

        for (Integer task: tasksOfWorkers) {
            task2Msgs.put(task, new ArrayList<>());
            task2recComp.put(task, false);
        }

        Properties props = configProperties(Constants.KAFKA_BROKER);
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            if (tuple.getSourceComponent().equals(Constants.BOLT_QUERY_ANALYZER)) {
                WarehouseQuery query = new WarehouseQuery(tuple);
                if (query.isSnapshotQuery())
                    registerNewQuery(query);

                return;
            }

            String msg = tuple.getString(0);
            int fromTask = tuple.getSourceTask();

            if (msg.length() == 0)
                task2recComp.put(fromTask, true);
            else {
                ArrayList<String> msgs = task2Msgs.get(fromTask);
                msgs.add(msg);
                int numMsgs = Integer.valueOf(msg.split(""+Constants.SEPARATOR)[0]);
                if (numMsgs == msgs.size())
                    task2recComp.put(fromTask, true);
            }

            boolean notReceiveAll = task2recComp.entrySet().stream().anyMatch(e -> !e.getValue());
            if (!notReceiveAll) {
                outputQueryResult();
                for (ArrayList<String> v: task2Msgs.values())
                    v.clear();
                for (Integer k: task2recComp.keySet())
                    task2recComp.put(k, false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(""));
    }

    private void registerNewQuery(WarehouseQuery query) {
        queries.add(query);
    }

    private void outputQueryResult() {
        WarehouseQuery query = queries.get(0);
        StringBuilder resultMessage = new StringBuilder();

        if (query.getMeasure().equals(WarehouseQuery.MEASURE_COUNT)) {
            Map<String, Long> result = task2Msgs.values().stream().flatMap(array ->
                    array.stream().flatMap(str -> {
                        String[] s1 = str.split("" + Constants.SEPARATOR);
                        String[] s2 = Arrays.copyOfRange(s1, 2, s1.length);
                        return Arrays.stream(s2).map(e -> {
                            int splitIndex = e.lastIndexOf(" ");
                            String key = e.substring(0, splitIndex);
                            Integer val = Integer.valueOf(e.substring(splitIndex + 1));
                            return new Tuple2<>(key, val);
                        });
                    })).collect(
                    Collectors.groupingBy(Tuple2::_1, Collectors.summingLong(Tuple2::_2)));

            if (query.needSort() == false) {
                result.forEach((k, v) -> resultMessage.append(k + "=" + v + "; "));
            } else {
                Comparator<Map.Entry<String, Long>> comp = (a, b) -> Long.compare(a.getValue(), b.getValue());
                final String sortOrder = query.getSortOrder();

                if (sortOrder.equals(QueryParser.ASC))
                    result.entrySet().stream().sorted(comp)
                            .forEach(e -> resultMessage.append(e.getKey() + "=" + e.getValue() + "; "));
                else
                    result.entrySet().stream().sorted(comp.reversed())
                            .forEach(e -> resultMessage.append(e.getKey() + "=" + e.getValue() + "; "));
            }
        } else if (query.getMeasure().equals(WarehouseQuery.MEASURE_WORD_COUNT)) {
            Collector<String, ?, Map<String, Long>> wordCountCollector
                    = Collector.of(HashMap::new,
                    (aMap, str) -> {
                        String[] s = str.split("\t");
                        String[] ss = Arrays.copyOfRange(s, 3, s.length);
                        for (String wordCount: ss) {
                            String[] pair = wordCount.split(" ");
                            long count = aMap.getOrDefault(pair[0], 0L);
                            aMap.put(pair[0], count + Long.valueOf(pair[1]));
                        }
                    },
                    (left, right) -> {
                        for (Map.Entry<String, Long> e: right.entrySet()) {
                            long count = left.getOrDefault(e.getKey(), 0L);
                            left.put(e.getKey(), count + e.getValue());
                        }
                        return left;
                    });

            /*
            task2Msgs.values().stream()
                    .flatMap(array -> array.stream())
                    .collect(Collectors.groupingBy(
                            str -> str.split("\t")[2],
                            wordCountCollector
                    )).forEach((k, v) -> resultMessage.append(k + ": " + v + "; \n"));
             */

            final int kOfTopK = query.getkOfTopK();
            task2Msgs.values().stream()
                    .flatMap(array -> array.stream())
                    .collect(Collectors.groupingBy(
                            str -> str.split("\t")[2],
                            wordCountCollector
                    )).forEach((k, v) -> {
                PriorityQueue<Map.Entry<String, Long>> pq =
                        new PriorityQueue<>((a,b)-> { return (int)(b.getValue() - a.getValue());});
                v.entrySet().forEach(e -> pq.add(e));
                resultMessage.append(k + ": ");
                int i = 0;
                Iterator<Map.Entry<String, Long>> iter = pq.iterator();
                while (iter.hasNext()) {
                    resultMessage.append(iter.next() + " ");
                    if (++i == kOfTopK)
                        break;
                }
                resultMessage.append("; ");
            });
        }

        if (resultMessage.length() == 0)
            resultMessage.append("Empty result set.");

        ProducerRecord<String, String> msg = new
                ProducerRecord<>(Constants.KAFKA_QUERY_RESULT_TOPIC,
                "snapshot-query-result", resultMessage.toString());
        producer.send(msg);
        queries.remove(0);
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
