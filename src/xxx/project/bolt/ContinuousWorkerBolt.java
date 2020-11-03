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
import xxx.project.util.*;
import xxx.project.worker.index.CostBasedHybridTree;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class ContinuousWorkerBolt extends BaseBasicBolt {
    private HashMap<Integer, WarehouseQuery> continuousQueries = new HashMap<>();
    private KafkaProducer<String, String> producer = null;
    private HashMap<Integer, HashMap<String, Integer>> countResults = new HashMap<>();
    private HashMap<Integer, HashMap<String, HashMap<String, Integer>>> topkResults = new HashMap<>();
    private HashMap<Integer, ArrayList<SpatioTextualObject>> qId2Objects = new HashMap<>();

    private List<CostBasedHybridTree> CHTrees = null;
    private Set<Integer> existedQueryIds = new HashSet<>();

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        Properties props = configProperties(Constants.KAFKA_BROKER);
        producer = new KafkaProducer<>(props);
        try {
            CHTrees.add(new CostBasedHybridTree());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (tuple.getSourceComponent().equals(Constants.BOLT_PARSER)) {
            SpatioTextualObject object = getObjectFromTuple(tuple);
            CHTrees.forEach(tree -> tree.processObject(object));
        } else if (tuple.getSourceComponent().equals(Constants.BOLT_QUERY_ANALYZER)) {
            // Add a new continuous query
            try {
                WarehouseQuery query = new WarehouseQuery(tuple);
                if (query.isSnapshotQuery())
                    return;
                int queryId = query.getQueryId();
                if (queryId == 1) { // the web system has been reset, need to clear all continuous queries
                    CHTrees.clear();
                    CHTrees.add(new CostBasedHybridTree());
                }

                if (existedQueryIds.contains(queryId)) {
                    CHTrees.forEach(tree -> tree.deleteQuery(query));
                    existedQueryIds.remove(queryId);

                    List<Integer> emptyTreeIds = new ArrayList<>();
                    for (int i = 0; i < CHTrees.size(); ++i) {
                        if (CHTrees.get(i).isEmpty())
                            emptyTreeIds.add(i);
                    }
                    if (!emptyTreeIds.isEmpty()) {
                        List<CostBasedHybridTree> newCHTrees = new ArrayList<>();
                        for (int i = 0; i < CHTrees.size(); ++i) {
                            if (!emptyTreeIds.contains(i))
                                newCHTrees.add(CHTrees.get(i));
                        }
                        CHTrees.clear();
                        CHTrees = newCHTrees;
                    }
                } else {
                    CostBasedHybridTree targetTree = CHTrees.get(CHTrees.size() - 1);
                    if (targetTree.isFull()) {
                        CHTrees.add(new CostBasedHybridTree());
                        targetTree = CHTrees.get(CHTrees.size() - 1);
                    }
                    targetTree.insertQuery(query);
                    String infoMessage = "The system registered the continuous query successfully. " + query.getQueryInfo();
                    ProducerRecord<String, String> msg = new
                            ProducerRecord<>(Constants.KAFKA_QUERY_RESULT_TOPIC, "register-continuous-query",
                            infoMessage); // notify the servlet of the registration
                    producer.send(msg);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (tuple.getSourceComponent().equals(Constants.CONTINUOUS_QUERY_REQUEST_SPOUT)) {
            try {
                WarehouseQuery query = new WarehouseQuery(tuple);
                int queryId = Integer.valueOf(tuple.getString(4));
                String queryResult = "";

                for (CostBasedHybridTree tree : CHTrees) {
                    String result = tree.getQueryResult(queryId);
                    if (result.length() > 0) {
                        queryResult = result;
                        break;
                    }
                }

                ProducerRecord<String, String> msg;
                if (queryResult.length() == 0) {
                    msg = new
                            ProducerRecord<>(Constants.KAFKA_CONTINUOUS_QUERY_RESULT_TOPIC,
                            "continuous-query-result", query.getQueryInfo() + ": Empty result set");
                } else {
                    msg = new
                            ProducerRecord<>(Constants.KAFKA_CONTINUOUS_QUERY_RESULT_TOPIC,
                            "continuous-query-result", query.getQueryInfo() + ": " + queryResult);
                }
                producer.send(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void execute2(Tuple tuple, BasicOutputCollector collector) {
        if (tuple.getSourceComponent().equals(Constants.BOLT_PARSER)) {
            // Check the new object against the continuous queries
            try {
                processObject(tuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (tuple.getSourceComponent().equals(Constants.BOLT_QUERY_ANALYZER)) {
            // Add a new continuous query
            try {
                WarehouseQuery query = new WarehouseQuery(tuple);
                if (query.isSnapshotQuery())
                    return;
                int queryId = query.getQueryId();
                if (queryId == 1) { // the web system has been reset, need to clear all continuous queries
                    continuousQueries.clear();
                    countResults.clear();
                    topkResults.clear();
                }

                continuousQueries.put(queryId, query);
                if (query.getMeasure().equals(WarehouseQuery.MEASURE_COUNT))
                    countResults.put(queryId, new HashMap<>());
                else if (query.getMeasure().equals(WarehouseQuery.MEASURE_WORD_COUNT))
                    countResults.put(queryId, new HashMap<>());

                String infoMessage = "The system registered the continuous query successfully. " + query.getQueryInfo();
                ProducerRecord<String, String> msg = new
                        ProducerRecord<>(Constants.KAFKA_QUERY_RESULT_TOPIC, "register-continuous-query",
                        infoMessage); // notify the servlet of the registration
                producer.send(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (tuple.getSourceComponent().equals(Constants.CONTINUOUS_QUERY_REQUEST_SPOUT)) {
            try {
//                ProducerRecord<String, String> msg = new
//                        ProducerRecord<>(Constants.KAFKA_CONTINUOUS_QUERY_RESULT_TOPIC,
//                        "continuous-query-result",   ": Updated result set");
//                producer.send(msg);
                int queryId = Integer.valueOf(tuple.getString(4));
                outputQueryResult(queryId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result"));
    }

    private SpatioTextualObject getObjectFromTuple(Tuple tuple) {
        String[] fields = ParserBolt.PARSED_FIEDLS;
        double lat = tuple.getDoubleByField(fields[1]);
        double lon = tuple.getDoubleByField(fields[2]);
        String text = tuple.getStringByField(fields[3]);
        String timestamp = tuple.getStringByField(fields[4]);
        String country = tuple.getStringByField(fields[5]);
        String city = tuple.getStringByField(fields[6]);
        String year = tuple.getStringByField(fields[7]);
        String month = tuple.getStringByField(fields[8]);
        String day = tuple.getStringByField(fields[9]);
        String hour = tuple.getStringByField(fields[10]);
        String topic = tuple.getStringByField(fields[11]);

        ArrayList<String> detailedTimeInfo =
                SpatioTextualObject.getDetailedTimeInfo(timestamp);
        String week = detailedTimeInfo.get(2);

        SpatioTextualObject object = new SpatioTextualObject(new Tuple2<>(lat, lon), text, timestamp,
                country, city, year, month, week, day, hour, topic);

        return object;
    }

    private void processObject(Tuple tuple) {
        String[] fields = ParserBolt.PARSED_FIEDLS;
        double lat = tuple.getDoubleByField(fields[1]);
        double lon = tuple.getDoubleByField(fields[2]);
        String text = tuple.getStringByField(fields[3]);
        String timestamp = tuple.getStringByField(fields[4]);
        String country = tuple.getStringByField(fields[5]);
        String city = tuple.getStringByField(fields[6]);
        String year = tuple.getStringByField(fields[7]);
        String month = tuple.getStringByField(fields[8]);
        String day = tuple.getStringByField(fields[9]);
        String hour = tuple.getStringByField(fields[10]);

        ArrayList<String> detailedTimeInfo =
                SpatioTextualObject.getDetailedTimeInfo(timestamp);
        String week = detailedTimeInfo.get(2);

        SpatioTextualObject object = new SpatioTextualObject(new Tuple2<>(lat, lon), text, timestamp,
                country, city, year, month, week, day, hour, "topic");

        for (Integer qId : continuousQueries.keySet()) {
            WarehouseQuery query = continuousQueries.get(qId);
            if (query.verify(query, object)) {
                if (query.getMeasure().equals(WarehouseQuery.MEASURE_COUNT)) {
                    HashMap<String, Integer> res = countResults.get(qId);
                    String group = object.getGroupBy(query.getGroupBy());
                    res.put(group, res.getOrDefault(group, 0) + 1);
                } else if (query.getMeasure().equals(WarehouseQuery.MEASURE_WORD_COUNT)) {
                    HashMap<String, HashMap<String, Integer>> res = topkResults.get(qId);
                    String group = object.getGroupBy(query.getGroupBy());

                    HashMap<String, Integer> aMap = res.get(group);
                    if (aMap == null) {
                        aMap = new HashMap<>();
                        String[] ss = object.getText().split("" + Constants.TEXT_SEPARATOR);
                        for (String s : ss) {
                            int count = aMap.getOrDefault(s, 0);
                            aMap.put(s, count + 1);
                        }
                        topkResults.put(qId, res);
                    } else {
                        String[] ss = object.getText().split("" + Constants.TEXT_SEPARATOR);
                        for (String s : ss) {
                            int count = aMap.getOrDefault(s, 0);
                            aMap.put(s, count + 1);
                        }
                    }
                }
            }
        }
    }

    private void outputQueryResult(Integer queryId) {
        WarehouseQuery query = continuousQueries.get(queryId);
        if (query == null) {
            ProducerRecord<String, String> msg = new
                    ProducerRecord<>(Constants.KAFKA_CONTINUOUS_QUERY_RESULT_TOPIC,
                    "continuous-query-result", query.getQueryInfo() + ": Empty result set");
            producer.send(msg);
            return;
        }
        if (query.getMeasure().equals(WarehouseQuery.MEASURE_COUNT)) {
            outputCountResults(query);
        } else if (query.getMeasure().equals(WarehouseQuery.MEASURE_WORD_COUNT)) {
            outputTopkResults(query);
        }
    }

    private void outputCountResults(WarehouseQuery query) {
        StringBuilder resultMessage = new StringBuilder();
        HashMap<String, Integer> res = countResults.get(query.getQueryId());
        if (res == null) {
            ProducerRecord<String, String> msg = new
                    ProducerRecord<>(Constants.KAFKA_CONTINUOUS_QUERY_RESULT_TOPIC,
                    "continuous-query-result", query.getQueryInfo() + ": Empty result set");
            producer.send(msg);
            return;
        }

        final String sortOrder = query.getSortOrder();
        Comparator<Map.Entry<String, Integer>> comp = (a, b) -> Integer.compare(a.getValue(), b.getValue());
        if (sortOrder.equals(QueryParser.ASC))
            res.entrySet().stream().sorted(comp)
                    .forEach(e -> resultMessage.append(e.getKey() + " " + e.getValue() + "; \n"));
        else
            res.entrySet().stream().sorted(comp.reversed())
                    .forEach(e -> resultMessage.append(e.getKey() + " " + e.getValue() + "; \n"));

        ProducerRecord<String, String> msg = new
                ProducerRecord<>(Constants.KAFKA_CONTINUOUS_QUERY_RESULT_TOPIC,
                "continuous-query-result", query.getQueryInfo() + ": " + resultMessage);
        producer.send(msg);
    }

    private void outputTopkResults(WarehouseQuery query) {
        StringBuilder resultMessage = new StringBuilder();
        HashMap<String, HashMap<String, Integer>> group2TopK = topkResults.get(query.getQueryId());
        if (group2TopK == null) {
            ProducerRecord<String, String> msg = new
                    ProducerRecord<>(Constants.KAFKA_CONTINUOUS_QUERY_RESULT_TOPIC,
                    "continuous-query-result", query.getQueryInfo() + ": Empty result set");
            producer.send(msg);
            return;
        }
        final int kOfTopK = query.getkOfTopK();
        group2TopK.entrySet().stream().forEach(e -> {
            resultMessage.append(e.getKey() + ": ");
            int i = 0;
            PriorityQueue<Map.Entry<String, Integer>> pq =
                    new PriorityQueue<>((a,b)-> { return b.getValue() - a.getValue();});
            e.getValue().entrySet().forEach(pair -> pq.add(pair));
            Iterator<Map.Entry<String, Integer>> iter = pq.iterator();
            while (iter.hasNext()) {
                resultMessage.append(iter.next() + " ");
                if (++i == kOfTopK)
                    break;
            }
            resultMessage.append("; \n");
        });

        ProducerRecord<String, String> msg = new
                ProducerRecord<>(Constants.KAFKA_CONTINUOUS_QUERY_RESULT_TOPIC,
                "continuous-query-result", query.getQueryInfo() + ": " + resultMessage);
        producer.send(msg);
    }

    @Deprecated
    private void processObject(Tuple tuple, String dummy) {
        String[] fields = ParserBolt.PARSED_FIEDLS;
        double lat = tuple.getDoubleByField(fields[1]);
        double lon = tuple.getDoubleByField(fields[2]);
        String text = tuple.getStringByField(fields[3]);
        String timestamp = tuple.getStringByField(fields[4]);
        String country = tuple.getStringByField(fields[5]);
        String city = tuple.getStringByField(fields[6]);
        String year = tuple.getStringByField(fields[7]);
        String month = tuple.getStringByField(fields[8]);
        String day = tuple.getStringByField(fields[9]);
        String hour = tuple.getStringByField(fields[10]);

        ArrayList<String> detailedTimeInfo =
                SpatioTextualObject.getDetailedTimeInfo(timestamp);
        String week = detailedTimeInfo.get(2);

        SpatioTextualObject object = new SpatioTextualObject(new Tuple2<>(lat, lon), text, timestamp,
                country, city, year, month, week, day, hour, "topic");
        continuousQueries.entrySet().stream().forEach(e -> {
            if (e.getValue().verify(e.getValue(), object)) {
                addObjectIntoResult(e.getKey(), object, "");
            }
        });
    }

    @Deprecated
    private void addObjectIntoResult(int qId, SpatioTextualObject object, String dummy) {
        ArrayList<SpatioTextualObject> objList = qId2Objects.get(qId);
        if (objList == null) {
            objList = new ArrayList<>();
            qId2Objects.put(qId, objList);
        }
        objList.add(object);
    }

    @Deprecated
    private String answerQuery(WarehouseQuery query, String dummy) {
        StringBuilder resultMessage = new StringBuilder();
        ArrayList<SpatioTextualObject> objects = qId2Objects.get(query.getQueryId());
        if (objects == null)
            return "Empty result set";

        if (query.getMeasure().equals(WarehouseQuery.MEASURE_COUNT)) {
            String group = query.getGroupBy();
            Map<String, Long> result =
                    objects.stream().map(o -> o.getGroupBy(group))
                            .collect(Collectors.groupingBy(object -> object, Collectors.counting()));
            result.entrySet().stream().forEach(e -> resultMessage.append(e.getKey() + " " + e.getValue() + "; \n"));
        } else if (query.getMeasure().equals(WarehouseQuery.MEASURE_WORD_COUNT)) {
            Collector<SpatioTextualObject, ?, Map<String, Long>> wordCountCollector
                    = Collector.of(HashMap::new, (aMap, obj) -> {
                String[] ss = obj.getText().split(""+ Constants.TEXT_SEPARATOR);
                for (String s: ss) {
                    long count = aMap.getOrDefault(s, 0L);
                    aMap.put(s, count + 1);
                }}, (left, right) -> {
                for (Map.Entry<String, Long> e: right.entrySet()) {
                    long count = left.getOrDefault(e.getKey(), 0L);
                    left.put(e.getKey(), count + e.getValue());
                }
                return left;
            });

            Map<String, Map<String, Long>> result =
                    objects.stream().collect(Collectors.groupingBy(
                            o -> WarehouseQuery.groupBy(query, o), wordCountCollector));
            result.entrySet().stream().forEach(e -> resultMessage.append(e.getKey() + " " + e.getValue() + "; \n"));
        }

        return resultMessage.toString();
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
