package xxx.project.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import xxx.project.util.Constants;
import xxx.project.util.SpatioTextualObject;
import xxx.project.util.Tuple2;
import xxx.project.util.WarehouseQuery;
import xxx.project.worker.index.Index;

import java.util.ArrayList;
import java.util.Map;

public class WorkerBolt extends BaseBasicBolt {
    private Index index;

    public WorkerBolt(Index index) {
        this.index = index;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (tuple.getSourceComponent().equals(Constants.BOLT_ROUTER)) {
            processObject(tuple);
        } else {
            System.out.println("Computing the result of the query...");
            try {
                WarehouseQuery query = new WarehouseQuery(tuple);
                if (query.isContinuousQuery())
                    return;
                ArrayList<String> messages = processQuery(query);
                if (messages == null)
                    collector.emit(new Values(""));
                else {
                    for (String msg : messages)
                        collector.emit(new Values(String.format("%d%c%s", messages.size(), Constants.SEPARATOR, msg)));
                    System.out.println("Number of message sent to the merger: " + messages.size());
                    messages.forEach(System.out::println);
                }
            } catch(Exception e){
                e.printStackTrace();
                return;
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result"));
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

        // for debug
//        String timestamp = "2014-11-02 01:11:11";
//        String year = "2014";
//        String month = "201411";
//        String day = "20141102";
//        String hour = "2014110201";

        ArrayList<String> detailedTimeInfo =
                SpatioTextualObject.getDetailedTimeInfo(timestamp);
        String week = detailedTimeInfo.get(2);

        SpatioTextualObject object = new SpatioTextualObject(new Tuple2<>(lat, lon), text, timestamp,
                country, city, year, month, week, day, hour, "topic");
        index.addObject(object);
    }

    private ArrayList<String> processQuery(WarehouseQuery query) {
        if (query.getMeasure().equals(WarehouseQuery.MEASURE_COUNT)) {
            Map<String, Integer> result = index.processCountQuery(query);
            return packCountMessages(result);
        } else { // query.getMeasure().equals(QueryParser.MEASURE_WORD_COUNT)
            Map<String, Map<String, Integer>> result = index.processWordCountQuery(query);
            return packWordCountMessages(result);
        }
    }

    private ArrayList<String> packCountMessages(Map<String, Integer> rawResult) {
        if (rawResult.isEmpty())
            return null;

        ArrayList<String> msgArray = new ArrayList<>();
        int msgId = 0;
        StringBuilder msgBuffer = new StringBuilder();

        for (Map.Entry<String, Integer> elem: rawResult.entrySet()) {
            String k = elem.getKey();
            Integer v = elem.getValue();

            String str = k + " " + v + Constants.SEPARATOR;
            if (msgBuffer.length() + str.length() > Constants.MSG_LEN_LIMIT) {
                msgArray.add(msgId + String.valueOf(Constants.SEPARATOR) + msgBuffer);
                ++msgId;
                msgBuffer.delete(0, msgBuffer.length());
            }
            msgBuffer.append(str);
        }
        if (msgBuffer.length() > 0)
            msgArray.add(msgId + String.valueOf(Constants.SEPARATOR) + msgBuffer);

        return msgArray;
    }

    private ArrayList<String> packWordCountMessages(Map<String, Map<String, Integer>> rawResult) {
        if (rawResult.isEmpty())
            return null;

        ArrayList<String> msgArray = new ArrayList<>();
        int msgId = 0;
        StringBuilder msgBuffer = new StringBuilder();

        for (Map.Entry<String, Map<String, Integer>> elem: rawResult.entrySet()) {
            String k = elem.getKey();
            Map<String, Integer> word2Count = elem.getValue();
            msgBuffer.append(k + Constants.SEPARATOR);

            for (Map.Entry<String, Integer> e: word2Count.entrySet()) {
                String str = e.getKey() + " " + e.getValue() + String.valueOf(Constants.SEPARATOR);
                if (msgBuffer.length() + str.length() > Constants.MSG_LEN_LIMIT) {
                    msgArray.add(msgId + String.valueOf(Constants.SEPARATOR) + msgBuffer);
                    ++msgId;
                    msgBuffer.delete(0, msgBuffer.length());
                    msgBuffer.append(k + Constants.SEPARATOR);
                }
                msgBuffer.append(str);
            }

            if (msgBuffer.length() > 0) {
                msgArray.add(msgId + String.valueOf(Constants.SEPARATOR) + msgBuffer);
                ++msgId;
                msgBuffer.delete(0, msgBuffer.length());
            }
        }

        return msgArray;
    }
}
