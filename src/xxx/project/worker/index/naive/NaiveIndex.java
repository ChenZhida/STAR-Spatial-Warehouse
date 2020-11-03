package xxx.project.worker.index.naive;

import xxx.project.util.Constants;
import xxx.project.util.SpatioTextualObject;
import xxx.project.util.WarehouseQuery;
import xxx.project.worker.index.Index;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class NaiveIndex implements Index, Serializable {
    private HashMap<Integer, List<SpatioTextualObject>> hour2Objects = new HashMap<>();

    public void addObject(SpatioTextualObject object) {
        String hour = object.getHour();
        int key = Integer.valueOf(hour);
        List<SpatioTextualObject> objectList = hour2Objects.get(key);

        if (objectList == null) {
            objectList = new ArrayList<>();
            hour2Objects.put(key, objectList);
        }
        objectList.add(object);

        if (hour2Objects.size() > Constants.NUM_HOURS_DATA_KEPT) {
            int oldestHour = Integer.MAX_VALUE;
            for (Integer currentHour : hour2Objects.keySet()) {
                if (currentHour < oldestHour)
                    oldestHour = currentHour;
            }

            hour2Objects.remove(oldestHour);
        }
    }

    public void readMaterializedCubesFromFile(String cubeFilePath) throws IOException, ClassNotFoundException {
        return;
    }

    public Map<String, Integer> processCountQuery(WarehouseQuery query) {
        Map<String, Integer> result = new HashMap<>();
        String group = query.getGroupBy();

        for (List<SpatioTextualObject> objects : hour2Objects.values()) {
            Map<String, Long> tmp =
                    objects.stream().filter(o -> query.verify(query, o))
                            .map(o -> o.getGroupBy(group))
                            .collect(Collectors.groupingBy(object -> object, Collectors.counting()));

            for (Map.Entry<String, Long> entry : tmp.entrySet()) {
                String key = entry.getKey();
                int count = result.getOrDefault(key, 0);
                result.put(key, count + entry.getValue().intValue());
            }
        }

        return result;
    }

    public Map<String, Map<String, Integer>> processWordCountQuery(WarehouseQuery query) {
        Map<String, Map<String, Integer>> result = new HashMap<>();
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

        for (List<SpatioTextualObject> objects : hour2Objects.values()) {
            Map<String, Map<String, Long>> tmp
                    = objects.stream().filter(o -> WarehouseQuery.verify(query, o))
                    .collect(Collectors.groupingBy(o -> WarehouseQuery.groupBy(query, o), wordCountCollector));

            for (Map.Entry<String, Map<String, Long>> group2WordFreq : tmp.entrySet()) {
                String group = group2WordFreq.getKey();
                Map<String, Long> wordFreqLong = group2WordFreq.getValue();

                Map<String, Integer> wordFreqInt = result.get(group);
                if (wordFreqInt == null) {
                    wordFreqInt = new HashMap<>();
                    result.put(group, wordFreqInt);
                }

                for (Map.Entry<String, Long> entry : wordFreqLong.entrySet()) {
                    String word = entry.getKey();
                    int oldVal = wordFreqInt.getOrDefault(word, 0);
                    int freq = entry.getValue().intValue();
                    wordFreqInt.put(word, oldVal + freq);
                }
            }
        }

        return result;
    }
}
