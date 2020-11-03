package xxx.project.util;

import java.util.*;
import java.util.stream.Collectors;

public class AggrValues {
    private String type;
    private int count = 0;
    private String[] terms = null;
    private int[] frequencys = null;
    private Set<String> termSet = null;
    private int numTerms = 0;

    public AggrValues(String type, int topKCounterSize) {
        this.type = type;
        if (type.equals(Constants.AGGR_TOPK)) {
            terms = new String[topKCounterSize];
            frequencys = new int[topKCounterSize];
            termSet = new HashSet<>();
        }
    }

    public void update(SpatioTextualObject object) {
        if (type.equals(Constants.AGGR_COUNT))
            ++count;
        else {
            Set<String> objTerms = object.getTerms();
            Map<String, Integer> term2Freq = new HashMap<>();
            for (String term : objTerms) {
                if (termSet.contains(term)) {
                    int i = 0;
                    while (i < numTerms && !terms[i].equals(term))
                        ++i;
                    ++frequencys[i];
                } else {
                    int freq = term2Freq.getOrDefault(term, 0) + 1;
                    term2Freq.put(term, freq);
                }
            }

            for (Map.Entry<String, Integer> entry : term2Freq.entrySet()) {
                String term = entry.getKey();
                int freq = entry.getValue();
                int minIndex = -1;
                int minFreq = Integer.MAX_VALUE;
                for (int i = 0; i < numTerms; ++i) {
                    if (frequencys[i] < minFreq) {
                        minFreq = frequencys[i];
                        minIndex = i;
                    }
                }

                String removedTerm = terms[minIndex];
                termSet.remove(removedTerm);
                terms[minIndex] = term;
                frequencys[minIndex] = freq;
            }
        }
    }

    public void merge(AggrValues other) {
        if (type.equals(Constants.AGGR_COUNT))
            count += other.count;
        else {
            Map<String, Integer> term2Freq = new HashMap<>();
            for (String term : other.termSet) {
                if (termSet.contains(term)) {
                    int i = 0;
                    while (i < numTerms && !terms[i].equals(term))
                        ++i;
                    ++frequencys[i];
                } else {
                    int freq = term2Freq.getOrDefault(term, 0) + 1;
                    term2Freq.put(term, freq);
                }
            }

            for (Map.Entry<String, Integer> entry : term2Freq.entrySet()) {
                String term = entry.getKey();
                int freq = entry.getValue();
                int minIndex = -1;
                int minFreq = Integer.MAX_VALUE;
                for (int i = 0; i < numTerms; ++i) {
                    if (frequencys[i] < minFreq) {
                        minFreq = frequencys[i];
                        minIndex = i;
                    }
                }

                String removedTerm = terms[minIndex];
                termSet.remove(removedTerm);
                terms[minIndex] = term;
                frequencys[minIndex] = freq;
            }
        }
    }

    public String getResult(int kOfTopK) {
        if (type.equals(Constants.AGGR_COUNT))
            return String.valueOf(count);
        else {
            StringBuilder builder = new StringBuilder();
            Tuple2[] results = new Tuple2[numTerms];
            for (int i = 0; i < numTerms; ++i) {
                String term = terms[i];
                int freq = frequencys[i];
                Tuple2<String, Integer> pair = new Tuple2<>(term, freq);
                results[i] = pair;
            }

            List<Tuple2> res = Arrays.stream(results).
                    sorted((x, y) -> Integer.compare((Integer)y._2(), (Integer)x._2())).
                    collect(Collectors.toList());
            int size = Math.min(kOfTopK, numTerms);
            for (int i = 0; i < size; ++i) {
                Tuple2 pair = res.get(i);
                String term = (String)pair._1();
                int freq = (Integer)pair._2();
                builder.append(term + ":" + freq + " ");
            }

            return builder.toString();
        }
    }
}
