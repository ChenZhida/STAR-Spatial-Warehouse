package xxx.project.worker.index;

import xxx.project.util.Constants;
import xxx.project.util.SpatioTextualObject;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InvertedIndex implements Serializable {
    public static final long serialVersionUID = 106L;

    public static HashSet<String> stopwords;
    static {
        stopwords = new HashSet<>(Arrays.asList(Constants.STOPWORDS.split(",")));
    }

    private HashMap<String, List<Integer>> term2ObjectId = null;
    private List<SpatioTextualObject> objects = null;

    public void addObject(SpatioTextualObject object) {
        if (term2ObjectId == null)
            term2ObjectId = new HashMap<>();
        if (objects == null)
            objects = new ArrayList<>();

        objects.add(object);
        int id = objects.size() - 1;
        Set<String> terms = extractTerms(object.getText());
        for (String term : terms) {
            if (!stopwords.contains(term)) {
                List<Integer> objectList = term2ObjectId.get(term);
                if (objectList == null) {
                    objectList = new LinkedList<>();
                    term2ObjectId.put(term, objectList);
                }
                objectList.add(id);
            }
        }
    }

    public Optional<List<SpatioTextualObject>> getObjectsByTerms(Set<String> terms) {
        terms.removeAll(stopwords);
        if (terms.isEmpty())
            return Optional.empty();

        List<List<Integer>> lists = terms.stream()
                .map(t -> term2ObjectId.get(t))
                .collect(Collectors.toList());
        boolean resultNotExists = lists.stream()
                .anyMatch(l -> l == null);

        if (resultNotExists)
            return Optional.empty();

        List<Integer> indexes = lists.stream()
                .map(l -> 0)
                .collect(Collectors.toList());
        List<Integer> resultObjectIds = new ArrayList<>();

        while (true) {
            boolean reachEnd = IntStream.range(0, lists.size())
                    .anyMatch(i -> lists.get(i).size() <= indexes.get(i));
            if (reachEnd)
                break;

            boolean allEqual = true;
            int minIdxOfIdxes = 0;
            for (int i = 1; i < lists.size(); ++i) {
                List<Integer> listMin = lists.get(minIdxOfIdxes);
                List<Integer> curList = lists.get(i);
                if (curList.get(indexes.get(i)) < listMin.get(indexes.get(minIdxOfIdxes))) {
                    allEqual = false;
                    minIdxOfIdxes = i;
                } else if (curList.get(indexes.get(i)) > listMin.get(indexes.get(minIdxOfIdxes)))
                    allEqual = false;
            }

            if (allEqual) {
                List<Integer> listMin = lists.get(minIdxOfIdxes);
                resultObjectIds.add(listMin.get(indexes.get(minIdxOfIdxes)));
                IntStream.range(0, indexes.size())
                        .forEach(i -> indexes.set(i, indexes.get(i) + 1));
            } else {
                indexes.set(minIdxOfIdxes, indexes.get(minIdxOfIdxes) + 1);
            }
        }

        List<SpatioTextualObject> results = resultObjectIds.stream()
                .map(k -> objects.get(k))
                .collect(Collectors.toList());

        return results.isEmpty() ? Optional.empty() : Optional.of(results);
    }

    public Optional<List<SpatioTextualObject>> getObjects() {
        return objects == null ? Optional.empty() :
                Optional.of(objects);
    }

    public int getNumObjects() {
        return objects == null ? 0 : objects.size();
    }

    private Set<String> extractTerms(String text) {
        Set<String> terms = new HashSet<>();
        Arrays.stream(text.split("" + Constants.TEXT_SEPARATOR))
                .forEach(t -> terms.add(t.toLowerCase()));

        return terms;
    }
}
