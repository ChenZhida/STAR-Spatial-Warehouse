package xxx.project.worker.index.quadtree;

import xxx.project.util.*;
import xxx.project.worker.index.Index;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class QuadTreeCache implements Index, Serializable {
    private class QuadTree implements Serializable {
        public static final long serialVersionUID = 103L;

        private class Node extends QuadTreeNode {
            public HashMap<String, QueryView> countViews = null;
            public HashMap<String, QueryView> topKViews = null;
            private Node[] children = new Node[4];

            public Node(int level, double minLat, double minLon, double maxLat, double maxLon) {
                super(level, minLat, minLon, maxLat, maxLon);
            }
        }

        private Node root = new Node(0, Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON);
        private HashMap<String, String> city2Country = new HashMap<>();
        private int numViews = 0;

        public QuadTree(List<Tuple4<String, String, String, QuadTreeNode>> materializedNodes) {
            for (Tuple4<String, String, String, QuadTreeNode> target : materializedNodes) {
                String group = target._1();
                String aggrFunc = target._2();
                QuadTreeNode targetNode = target._4();
                QueryView materializedView = null;

                if (aggrFunc.equals(Constants.AGGR_COUNT)) {
                    materializedView = new QueryView(numViews++, aggrFunc, group,
                            targetNode.minLat, targetNode.minLon,
                            targetNode.maxLat, targetNode.maxLon);
                } else {
                    String text = target._3();
                    List<String> keyList = new ArrayList<>();
                    for (String term : text.split(" "))
                        keyList.add(term);
                    Set<String> keys = new HashSet<>(keyList);
                    materializedView = new QueryView(numViews++, aggrFunc, group,
                            targetNode.minLat, targetNode.minLon,
                            targetNode.maxLat, targetNode.maxLon,
                            keys);
                }

                materializeNode(root, targetNode, materializedView);
            }
        }

        public void updateView(SpatioTextualObject object) {
            if (root.isInsideNodeRange(object.getCoord())) {
                updateDependency(object);
                updateView(root, object);
            }
        }

        private void updateView(Node node, SpatioTextualObject object) {
            for (QueryView view : node.countViews.values())
                view.updateView(object);

            for (QueryView view : node.topKViews.values())
                view.updateView(object);

            if (isLeaf(node))
                return;

            for (Node child : node.children) {
                if (child.isInsideNodeRange(object.getCoord())) {
                    updateView(child, object);
                    break;
                }
            }
        }

        public void updateDependency(SpatioTextualObject object) {
            String city = object.getCity();
            String country = object.getCountry();
            city2Country.put(city, country);
        }

        public Map<String, Map<String, Integer>> getWordCountAggregation(WarehouseQuery query,
                                                                         Optional<List<String>> ancestors) {
            QueryView view = root.topKViews.get(query.getGroupBy());
            if (view != null) {
                Map<String, AggrValues> aggrMap = view.getResult();
                Map<String, Map<String, Integer>> result = new HashMap<>();
                for (Map.Entry<String, AggrValues> entry : aggrMap.entrySet()) {
                    String group = entry.getKey();
                    String[] termAndFreqArray = entry.getValue().
                            getResult(query.getkOfTopK()).split(" ");

                    Map<String, Integer> resultVals = new HashMap<>();
                    for (String termAndFreq : termAndFreqArray) {
                        String[] pair = termAndFreq.split(":");
                        resultVals.put(pair[0], Integer.valueOf(pair[1]));
                    }
                    result.put(group, resultVals);
                }

                return result;
            } else if (ancestors.isPresent()) {
                String optAncestor = null;
                HashMap<String, Integer> optAncestorMap = null;
                int minSize = Integer.MAX_VALUE;
                for (String ancestor : ancestors.get()) {
                    QueryView ancestorView = root.countViews.get(ancestor);
                    if (ancestorView != null && ancestorView.getViewSize() < minSize) {
                        optAncestor = ancestor;
                        minSize = ancestorView.getViewSize();
                    }
                }

                return getWordCountAggregationMapFromAncestor(optAncestor, query);
            }

            return null;
        }

        private Map<String, Map<String, Integer>> getWordCountAggregationMapFromAncestor(
                String ancestor,
                WarehouseQuery query) {
            Map<String, Map<String, Integer>> result = new HashMap<>();
            String child = query.getGroupBy();
            Map<String, AggrValues> ancestorMap = root.topKViews.
                    get(ancestor).getResult();
            if (child.equals("none")) {
                Map<String, Integer> resultValues = new HashMap<>();
                for (Map.Entry<String, AggrValues> entry : ancestorMap.entrySet()) {
                    String[] termAndFreqArray = entry.getValue().
                            getResult(query.getkOfTopK()).split(" ");
                    for (String termAndFreq : termAndFreqArray)
                        resultValues.put(termAndFreq.split(":")[0],
                                Integer.valueOf(termAndFreq.split(":")[1]));
                }

                result.put("none", resultValues);
                return result;
            }

            String[] attrsAncestor = ancestor.split(" ");
            String[] attrsChild = child.split(" ");
            String[] attrsAn = {"", "", ""};
            String[] attrsCh = {"", "", ""};
            for (String attr : attrsAncestor) {
                if (spaceDimension.contains(attr))
                    attrsAn[0] = attr;
                else if (timeDimension.contains(attr))
                    attrsAn[1] = attr;
                else if (textDimension.contains(attr))
                    attrsAn[2] = attr;
            }
            for (String attr : attrsChild) {
                if (spaceDimension.contains(attr))
                    attrsCh[0] = attr;
                else if (timeDimension.contains(attr))
                    attrsCh[1] = attr;
                else if (textDimension.contains(attr))
                    attrsCh[2] = attr;
            }

            int[] idxesAn = {-1, -1, -1};
            if (!attrsAn[0].isEmpty()) {
                idxesAn[0] = 0;
                if (!attrsAn[1].isEmpty()) {
                    idxesAn[1] = 1;
                    if (!attrsAn[2].isEmpty())
                        idxesAn[2] = 2;
                } else if (!attrsAn[2].isEmpty())
                    idxesAn[2] = 1;
            }
            if (attrsAn[0].isEmpty() && !attrsAn[1].isEmpty()) {
                idxesAn[1] = 0;
                if (!attrsAn[2].isEmpty())
                    idxesAn[2] = 1;
            }
            if (attrsAn[0].isEmpty() && attrsAn[1].isEmpty() && !attrsAn[2].isEmpty())
                idxesAn[2] = 0;

            // 0: "", 1: same as the ancestor, 2: obtained from the ancestor
            int flagChildSpace = 0, flagChildTime = 0, flagChildText = 0;
            int endPosTime = 0;

            if (attrsAn[0].equals(attrsCh[0]))
                flagChildSpace = 1;
            else if (attrsAn[0].equals("city") && attrsCh[0].equals("country"))
                flagChildSpace = 2;

            if (attrsAn[1].equals(attrsCh[1]))
                flagChildTime = 1;
            else if (attrsAn[1].equals("hour") && attrsCh[1].equals("day")) {
                flagChildTime = 2;
                endPosTime = 8;
            } else if (attrsAn[1].equals("hour") && attrsCh[1].equals("week")) {
                flagChildTime = 2;
                endPosTime = -1;
            } else if (attrsAn[1].equals("hour") && attrsCh[1].equals("month")) {
                flagChildTime = 2;
                endPosTime = 6;
            } else if (attrsAn[1].equals("day") && attrsCh[1].equals("week")) {
                flagChildTime = 2;
                endPosTime = -1;
            } else if (attrsAn[1].equals("day") && attrsCh[1].equals("month")) {
                flagChildTime = 2;
                endPosTime = 6;
            }

            if (attrsAn[2].equals(attrsCh[2]))
                flagChildText = 1;

            final String separator = SpatioTextualObject.DIMENSION_SEPARATOR;
            for (Map.Entry<String, AggrValues> entry : ancestorMap.entrySet()) {
                String[] splitList = entry.getKey().split(SpatioTextualObject.DIMENSION_SEPARATOR);
                String ancestorSpace = idxesAn[0] >= 0 ? splitList[idxesAn[0]] : "";
                String ancestorTime = idxesAn[1] >= 0 ? splitList[idxesAn[1]] : "";
                String ancestorText = idxesAn[2] >= 0 ? splitList[idxesAn[2]] : "";

                String childSpace = "", childTime = "", childText = "";

                if (flagChildSpace == 1)
                    childSpace = ancestorSpace;
                else if (flagChildSpace == 2)
                    childSpace = city2Country.get(ancestorSpace);

                if (flagChildTime == 1)
                    childTime = ancestorTime;
                else if (flagChildTime == 2) {
                    if (endPosTime > 0)
                        childTime = ancestorTime.substring(0, endPosTime);
                    else {
                        String year = ancestorTime.substring(0, 4);
                        String month = ancestorTime.substring(4, 6);
                        String formattedDate = year + "-" + month + "-" + ancestorTime.substring(6, 8) + " 00:00:00";

                        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        LocalDateTime dateTime = LocalDateTime.parse(formattedDate, format);
                        TemporalField weekOfYear = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear();
                        String week = year + String.valueOf(dateTime.get(weekOfYear));
                        childTime = week;
                    }
                }

                if (flagChildText == 1)
                    childText = ancestorText;

                StringBuilder childKey = new StringBuilder();
                if (!childSpace.isEmpty())
                    childKey.append(childSpace);
                if (!childTime.isEmpty()) {
                    if (childKey.length() != 0)
                        childKey.append(separator + childTime);
                    else
                        childKey.append(childTime);
                }
                if (!childText.isEmpty()) {
                    if (childKey.length() != 0)
                        childKey.append(separator + childText);
                    else
                        childKey.append(childText);
                }

                Map<String, Integer> resultValue =
                        result.getOrDefault(childKey.toString(), new HashMap<>());

                String[] termAndFreqArray = entry.getValue().
                        getResult(query.getkOfTopK()).split(" ");
                for (String termAndFreq : termAndFreqArray) {
                    String term = termAndFreq.split(":")[0];
                    int freq = Integer.valueOf(termAndFreq.split(":")[1]);

                    int newFreq = resultValue.getOrDefault(term, 0) + freq;
                    resultValue.put(term, newFreq);
                }

                result.put(childKey.toString(), resultValue);
            }

            return result;
        }

        public Map<String, Map<String, Integer>> getWordCountAggregationByRange(WarehouseQuery query,
                                                                                Optional<List<String>> ancestors,
                                                                                ObjectCacheManager objectCacheManager) {
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

            Tuple4<Double, Double, Double, Double> range = query.getRange().get();
            Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
            Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());
            List<SpatioTextualObject> objects =
                    rawData.getObjectsByRange(cornerFrom, cornerTo, objectCacheManager).orElse(new ArrayList<>());
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

            return result;
        }

        private Map<String, Integer> getCountAggregationMapFromAncestor(String ancestor,
                                                        WarehouseQuery query) {
            Map<String, Integer> result = new HashMap<>();
            String child = query.getGroupBy();
            Map<String, AggrValues> ancestorMap = root.countViews.
                    get(ancestor).getResult();
            if (child.equals("none")) {
                for (Map.Entry<String, AggrValues> entry : ancestorMap.entrySet()) {
                    int count = result.getOrDefault("none", 0);
                    result.put("none", count +
                            Integer.valueOf(entry.getValue().getResult(0)));
                }
                return result;
            }

            String[] attrsAncestor = ancestor.split(" ");
            String[] attrsChild = child.split(" ");
            String[] attrsAn = {"", "", ""};
            String[] attrsCh = {"", "", ""};
            for (String attr : attrsAncestor) {
                if (spaceDimension.contains(attr))
                    attrsAn[0] = attr;
                else if (timeDimension.contains(attr))
                    attrsAn[1] = attr;
                else if (textDimension.contains(attr))
                    attrsAn[2] = attr;
            }
            for (String attr : attrsChild) {
                if (spaceDimension.contains(attr))
                    attrsCh[0] = attr;
                else if (timeDimension.contains(attr))
                    attrsCh[1] = attr;
                else if (textDimension.contains(attr))
                    attrsCh[2] = attr;
            }

            int[] idxesAn = {-1, -1, -1};
            if (!attrsAn[0].isEmpty()) {
                idxesAn[0] = 0;
                if (!attrsAn[1].isEmpty()) {
                    idxesAn[1] = 1;
                    if (!attrsAn[2].isEmpty())
                        idxesAn[2] = 2;
                } else if (!attrsAn[2].isEmpty())
                    idxesAn[2] = 1;
            }
            if (attrsAn[0].isEmpty() && !attrsAn[1].isEmpty()) {
                idxesAn[1] = 0;
                if (!attrsAn[2].isEmpty())
                    idxesAn[2] = 1;
            }
            if (attrsAn[0].isEmpty() && attrsAn[1].isEmpty() && !attrsAn[2].isEmpty())
                idxesAn[2] = 0;

            // 0: "", 1: same as the ancestor, 2: obtained from the ancestor
            int flagChildSpace = 0, flagChildTime = 0, flagChildText = 0;
            int endPosTime = 0;

            if (attrsAn[0].equals(attrsCh[0]))
                flagChildSpace = 1;
            else if (attrsAn[0].equals("city") && attrsCh[0].equals("country"))
                flagChildSpace = 2;

            if (attrsAn[1].equals(attrsCh[1]))
                flagChildTime = 1;
            else if (attrsAn[1].equals("hour") && attrsCh[1].equals("day")) {
                flagChildTime = 2;
                endPosTime = 8;
            } else if (attrsAn[1].equals("hour") && attrsCh[1].equals("week")) {
                flagChildTime = 2;
                endPosTime = -1;
            } else if (attrsAn[1].equals("hour") && attrsCh[1].equals("month")) {
                flagChildTime = 2;
                endPosTime = 6;
            } else if (attrsAn[1].equals("day") && attrsCh[1].equals("week")) {
                flagChildTime = 2;
                endPosTime = -1;
            } else if (attrsAn[1].equals("day") && attrsCh[1].equals("month")) {
                flagChildTime = 2;
                endPosTime = 6;
            }

            if (attrsAn[2].equals(attrsCh[2]))
                flagChildText = 1;

            final String separator = SpatioTextualObject.DIMENSION_SEPARATOR;
            for (Map.Entry<String, AggrValues> entry : ancestorMap.entrySet()) {
                String[] splitList = entry.getKey().split(SpatioTextualObject.DIMENSION_SEPARATOR);
                String ancestorSpace = idxesAn[0] >= 0 ? splitList[idxesAn[0]] : "";
                String ancestorTime = idxesAn[1] >= 0 ? splitList[idxesAn[1]] : "";
                String ancestorText = idxesAn[2] >= 0 ? splitList[idxesAn[2]] : "";

                String childSpace = "", childTime = "", childText = "";

                if (flagChildSpace == 1)
                    childSpace = ancestorSpace;
                else if (flagChildSpace == 2)
                    childSpace = city2Country.get(ancestorSpace);

                if (flagChildTime == 1)
                    childTime = ancestorTime;
                else if (flagChildTime == 2) {
                    if (endPosTime > 0)
                        childTime = ancestorTime.substring(0, endPosTime);
                    else {
                        String year = ancestorTime.substring(0, 4);
                        String month = ancestorTime.substring(4, 6);
                        String formattedDate = year + "-" + month + "-" + ancestorTime.substring(6, 8) + " 00:00:00";

                        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        LocalDateTime dateTime = LocalDateTime.parse(formattedDate, format);
                        TemporalField weekOfYear = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear();
                        String week = year + String.valueOf(dateTime.get(weekOfYear));
                        childTime = week;
                    }
                }

                if (flagChildText == 1)
                    childText = ancestorText;

                StringBuilder childKey = new StringBuilder();
                if (!childSpace.isEmpty())
                    childKey.append(childSpace);
                if (!childTime.isEmpty()) {
                    if (childKey.length() != 0)
                        childKey.append(separator + childTime);
                    else
                        childKey.append(childTime);
                }
                if (!childText.isEmpty()) {
                    if (childKey.length() != 0)
                        childKey.append(separator + childText);
                    else
                        childKey.append(childText);
                }

                int count = result.getOrDefault(childKey.toString(), 0);
                result.put(childKey.toString(),
                        count + Integer.valueOf(entry.getValue().getResult(0)));
            }

            return result;
        }

        public Map<String, Integer> getCountAggregation(WarehouseQuery query,
                                                        Optional<List<String>> ancestors) {
            QueryView view = root.countViews.get(query.getGroupBy());
            if (view != null) {
                Map<String, AggrValues> aggrMap = view.getResult();
                Map<String, Integer> res = new HashMap<>();
                for (Map.Entry<String, AggrValues> entry : aggrMap.entrySet()) {
                    String group = entry.getKey();
                    int val = Integer.valueOf(entry.getValue().getResult(0));
                    res.put(group, val);
                }
                return res;
            } else if (ancestors.isPresent()) {
                    String optAncestor = null;
                    HashMap<String, Integer> optAncestorMap = null;
                    int minSize = Integer.MAX_VALUE;
                    for (String ancestor : ancestors.get()) {
                        QueryView ancestorView = root.countViews.get(ancestor);
                        if (ancestorView != null && ancestorView.getViewSize() < minSize) {
                            optAncestor = ancestor;
                            minSize = ancestorView.getViewSize();
                        }
                    }

                    return getCountAggregationMapFromAncestor(optAncestor, query);
                }

            return null;
        }

        public Map<String, Integer> getCountAggregationByRange(WarehouseQuery query,
                                                  Optional<List<String>> ancestors) {
            Tuple4<Double, Double, Double, Double> range = query.getRange().get();
            Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
            Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());
            if (!root.isOverlappedNodeRange(cornerFrom, cornerTo))
                return null;

            Map<String, Integer> result = new HashMap<>();
            getCountAggregationByRange(root, result, ancestors, query, cornerFrom, cornerTo);

            return result;
        }

        public Map<String, Integer> getCountAggregationByRange(WarehouseQuery query,
                                                               Optional<List<String>> ancestors,
                                                               ObjectCacheManager objectCacheManager) {
            Tuple4<Double, Double, Double, Double> range = query.getRange().get();
            Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
            Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());
            if (!root.isOverlappedNodeRange(cornerFrom, cornerTo))
                return null;

            Map<String, Integer> result = new HashMap<>();
            getCountAggregationByRange(root, result, ancestors, query, objectCacheManager, cornerFrom, cornerTo);

            return result;
        }

        private void getCountAggregationByRange(Node node, Map<String, Integer> result,
                                                Optional<List<String>> ancestors,
                                                WarehouseQuery query,
                                                Tuple2<Double, Double> cornerFrom,
                                                Tuple2<Double, Double> cornerTo) {
            if (node.countViews.get(query.getGroupBy()) != null
                    && node.isEnclosingNodeRange(cornerFrom, cornerTo)) {
                Map<String, AggrValues> aggrMap = node.countViews.get(query.getGroupBy()).getResult();
                for (Map.Entry<String, AggrValues> entry : aggrMap.entrySet()) {
                    String key = entry.getKey();
                    int val = Integer.valueOf(entry.getValue().getResult(0));
                    int count = result.getOrDefault(key, 0) + val;
                    result.put(key, count);
                }

                return;
            } else if (ancestors.isPresent()) {
                String optAncestor = null;
                int minSize = Integer.MAX_VALUE;
                for (String ancestor : ancestors.get()) {
                    QueryView candView = node.countViews.get(ancestor);
                    if (candView != null && candView.getViewSize() < minSize) {
                        optAncestor = ancestor;
                        minSize = candView.getViewSize();
                    }
                }

                if (optAncestor != null) {
                    Map<String, Integer> ancestorResult = getCountAggregationMapFromAncestor(
                            optAncestor, query);
                    for (Map.Entry<String, Integer> entry : ancestorResult.entrySet()) {
                        String key = entry.getKey();
                        int val = entry.getValue();
                        int newVal = result.getOrDefault(key, 0) + val;
                        result.put(key, newVal);
                    }

                    return;
                }
            }

            if (isLeaf(node)) {
                List<SpatioTextualObject> objects
                        = rawData.getObjectsByRange(cornerFrom, cornerTo).orElse(new ArrayList<>());
                for (SpatioTextualObject object : objects) {
                    String combinedDimValue = object.getGroupBy(query.getGroupBy());
                    int count = result.getOrDefault(combinedDimValue, 0);
                    result.put(combinedDimValue, count + 1);
                }
                return;
            }

            for (Node child : node.children) {
                if (child.isOverlappedNodeRange(cornerFrom, cornerTo))
                    getCountAggregationByRange(child, result, ancestors, query, cornerFrom, cornerTo);
            }
        }

        private void getCountAggregationByRange(Node node, Map<String, Integer> result,
                                                Optional<List<String>> ancestors,
                                                WarehouseQuery query,
                                                ObjectCacheManager objectCacheManager,
                                                Tuple2<Double, Double> cornerFrom,
                                                Tuple2<Double, Double> cornerTo) {
            if (node.countViews.get(query.getGroupBy()) != null
                    && node.isEnclosingNodeRange(cornerFrom, cornerTo)) {
                Map<String, AggrValues> aggrMap = node.countViews.get(query.getGroupBy()).getResult();
                for (Map.Entry<String, AggrValues> entry : aggrMap.entrySet()) {
                    String key = entry.getKey();
                    int val = Integer.valueOf(entry.getValue().getResult(0));
                    int count = result.getOrDefault(key, 0) + val;
                    result.put(key, count);
                }

                return;
            } else if (ancestors.isPresent()) {
                String optAncestor = null;
                int minSize = Integer.MAX_VALUE;
                for (String ancestor : ancestors.get()) {
                    QueryView candView = node.countViews.get(ancestor);
                    if (candView != null && candView.getViewSize() < minSize) {
                        optAncestor = ancestor;
                        minSize = candView.getViewSize();
                    }
                }

                if (optAncestor != null) {
                    Map<String, Integer> ancestorResult = getCountAggregationMapFromAncestor(
                            optAncestor, query);
                    for (Map.Entry<String, Integer> entry : ancestorResult.entrySet()) {
                        String key = entry.getKey();
                        int val = entry.getValue();
                        int newVal = result.getOrDefault(key, 0) + val;
                        result.put(key, newVal);
                    }

                    return;
                }
            }

            if (isLeaf(node)) {
                List<Integer> leafNodeIds = rawData.getLeafNodeIdsByRange(cornerFrom, cornerTo);
                Set<Integer> unCachedLeafNodeIds = new HashSet<>();
                for (Integer leafId : leafNodeIds) {
                    List<SpatioTextualObject> cachedObjects = objectCacheManager.
                            getObjectsUsingCache(rawData, leafId, cornerFrom, cornerTo);

                    if (!cachedObjects.isEmpty()) {
                        for (SpatioTextualObject object : cachedObjects) {
                            String combinedDimValue = object.getGroupBy(query.getGroupBy());
                            int count = result.getOrDefault(combinedDimValue, 0);
                            result.put(combinedDimValue, count + 1);
                        }
                    } else {
                        unCachedLeafNodeIds.add(leafId);
                    }
                }

                if (!unCachedLeafNodeIds.isEmpty()) {
                    List<SpatioTextualObject> objects = rawData.
                            getObjectsByRange(cornerFrom, cornerTo, unCachedLeafNodeIds).
                            orElse(new ArrayList<>());
                    for (SpatioTextualObject object : objects) {
                        String combinedDimValue = object.getGroupBy(query.getGroupBy());
                        int count = result.getOrDefault(combinedDimValue, 0);
                        result.put(combinedDimValue, count + 1);
                    }
                }

                return;
            }

            for (Node child : node.children) {
                if (child.isOverlappedNodeRange(cornerFrom, cornerTo))
                    getCountAggregationByRange(child, result, ancestors, query, cornerFrom, cornerTo);
            }
        }

        private void materializeNode(Node node, QuadTreeNode targetNode, QueryView view) {
            if (node.equals(targetNode)) {
                String aggrFunc = view.getAggrFunc();
                String group = view.getGroupBy();
                if (aggrFunc.equals(Constants.AGGR_COUNT) && node.countViews == null)
                    node.countViews = new HashMap<>();
                else if (aggrFunc.equals(Constants.AGGR_TOPK) && node.topKViews == null)
                    node.topKViews = new HashMap<>();

                if (aggrFunc.equals(Constants.AGGR_COUNT))
                    node.countViews.put(group, view);
                else
                    node.topKViews.put(group, view);

                return;
            }

            if (isLeaf(node)) {
                double middleLat = (node.minLat + node.maxLat) / 2.0;
                double middleLon = (node.minLon + node.maxLon) / 2.0;

                node.children[0] = new Node(node.level + 1, node.minLat, node.minLon, middleLat, middleLon);
                node.children[1] = new Node(node.level + 1, node.minLat, middleLon, middleLat, node.maxLon);
                node.children[2] = new Node(node.level + 1, middleLat, node.minLon, node.maxLat, middleLon);
                node.children[3] = new Node(node.level + 1, middleLat, middleLon, node.maxLat, node.maxLon);
            }

            for (Node child : node.children) {
                if (child.isInsideNodeRange(new Tuple2<>(targetNode.minLat, targetNode.minLon))) {
                    materializeNode(child, targetNode, view);
                    break;
                }
            }
        }

        private boolean isLeaf(Node node) {
            return node.children[0] == null;
        }
    }

    public static final long serialVersionUID = 1002L;
    private RawData rawData = new RawData();
    private ObjectCacheManager objectCacheManager = new ObjectCacheManager();
    private QuadTree quadTree = null;
    private List<Tuple4<String, String, String, QuadTreeNode>> materializedPairsOfGroupAndNode = null;
    private List<String> spaceDimension = Arrays.asList("city", "country");
    private List<String> timeDimension = Arrays.asList("hour", "day", "week", "month");
    private List<String> textDimension = Arrays.asList("topic");
    private List<Tuple2<String, String>> spaceDependencies;
    private List<Tuple2<String, String>> timeDependencies;
    private List<Tuple2<String, String>> textDependencies;
    private List<Tuple2<String, String>> groupDependencies;
    private HashMap<String, Integer> group2Level;

    public QuadTreeCache() {
        setSpaceDependencies();
        setTimeDependencies();
        setTextDependencies();
        setGroupDependenciesAndGroup2Level();
    }

    public void readMaterializedCubesFromFile(String fileName) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(fileName);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        materializedPairsOfGroupAndNode = (List<Tuple4<String, String, String, QuadTreeNode>>)in.readObject();
        quadTree = new QuadTree(materializedPairsOfGroupAndNode);
        in.close();
        fileIn.close();
    }

    public void addObject(SpatioTextualObject object) {
        int[] nodeId = {-1};
        rawData.addObject(object, nodeId);
        objectCacheManager.addObject2Cache(nodeId[0], object);
        quadTree.updateView(object);

        List<String> hours = rawData.getHours();
        if (hours.size() > Constants.NUM_HOURS_DATA_KEPT) {
            int oldestHour = Integer.MAX_VALUE;
            for (int i = 0; i < hours.size(); ++i) {
                int currentHour = Integer.valueOf(hours.get(i));
                if (currentHour < oldestHour)
                    oldestHour = currentHour;
            }
            rawData.removeDataAtHour(String.valueOf(oldestHour));
        }
    }

    public Map<String, Integer> processCountQuery(WarehouseQuery query) {
        if (query.getRange().isPresent()) {
            Tuple4<Double, Double, Double, Double> range =
                    query.getRange().get();
            Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
            Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());
            return getCountAggregationByRange(query);
        } else {
            return getCountAggregation(query);
        }
    }

    public Map<String, Integer> getCountAggregation(WarehouseQuery query) {
        Optional<List<String>> ancestors = getAncestorGroups(query.getGroupBy());
        return quadTree.getCountAggregation(query, ancestors);
    }

    private Map<String, Integer> getCountAggregationByRange(WarehouseQuery query) {
        Optional<List<String>> ancestors = getAncestorGroups(query.getGroupBy());
        return quadTree.getCountAggregationByRange(query, ancestors, objectCacheManager);
    }

    public Map<String, Map<String, Integer>> processWordCountQuery(WarehouseQuery query) {
        if (query.getRange().isPresent()) {
            Tuple4<Double, Double, Double, Double> range =
                    query.getRange().get();
            Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
            Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());
            return getWordCountAggregationByRange(query);
        } else {
            return getWordCountAggregation(query);
        }
    }

    public Map<String, Map<String, Integer>> getWordCountAggregationByRange(WarehouseQuery query) {
        Optional<List<String>> ancestors = getAncestorGroups(query.getGroupBy());
        return quadTree.getWordCountAggregationByRange(query, ancestors, objectCacheManager);
    }

    public Map<String, Map<String, Integer>> getWordCountAggregation(WarehouseQuery query) {
        Optional<List<String>> ancestors = getAncestorGroups(query.getGroupBy());
        return quadTree.getWordCountAggregation(query, ancestors);
    }

    public void addObjects(List<SpatioTextualObject> objects) {
        for (SpatioTextualObject object : objects)
            addObject(object);

    }

    private boolean isAncestorOf(String ancestorGroup, String childGroup) {
        if (ancestorGroup.equals(childGroup))
            return false;
        else if (childGroup.equals("none"))
            return true;

        List<String> spaceDimension = Arrays.asList("city", "country");
        List<String> timeDimension = Arrays.asList("hour", "day", "week", "month");
        List<String> textDimension = Arrays.asList("topic");

        String[] attrsAncestor = ancestorGroup.split(" ");
        String[] attrsChild = childGroup.split(" ");

        List<String> parsedAttrsAncestor = Arrays.asList("", "", "");
        for (String attr : attrsAncestor) {
            if (spaceDimension.contains(attr))
                parsedAttrsAncestor.set(0, attr);
            else if (timeDimension.contains(attr))
                parsedAttrsAncestor.set(1, attr);
            else if (textDimension.contains(attr))
                parsedAttrsAncestor.set(2, attr);
        }

        List<String> parsedAttrsChild = Arrays.asList("", "", "");
        for (String attr : attrsChild) {
            if (spaceDimension.contains(attr))
                parsedAttrsChild.set(0, attr);
            else if (timeDimension.contains(attr))
                parsedAttrsChild.set(1, attr);
            else if (textDimension.contains(attr))
                parsedAttrsChild.set(2, attr);
        }

        String[] attrsAn = parsedAttrsAncestor.toArray(new String[0]);
        String[] attrsCh = parsedAttrsChild.toArray(new String[0]);

        if ((attrsAn[0].equals("") && attrsCh[0].equals("country")) ||
                (attrsAn[0].equals("") && attrsCh[0].equals("city")) ||
                (attrsAn[0].equals("country") && attrsCh[0].equals("city")))
            return false;
        if ((attrsAn[1].equals("") && attrsCh[1].equals("month")) ||
                (attrsAn[1].equals("") && attrsCh[1].equals("week")) ||
                (attrsAn[1].equals("") && attrsCh[1].equals("day")) ||
                (attrsAn[1].equals("") && attrsCh[1].equals("hour")) ||
                (attrsAn[1].equals("month") && attrsCh[1].equals("day")) ||
                (attrsAn[1].equals("month") && attrsCh[1].equals("hour")) ||
                (attrsAn[1].equals("week") && attrsCh[1].equals("day")) ||
                (attrsAn[1].equals("week") && attrsCh[1].equals("hour")) ||
                (attrsAn[1].equals("week") && attrsCh[1].equals("month")) ||
                (attrsAn[1].equals("month") && attrsCh[1].equals("week")) ||
                (attrsAn[1].equals("day") && attrsCh[1].equals("hour")))
            return false;
        if (attrsAn[2].equals("") && attrsCh[2].equals("topic"))
            return false;

        return true;
    }

    private Optional<List<String>> getAncestorGroups(String child) {
        List<String> ancestors = Arrays.stream(Constants.GROUPS)
                .filter(ancestor -> isAncestorOf(ancestor, child))
                .collect(Collectors.toList());

        return ancestors.isEmpty() ? Optional.empty() : Optional.of(ancestors);
    }

    private void setSpaceDependencies() {
        Tuple2<String, String> d1 = new Tuple2<>("city", "country");
        Tuple2<String, String> d2 = new Tuple2<>("country", "none");

        spaceDependencies = Arrays.asList(d1, d2);
    }

    private void setTimeDependencies() {
        Tuple2<String, String> d1 = new Tuple2<>("hour", "day");
        Tuple2<String, String> d2 = new Tuple2<>("day", "week");
        Tuple2<String, String> d3 = new Tuple2<>("day", "month");
        Tuple2<String, String> d4 = new Tuple2<>("week", "none");
        Tuple2<String, String> d5 = new Tuple2<>("month", "none");

        timeDependencies = Arrays.asList(d1, d2, d3, d4, d5);
    }

    private void setTextDependencies() {
        Tuple2<String, String> d1 = new Tuple2<>("topic", "none");

        textDependencies = Arrays.asList(d1);
    }

    private void setGroupDependenciesAndGroup2Level() {
        Queue<String> groups2Visit = new LinkedList<>();
        List<Tuple2<String, String>> dependencies = new ArrayList<>();
        HashMap<String, Integer> group2LevelMap = new HashMap<>();
        HashSet<String> visitedGroups = new HashSet<>();

        String root = Constants.ROOT.replaceAll(" ", ",");
        groups2Visit.add(root);
        group2LevelMap.put(root, 0);
        setGroupDependenciesAndGroup2Level(groups2Visit, dependencies, group2LevelMap, visitedGroups);

        groupDependencies =
                dependencies.stream().map(d -> {
                    String from = d._1();
                    String to = d._2();
                    List<String> array1 = Arrays.asList(from, to);
                    List<String> array2 = new ArrayList<>();
                    array1.forEach(s -> {
                        String tmp =
                                s.replaceAll(",", " ")
                                        .replaceAll("none", " ")
                                        .trim()
                                        .replaceAll(" +", " ");
                        array2.add(tmp.isEmpty() ? "none" : tmp);
                    });

                    return new Tuple2<>(array2.get(0), array2.get(1));
                }).collect(Collectors.toList());

        group2Level = new HashMap<>();
        for (Map.Entry<String, Integer> entry : group2LevelMap.entrySet()) {
            String key = entry.getKey();
            int val = entry.getValue();
            key = key.replaceAll(",", " ")
                    .replaceAll("none", " ")
                    .trim()
                    .replaceAll(" +", " ");
            if (key.isEmpty())
                key = "none";
            group2Level.put(key, val);
        }
    }

    private void setGroupDependenciesAndGroup2Level(Queue<String> groups2Visit,
                                                    List<Tuple2<String, String>> dependencies,
                                                    HashMap<String, Integer> group2LevelMap,
                                                    HashSet<String> visitedGroups) {
        String group = groups2Visit.poll();
        visitedGroups.add(group);
        String[] attrs = group.split(",");
        String spaceAttr = attrs[0];
        String timeAttr = attrs[1];
        String textAttr = attrs[2];

        List<String> spaceChildren = spaceDependencies.stream()
                .filter(e -> {
                    String from = e._1();
                    String to = e._2();
                    return from.equals(spaceAttr);
                })
                .map(e -> e._2())
                .collect(Collectors.toList());
        List<String> timeChildren = timeDependencies.stream()
                .filter(e -> {
                    String from = e._1();
                    String to = e._2();
                    return from.equals(timeAttr);
                })
                .map(e -> e._2())
                .collect(Collectors.toList());
        List<String> textChildren = textDependencies.stream()
                .filter(e -> {
                    String from = e._1();
                    String to = e._2();
                    return from.equals(textAttr);
                })
                .map(e -> e._2())
                .collect(Collectors.toList());

        List<String> children = new ArrayList<>();
        spaceChildren.forEach(space -> {
            String child = space + "," + timeAttr + "," + textAttr;
            children.add(child);
        });
        timeChildren.forEach(time -> {
            String label = spaceAttr + "," + time + "," + textAttr;
            children.add(label);
        });
        textChildren.forEach(text -> {
            String label = spaceAttr + "," + timeAttr + "," + text;
            children.add(label);
        });

        if (children.isEmpty())
            return;

        int level = group2LevelMap.get(group) + 1;
        children.forEach(c -> {
            if (!visitedGroups.contains(c))
                groups2Visit.add(c);

            Integer childLevel = group2LevelMap.get(c);
            if (childLevel == null || level < childLevel)
                group2LevelMap.put(c, level);

            Tuple2<String, String> dependency = new Tuple2<>(group, c);
            if (!dependencies.contains(dependency))
                dependencies.add(dependency);
        });

        setGroupDependenciesAndGroup2Level(groups2Visit, dependencies, group2LevelMap, visitedGroups);
    }
}
