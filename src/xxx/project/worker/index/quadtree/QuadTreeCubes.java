package xxx.project.worker.index.quadtree;

import xxx.project.util.*;
import xxx.project.worker.index.Index;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.stream.Collectors;

public class QuadTreeCubes implements Index, Serializable {
    public static final long serialVersionUID = 102L;

    private class QuadTree implements Serializable {
        public static final long serialVersionUID = 103L;

        private class Node extends QuadTreeNode {
            public HashMap<String, HashMap<String, Integer>> group2Aggregation = null;
            private Node[] children = new Node[4];

            public Node(int level, double minLat, double minLon, double maxLat, double maxLon) {
                super(level, minLat, minLon, maxLat, maxLon);
            }
        }

        private Node root = new Node(0, Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON);
        private HashMap<String, String> city2Country = new HashMap<>();

        public QuadTree(RawData.QuadTree rawDataQuadTree) {
            root = copyNode(rawDataQuadTree.root);
        }

        private Node copyNode(RawData.RawDataQuadTreeNode source) {
            Node target = new Node(source.level, source.minLat, source.minLon, source.maxLat, source.maxLon);
            if (source.children[0] != null) {
                for (int i = 0; i < source.children.length; ++i)
                    target.children[i] = copyNode(source.children[i]);
            }

            return target;
        }

        public QuadTree(List<Tuple2<String, QuadTreeNode>> materializedNodes) {
            for (Tuple2<String, QuadTreeNode> target : materializedNodes) {
                String group = target._1();
                QuadTreeNode targetNode = target._2();
                materializeNode(root, group, targetNode);
            }
        }

        public void updateCube(SpatioTextualObject object) {
            if (root.isInsideNodeRange(object.getCoord())) {
                updateDependency(object);
                updateCube(root, object);
            }
        }

        private void updateCube(Node node, SpatioTextualObject object) {
            if (node.group2Aggregation != null) {
                for (String group : node.group2Aggregation.keySet()) {
                    HashMap<String, Integer> combinedValue2Count =
                            node.group2Aggregation.get(group);
                    String combinedDimValue = object.getGroupBy(group);
                    int count = combinedValue2Count.getOrDefault(combinedDimValue, 0);
                    combinedValue2Count.put(combinedDimValue, count + 1);
                }
            }

            if (isLeaf(node))
                return;

            for (Node child : node.children) {
                if (child.isInsideNodeRange(object.getCoord())) {
                    updateCube(child, object);
                    break;
                }
            }
        }

        public void updateDependency(SpatioTextualObject object) {
            String city = object.getCity();
            String country = object.getCountry();
            city2Country.put(city, country);
        }

        private void setAggregationMapFromAncestor(String ancestor, String child,
                                                   Map<String, Integer> ancestorMap,
                                                   Map<String, Integer> result) {
            if (child.equals("none")) {
                for (Map.Entry<String, Integer> entry : ancestorMap.entrySet()) {
                    int count = result.getOrDefault("none", 0);
                    result.put("none", count + entry.getValue());
                }
                return;
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
            for (Map.Entry<String, Integer> entry : ancestorMap.entrySet()) {
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
                result.put(childKey.toString(), count + entry.getValue());
            }
        }

        public boolean getCountAggregation(String group, Optional<List<String>> ancestors,
                                           Map<String, Integer> result) {
            if (root.group2Aggregation != null) {
                HashMap<String, Integer> combinedValue2Count = root.group2Aggregation.get(group);
                if (combinedValue2Count != null) {
                    for (Map.Entry<String, Integer> entry : combinedValue2Count.entrySet()) {
                        String key = entry.getKey();
                        int val = entry.getValue();
                        int count = result.getOrDefault(key, 0) + val;
                        result.put(key, count);
                    }

                    return true;
                } else if (ancestors.isPresent()) {
                    String optAncestor = null;
                    HashMap<String, Integer> optAncestorMap = null;
                    int minSize = Integer.MAX_VALUE;
                    for (String ancestor : ancestors.get()) {
                        HashMap<String, Integer> ancestorMap = root.group2Aggregation.get(ancestor);
                        if (ancestorMap != null && ancestorMap.size() < minSize) {
                            optAncestor = ancestor;
                            optAncestorMap = ancestorMap;
                            minSize = optAncestorMap.size();
                        }
                    }
                    setAggregationMapFromAncestor(optAncestor, group, optAncestorMap, result);

                    return true;
                }
            }

            return false;
        }

        public boolean getCountAggregationByRange(String group, Optional<List<String>> ancestors,
                                                  Map<String, Integer> result, Set<QuadTreeNode> checkedNodes,
                                                  Tuple2<Double, Double> cornerFrom,
                                                  Tuple2<Double, Double> cornerTo) {
            if (!root.isOverlappedNodeRange(cornerFrom, cornerTo))
                return true;

            List<Boolean> isComplete = Arrays.asList(true);
            getCountAggregationByRange(root, group, ancestors, result, isComplete, checkedNodes, cornerFrom, cornerTo);

            return isComplete.get(0);
        }

        private void getCountAggregationByRange(Node node, String group, Optional<List<String>> ancestors,
                                                Map<String, Integer> result,
                                                List<Boolean> isComplete,
                                                Set<QuadTreeNode> checkedNodes,
                                                Tuple2<Double, Double> cornerFrom,
                                                Tuple2<Double, Double> cornerTo) {
            if (node.group2Aggregation != null && node.isEnclosingNodeRange(cornerFrom, cornerTo)) {
                HashMap<String, Integer> combinedValue2Count = node.group2Aggregation.get(group);
                if (combinedValue2Count != null) {
                    for (Map.Entry<String, Integer> entry : combinedValue2Count.entrySet()) {
                        String key = entry.getKey();
                        int val = entry.getValue();
                        int count = result.getOrDefault(key, 0) + val;
                        result.put(key, count);
                    }

                    checkedNodes.add(node);
                    return;
                } else if (ancestors.isPresent()) {
                    String optAncestor = null;
                    HashMap<String, Integer> optAncestorMap = null;
                    int minSize = Integer.MAX_VALUE;
                    for (String ancestor : ancestors.get()) {
                        HashMap<String, Integer> ancestorMap = node.group2Aggregation.get(ancestor);
                        if (ancestorMap != null && ancestorMap.size() < minSize) {
                            optAncestor = ancestor;
                            optAncestorMap = ancestorMap;
                            minSize = optAncestorMap.size();
                        }
                    }

                    if (optAncestor != null) {
                        checkedNodes.add(node);
                        setAggregationMapFromAncestor(optAncestor, group, optAncestorMap, result);
                        return;
                    }
                }
            }

            if (isLeaf(node)) {
                isComplete.set(0, false);
                return;
            }

            for (Node child : node.children) {
                if (child.isOverlappedNodeRange(cornerFrom, cornerTo))
                    getCountAggregationByRange(child, group, ancestors, result, isComplete, checkedNodes, cornerFrom, cornerTo);
            }
        }

        private void materializeNode(Node node, String group, QuadTreeNode targetNode) {
            if (node.equals(targetNode)) {
                if (node.group2Aggregation == null)
                    node.group2Aggregation = new HashMap<>();
                node.group2Aggregation.put(group, new HashMap<>());

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
                    materializeNode(child, group, targetNode);
                    break;
                }
            }
        }

        private boolean isLeaf(Node node) {
            return node.children[0] == null;
        }

        public double getCostsOfQueries(List<WarehouseQuery> queries,
                                        HashMap<String, HashMap<QuadTreeNode, NodeStat>> materializedNodes,
                                        HashMap<String, HashMap<QuadTreeNode, NodeStat>> group2Stats) {
            double sumCosts = 0.0;
            for (WarehouseQuery query : queries) {
                if (!root.isOverlappedNodeRange(query.getRange().get())) {
                    sumCosts += 0.0;
                    continue;
                }

                String group = query.getGroupBy();
                Tuple4<Double, Double, Double, Double> range = query.getRange().get();

                List<String> ancestors = new ArrayList<>();
                for (String ancestor : materializedNodes.keySet())
                    if (isAncestorOf(ancestor, group))
                        ancestors.add(ancestor);

                sumCosts += getCostOfQuery(root, group, range, ancestors, materializedNodes, group2Stats);
            }

            return sumCosts;
        }

        private double getCostOfQuery(Node node, String group, Tuple4<Double, Double, Double, Double> range,
                                      List<String> ancestors,
                                      HashMap<String, HashMap<QuadTreeNode, NodeStat>> materializedNodes,
                                      HashMap<String, HashMap<QuadTreeNode, NodeStat>> group2Stats) {
            if (node.isEnclosingNodeRange(range)) {
                HashMap<QuadTreeNode, NodeStat> node2Stat = materializedNodes.get(group);
                if (node2Stat != null && node2Stat.containsKey(node)) {
                    return node2Stat.get(node).getCardinality();
                }

                if (!ancestors.isEmpty()) {
                    int minSize = Integer.MAX_VALUE;
                    for (String ancestor : ancestors) {
                        HashMap<QuadTreeNode, NodeStat> ancestorNode2Stat = materializedNodes.get(ancestor);
                        if (ancestorNode2Stat != null && ancestorNode2Stat.containsKey(node)) {
                            NodeStat ancestorStat = ancestorNode2Stat.get(node);
                            if (ancestorStat.getCardinality() < minSize)
                                minSize = ancestorStat.getCardinality();
                        }
                    }

                    if (minSize != Integer.MAX_VALUE)
                        return minSize;
                }
            }

            if (isLeaf(node))
                return group2Stats.get(group).get(node).numObjects;

            double sum = 0.0;
            for (Node child : node.children) {
                if (child.isOverlappedNodeRange(range))
                    sum += getCostOfQuery(child, group, range, ancestors, materializedNodes, group2Stats);
            }

            return sum;
        }
    }

    private List<String> spaceDimension = Arrays.asList("city", "country");
    private List<String> timeDimension = Arrays.asList("hour", "day", "week", "month");
    private List<String> textDimension = Arrays.asList("topic");
    private List<Tuple2<String, String>> spaceDependencies;
    private List<Tuple2<String, String>> timeDependencies;
    private List<Tuple2<String, String>> textDependencies;
    private List<Tuple2<String, String>> groupDependencies;
    private HashMap<String, Integer> group2Level;
    private List<Tuple2<String, QuadTreeNode>> materializedPairsOfGroupAndNode = null;
    private HashMap<Integer, QuadTree> hour2QuadTree = new HashMap<>();
    private RawData rawData = new RawData();

    // For performance evaluation
    public static long timeReadCubes = 0L;
    public static long timeReadRawData = 0L;
    public static long numReadedObjectsInRawData = 0L;
    public static long numObjectsForQueries = 0L;

    public static boolean isAncestorOf(String ancestorGroup, String childGroup) {
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

    private static Optional<List<String>> getAncestorGroups(String child) {
        List<String> ancestors = Arrays.stream(Constants.GROUPS)
                .filter(ancestor -> isAncestorOf(ancestor, child))
                .collect(Collectors.toList());

        return ancestors.isEmpty() ? Optional.empty() : Optional.of(ancestors);
    }

    public QuadTreeCubes() {
        setSpaceDependencies();
        setTimeDependencies();
        setTextDependencies();
        setGroupDependenciesAndGroup2Level();
    }

    public QuadTreeCubes(HashMap<Integer, List<Tuple2<String, QuadTreeNode>>> hour2MaterializedNodes) {
        setSpaceDependencies();
        setTimeDependencies();
        setTextDependencies();
        setGroupDependenciesAndGroup2Level();
        for (Map.Entry<Integer, List<Tuple2<String, QuadTreeNode>>> entry :
                hour2MaterializedNodes.entrySet()) {
            int hour = entry.getKey();
            List<Tuple2<String, QuadTreeNode>> materializedNodes = entry.getValue();
            QuadTree quadTree = new QuadTree(materializedNodes);
            hour2QuadTree.put(hour, quadTree);
        }
    }

    public Map<String, Integer> processCountQuery(WarehouseQuery query) {
        if (query.getRange().isPresent()) {
            Tuple4<Double, Double, Double, Double> range =
                    query.getRange().get();
            Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
            Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());
            return getCountAggregationByRange(query.getGroupBy(), cornerFrom, cornerTo);
        } else {
            return getCountAggregation(query.getGroupBy());
        }
    }

    public Map<String, Map<String, Integer>> processWordCountQuery(WarehouseQuery query) {
        if (query.getRange().isPresent()) {
            Tuple4<Double, Double, Double, Double> range =
                    query.getRange().get();
            Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
            Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());
            return getWordCountAggregationByRange(query.getGroupBy(), cornerFrom, cornerTo);
        } else {
            return getWordCountAggregation(query.getGroupBy());
        }
    }

    public void addObject(SpatioTextualObject object) {
        rawData.addObject(object);
        int hour = Integer.valueOf(object.getHour());
        QuadTree quadTree = hour2QuadTree.get(hour);

        if (quadTree == null) {
            addCubesAtHour(String.valueOf(hour));
            quadTree = hour2QuadTree.get(hour);
        }
        quadTree.updateCube(object);

        List<String> hours = rawData.getHours();
        if (hours.size() > Constants.NUM_HOURS_DATA_KEPT) {
            int oldestHour = Integer.MAX_VALUE;
            for (int i = 0; i < hours.size(); ++i) {
                int currentHour = Integer.valueOf(hours.get(i));
                if (currentHour < oldestHour)
                    oldestHour = currentHour;
            }
            rawData.removeDataAtHour(String.valueOf(oldestHour));
            removeCubesAtHour(String.valueOf(oldestHour));
        }
    }

    public void addObjects(List<SpatioTextualObject> objects) {
        for (SpatioTextualObject object : objects)
            addObject(object);
    }

    private void addCubesAtHour(String hour) {
        QuadTree quadTree = new QuadTree(materializedPairsOfGroupAndNode);
        hour2QuadTree.put(Integer.valueOf(hour), quadTree);
    }

    private void removeCubesAtHour(String hour) {
        hour2QuadTree.remove(Integer.valueOf(hour));
    }

    public Map<String, Integer> getCountAggregation(String group) {
        Map<String, Integer> result = new HashMap<>();
        Optional<List<String>> ancestors = getAncestorGroups(group);

        for (Map.Entry<Integer, QuadTree> entry : hour2QuadTree.entrySet()) {
            int hour = entry.getKey();
            QuadTree quadTree = entry.getValue();

            boolean isComplete = quadTree.getCountAggregation(group, ancestors, result);
            if (!isComplete) {
                List<SpatioTextualObject> objects =
                        rawData.getObjectsByTime(String.valueOf(hour), String.valueOf(hour)).orElse(new ArrayList<>());
                for (SpatioTextualObject object : objects) {
                    String combinedDimValue = object.getGroupBy(group);
                    int count = result.getOrDefault(combinedDimValue, 0);
                    result.put(combinedDimValue, count + 1);
                }
            }
        }

        return result;
    }

    public Map<String, Integer> getCountAggregationByRange(String group, Tuple2<Double, Double> cornerFrom,
                                                           Tuple2<Double, Double> cornerTo) {
        Map<String, Integer> result = new HashMap<>();
        Optional<List<String>> ancestors = getAncestorGroups(group);

        for (Map.Entry<Integer, QuadTree> entry : hour2QuadTree.entrySet()) {
            int hour = entry.getKey();
            QuadTree quadTree = entry.getValue();

            Set<QuadTreeNode> checkedNodes = new HashSet<>();
            long t1 = System.currentTimeMillis();
            boolean isComplete = quadTree.getCountAggregationByRange(group, ancestors, result, checkedNodes, cornerFrom, cornerTo);
            long t2 = System.currentTimeMillis();
            timeReadCubes += t2 - t1;
            t1 = t2;
            if (!isComplete) {
                List<SpatioTextualObject> objects
                        = rawData.getObjectsByTimeRange(String.valueOf(hour), String.valueOf(hour), checkedNodes, cornerFrom, cornerTo).orElse(new ArrayList<>());
                for (SpatioTextualObject object : objects) {
                    String combinedDimValue = object.getGroupBy(group);
                    int count = result.getOrDefault(combinedDimValue, 0);
                    result.put(combinedDimValue, count + 1);
                }
                numReadedObjectsInRawData += objects.size();
            }
            t2 = System.currentTimeMillis();
            timeReadRawData += t2 - t1;
        }

        return result;
    }

    public Map<String, Map<String, Integer>> getWordCountAggregation(String group) {
        return null;
    }

    public Map<String, Map<String, Integer>> getWordCountAggregationByRange(String group,
                                                                            Tuple2<Double, Double> cornerFrom,
                                                                            Tuple2<Double, Double> cornerTo) {
        return null;
    }

    public void readMaterializedCubesFromFile(String fileName) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(fileName);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        materializedPairsOfGroupAndNode = (List<Tuple2<String, QuadTreeNode>>)in.readObject();
        in.close();
        fileIn.close();
    }

    public void materializeCubesAndWrite2File(RawData sampleRawData, List<WarehouseQuery> queries,
                                              Map<String, List<WarehouseQuery>> log, String fileName) throws IOException {
        List<Tuple2<String, QuadTreeNode>> materializedCubes = materializeCubes(sampleRawData, queries, log);

        List<Tuple2<String, QuadTreeNode>> copy = new ArrayList<>();
        for (Tuple2<String, QuadTreeNode> pair : materializedCubes)
            copy.add(new Tuple2<>(pair._1(), pair._2().getCopy()));

        FileOutputStream fileOut = new FileOutputStream(fileName);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(copy);
        out.close();
        fileOut.close();
        System.out.println("Materialized cubes are saved in " + fileName);
    }

    public List<Tuple2<String, QuadTreeNode>> materializeCubes(
            RawData sampleRawData, List<WarehouseQuery> queries, Map<String, List<WarehouseQuery>> log) {
        int hourWithMaxObjects = sampleRawData.hour2QuadTree.entrySet().stream()
                .max((left, right) -> Integer.compare(left.getValue().getNumObjects(), right.getValue().getNumObjects()))
                .map(e -> e.getKey())
                .get();
        System.out.println("Use the sample of objects at hour: " + hourWithMaxObjects);
        System.out.println("Number of objects: " + sampleRawData.getNumObjectsByHour(String.valueOf(hourWithMaxObjects)));

        HashMap<String, HashMap<QuadTreeNode, NodeStat>> group2Stats =
                sampleRawData.getGroup2NodeStatByHour(log, hourWithMaxObjects);
        RawData.QuadTree rawDataQuadTree = sampleRawData.getQuadTreeByHour(hourWithMaxObjects);

        return cubeSelectionAlg1(rawDataQuadTree, queries, group2Stats);
    }

    private List<Tuple2<String, QuadTreeNode>> cubeSelectionAlg1(RawData.QuadTree rawDataQuadTree,
                                                                 List<WarehouseQuery> queries,
                                                                 HashMap<String, HashMap<QuadTreeNode, NodeStat>> group2Stats) {
        HashMap<String, HashMap<QuadTreeNode, NodeStat>> copy = copyGroup2Stats(group2Stats);
        for (HashMap<QuadTreeNode, NodeStat> stats : copy.values()) {
            stats.entrySet().removeIf(e -> {
                List<Double> costs = getCosts(e.getValue());
                return costs.get(0) >= costs.get(1);
            });
        }
        removeNodesCanBeReduced(copy);

        long sumMemoryCost = 0L;
        List<Tuple3<String, QuadTreeNode, NodeStat>> array = new ArrayList<>();
        for (Map.Entry<String, HashMap<QuadTreeNode, NodeStat>> entry : copy.entrySet()) {
            for (Map.Entry<QuadTreeNode, NodeStat> nodeAndStat : entry.getValue().entrySet()) {
                array.add(new Tuple3<>(entry.getKey(), nodeAndStat.getKey(), nodeAndStat.getValue()));
                sumMemoryCost += nodeAndStat.getValue().getCardinality();
            }
        }

        if (sumMemoryCost <= Constants.MEM_CAPACITY) {
            List<Tuple2<String, QuadTreeNode>> selectedNodes = new ArrayList<>();
            for (Tuple3<String, QuadTreeNode, NodeStat> tuple : array)
                selectedNodes.add(new Tuple2<>(tuple._1(), tuple._2()));

            System.out.println("Number of selected nodes: " + selectedNodes.size());
            System.out.println("Memory cost: " + sumMemoryCost);

            return selectedNodes;
        }

        QuadTree quadTree = new QuadTree(rawDataQuadTree);
        HashMap<String, HashMap<QuadTreeNode, NodeStat>> materializedNodes = new HashMap<>();
        for (WarehouseQuery query : queries) {
            String group = query.getGroupBy();
            HashMap<QuadTreeNode, NodeStat> node2Stat = materializedNodes.get(group);
            if (node2Stat == null) {
                node2Stat = new HashMap<>();
                materializedNodes.put(group, node2Stat);
            }
        }

        sumMemoryCost = 0L;
        int numPhases = array.size();
        System.out.println("Num phases: " + numPhases);

        for (int i = 0; i < numPhases; ++i) {
            Tuple3<String, QuadTreeNode, NodeStat> optimalTuple = null;
            double optimalMetric = -Double.MAX_VALUE;
            long memoryCost = 0L;

            double queryCostBefore = quadTree.getCostsOfQueries(queries, materializedNodes, group2Stats);
            for (Tuple3<String, QuadTreeNode, NodeStat> tuple : array) {
                String group = tuple._1();
                QuadTreeNode node = tuple._2();
                NodeStat stat = tuple._3();

                HashMap<QuadTreeNode, NodeStat> node2Stat = materializedNodes.get(group);
                if (node2Stat.containsKey(node))
                    continue;

                node2Stat.put(node, stat);
                double queryCostAfter = quadTree.getCostsOfQueries(queries, materializedNodes, group2Stats);
                node2Stat.remove(node);

                double benefit = Constants.WEIGHT_QUERY * (queryCostBefore - queryCostAfter) -
                        Constants.WEIGHT_MAINTAIN * stat.numObjects;
                int size = stat.getCardinality();
                double metric = benefit / size;

                if (metric > 0 && metric > optimalMetric) {
                    optimalMetric = metric;
                    optimalTuple = tuple;
                    memoryCost = size;
                }
            }

            if (optimalTuple != null && sumMemoryCost + memoryCost <= Constants.MEM_CAPACITY) {
                HashMap<QuadTreeNode, NodeStat> node2Stat = materializedNodes.get(optimalTuple._1());
                node2Stat.put(optimalTuple._2(), optimalTuple._3());
                sumMemoryCost += memoryCost;
            }

            if (i % 10 == 0)
                System.out.printf("Finish %d phases\n", i + 1);
        }


        List<Tuple2<String, QuadTreeNode>> selectedNodes = new ArrayList<>();
        for (Map.Entry<String, HashMap<QuadTreeNode, NodeStat>> entry : materializedNodes.entrySet()) {
            String group = entry.getKey();
            for (QuadTreeNode quadTreeNode : entry.getValue().keySet())
                selectedNodes.add(new Tuple2<>(group, quadTreeNode));
        }
        System.out.println("Number of selected nodes: " + selectedNodes.size());
        System.out.println("Memory cost: " + sumMemoryCost);

        return selectedNodes;
    }

    private HashMap<String, HashMap<QuadTreeNode, NodeStat>> copyGroup2Stats(
            HashMap<String, HashMap<QuadTreeNode, NodeStat>> group2Stats) {
        HashMap<String, HashMap<QuadTreeNode, NodeStat>> copy = new HashMap<>();
        for (Map.Entry<String, HashMap<QuadTreeNode, NodeStat>> entry : group2Stats.entrySet()) {
            String group = entry.getKey();
            HashMap<QuadTreeNode, NodeStat> node2Stat = new HashMap<>();
            copy.put(group, node2Stat);

            for (Map.Entry<QuadTreeNode, NodeStat> sourceNode2Stat : entry.getValue().entrySet())
                node2Stat.put(sourceNode2Stat.getKey(), sourceNode2Stat.getValue());
        }

        return copy;
    }

    private void removeNodesCanBeReduced(HashMap<String, HashMap<QuadTreeNode, NodeStat>> group2Stats) {
        List<String> groups = new ArrayList<>(group2Stats.keySet());
        Comparator<String> comparator =
                Comparator.comparingInt(group2Level::get);
        Collections.sort(groups, comparator);

        for (String group : groups) {
            HashMap<QuadTreeNode, NodeStat> node2NodeStat = group2Stats.get(group);
            List<String> ancestors = group2Stats.keySet()
                    .stream()
                    .filter(ancestor -> isAncestorOf(ancestor, group))
                    .collect(Collectors.toList());
            node2NodeStat.entrySet().removeIf(entry -> {
                QuadTreeNode node = entry.getKey();
                NodeStat stat = entry.getValue();
                if (!ancestors.isEmpty()) {
                    NodeStat ancestorMinCardinality = ancestors.stream()
                            .filter(e -> group2Stats.containsKey(e) &&
                                    group2Stats.get(e).containsKey(node) &&
                                    group2Stats.get(e).get(node).numQueries != 0)
                            .map(e -> group2Stats.get(e).get(node))
                            .min(Comparator.comparingInt(NodeStat::getCardinality))
                            .orElse(null);

                    if (ancestorMinCardinality != null) {
                        List<Double> costsUsingAndNotUsingAncestor = getCosts(ancestorMinCardinality, stat);
                        if (costsUsingAndNotUsingAncestor.get(0) <= costsUsingAndNotUsingAncestor.get(1))
                            return true;
                    }
                }

                return false;
            });
        }
    }

    private List<Double> getCosts(NodeStat stat) {
        if (stat.numQueries == 0)
            return Arrays.asList(100000.0, 0.0);

        double weightMaintain = Constants.WEIGHT_MAINTAIN, weightQuery = Constants.WEIGHT_QUERY;

        double maintainCost = stat.numObjects;
        int cardinality = stat.distinctValues.size();
        double queryCostWithCube = cardinality * stat.numQueries;
        double costWithCube = maintainCost * weightMaintain + queryCostWithCube * weightQuery;

        double costWithoutCube = stat.numQueries * stat.numObjects;

        return Arrays.asList(costWithCube, costWithoutCube);
    }

    private List<Double> getCosts(NodeStat ancestor, NodeStat child) {
        double weightMaintain = Constants.WEIGHT_MAINTAIN, weightQuery = Constants.WEIGHT_QUERY;
        int numObjects = child.numObjects;

        double costUsingAncestor = ancestor.getCardinality() * child.numQueries;

        double maintainCost = numObjects;
        double queryCostUsingNewCube = child.getCardinality() * child.numQueries;
        double costUsingNewCube = maintainCost * weightMaintain + queryCostUsingNewCube * weightQuery;

        return Arrays.asList(costUsingAncestor, costUsingNewCube);
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

    private static void compareQueryResult(RawData rawData, QuadTreeCubes cubes, WarehouseQuery query) {
        Tuple4<Double, Double, Double, Double> queryRange = query.getRange().get();
        System.out.printf("Comparing the result for query: {%s, [(%f, %f), (%f, %f)]}\n",
                query.getGroupBy(), queryRange._1(), queryRange._2(), queryRange._3(), queryRange._4());

        System.out.println("Running the queries without using the cubes...");
        String group = query.getGroupBy();
        Tuple4<Double, Double, Double, Double> range = query.getRange().get();
        Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
        Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());

        List<SpatioTextualObject> os =
                rawData.getObjectsByRange(cornerFrom, cornerTo).orElse(new ArrayList<>());
        Map<String, Integer> aggregation = new HashMap<>();
        for (SpatioTextualObject o : os) {
            String key = o.getGroupBy(group);
            int count = aggregation.getOrDefault(key, 0) + 1;
            aggregation.put(key, count);
        }
        aggregation.forEach((k, v) -> System.out.printf("%s:%d\n", k, v));

        System.out.println("Running the queries using the cubes...");
        aggregation = cubes.processCountQuery(query);
        aggregation.forEach((k, v) -> System.out.printf("%s:%d\n", k, v));
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String dataPath = args[0];
        int numObjects = Integer.valueOf(args[1]);
        int numQueries = Integer.valueOf(args[2]);
        double rangeRatio = Double.valueOf(args[3]);
        String cubeFile = args[4];

//        String dataPath = "resources/sql_tweets.txt";
//        int numObjects = 100000;
//        int numQueries = 1000;
//        double rangeRatio = 0.1;
//        String cubeFile = "resources/cubes.txt";

        System.out.println("Configuration:");
        System.out.println("\tdataPath = " + dataPath);
        System.out.println("\tnumObjects = " + numObjects);
        System.out.println("\tnumQueries = " + numQueries);
        System.out.println("\trangeRatio = " + rangeRatio);
        System.out.println("\tcubeFile = " + cubeFile);

        long t1 = System.currentTimeMillis();
        System.out.println("Reading the raw data...");
        List<SpatioTextualObject> objects = UtilFunctions.createObjects(dataPath, numObjects);
        long t2 = System.currentTimeMillis();
        System.out.printf("Done! It takes %f seconds\n", (t2-t1)/1000.0);
        System.out.println("Number of objects: " + objects.size());

        List<WarehouseQuery> queries = UtilFunctions.createQueries(objects, numQueries,
                Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON, rangeRatio);
        Map<String, List<WarehouseQuery>> log = queries
                .stream()
                .filter(q -> q.getRange().isPresent())
                .collect(Collectors.groupingBy(q -> q.getGroupBy()));

        t1 = System.currentTimeMillis();
        System.out.println("Running the cube materialization algorithm...");
        RawData sampleRawData = new RawData();
        for (SpatioTextualObject object : objects) {
            object.setHour("20141111");
            sampleRawData.addObject(object);
        }
        QuadTreeCubes cubes = new QuadTreeCubes();
        cubes.materializeCubesAndWrite2File(sampleRawData, queries, log, cubeFile);
        cubes.readMaterializedCubesFromFile(cubeFile);
        t2 = System.currentTimeMillis();
        System.out.printf("Done! It takes %f seconds\n", (t2-t1)/1000.0);

        System.out.println("Running the queries without using the cubes...");
        t1 = System.currentTimeMillis();
        for (WarehouseQuery query : queries) {
            String group = query.getGroupBy();
            Tuple4<Double, Double, Double, Double> range = query.getRange().get();
            Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
            Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());

            List<SpatioTextualObject> os =
                    sampleRawData.getObjectsByRange(cornerFrom, cornerTo).orElse(new ArrayList<>());
            Map<String, Integer> aggregation = new HashMap<>();
            for (SpatioTextualObject o : os) {
                String key = o.getGroupBy(group);
                int count = aggregation.getOrDefault(key, 0) + 1;
                aggregation.put(key, count);
            }
            numObjectsForQueries += os.size();
        }
        t2 = System.currentTimeMillis();
        System.out.printf("Done! It takes %f seconds\n", (t2-t1)/1000.0);

        System.out.println("Running the queries using the cubes...");
        cubes.addObjects(objects);
        t1 = System.currentTimeMillis();
        for (WarehouseQuery query : queries)
            cubes.processCountQuery(query);
        t2 = System.currentTimeMillis();
        System.out.printf("Done! It takes %f seconds\n", (t2-t1)/1000.0);
        System.out.printf("Time used in reading cubes: %f seconds\n", timeReadCubes/1000.0);
        System.out.printf("Time used in reading raw data: %f seconds\n", timeReadRawData/1000.0);
        System.out.printf("Number of checked objects for the queries: %d\n", numObjectsForQueries);
        System.out.printf("Number of objects in reading raw data: %d\n", numReadedObjectsInRawData);
        System.out.printf("Percentage of objects in reading raw data: %f\n",
                (double)numReadedObjectsInRawData/numObjectsForQueries);
    }
}
