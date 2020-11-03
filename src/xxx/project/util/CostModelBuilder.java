package xxx.project.util;

import java.util.*;
import java.util.stream.Collectors;

public class CostModelBuilder {
    class MapBasedIndex {
        private Map<Integer, WarehouseQuery> queryMap = new HashMap<>();
        private Map<String, List<Integer>> topic2QueryIds = new HashMap<>();
        private Map<Integer, Map<String, Integer>> queryId2Result = new HashMap<>();
        private long sumTimeCheckObjects = 0L;
        private long sumTimeCheckQueries = 0L;

        public MapBasedIndex(List<WarehouseQuery> queries) {
            for (WarehouseQuery query : queries) {
                int id = query.getQueryId();
                queryMap.put(id, query);
                queryId2Result.put(id, new HashMap<>());
            }
            buildTopic2QueryIds();
        }

        private void buildTopic2QueryIds() {
            for (Map.Entry<Integer, WarehouseQuery> entry : queryMap.entrySet()) {
                int id = entry.getKey();
                WarehouseQuery query = entry.getValue();
                String topic = query.getTopic();

                List<Integer> queryIds = topic2QueryIds.get(topic);
                if (queryIds == null) {
                    queryIds = new ArrayList<>();
                    topic2QueryIds.put(topic, queryIds);
                }
                queryIds.add(id);
            }
        }

        public void processObject(SpatioTextualObject object) {
            long t1 = System.currentTimeMillis();
            String objectTopic = object.getTopic();
            long t2 = System.currentTimeMillis();
            sumTimeCheckObjects += t2 - t1;

            t1 = System.currentTimeMillis();
            List<Integer> queryIds = topic2QueryIds.get(objectTopic);
            updateQueryResults(queryIds, object);
            t2 = System.currentTimeMillis();
            sumTimeCheckQueries += t2 - t1;
        }

        private void updateQueryResults(List<Integer> queryIds, SpatioTextualObject object) {
            for (int queryId : queryIds) {
                Map<String, Integer> result = queryId2Result.get(queryId);
                String group = queryMap.get(queryId).getGroupBy();
                String objectGroup = object.getGroupBy(group);

                int val = result.getOrDefault(objectGroup, 0) + 1;
                result.put(objectGroup, val);
            }
        }

        public long getSumTimeCheckObjects() {
            return sumTimeCheckObjects;
        }

        public long getSumTimeCheckQueries() {
            return sumTimeCheckQueries;
        }
    }

    class InvertedIndex {
        private Map<Integer, WarehouseQuery> queryMap = new HashMap<>();
        private Map<String, List<Integer>> key2QueryIds = new HashMap<>();
        private Map<Integer, Map<String, Integer>> queryId2Result = new HashMap<>();
        private long sumTimeCheckObjects = 0L;
        private long sumTimeCheckQueries = 0L;

        public InvertedIndex(List<WarehouseQuery> queries) {
            for (WarehouseQuery query : queries) {
                int id = query.getQueryId();
                queryMap.put(id, query);
                queryId2Result.put(id, new HashMap<>());

                Set<String> keys = query.getKeywords().get();
                List<String> queryKeys = new ArrayList<>(keys);
                String indexedKey = queryKeys.get(0);

                List<Integer> queryIdList = key2QueryIds.get(indexedKey);
                if (queryIdList == null) {
                    queryIdList = new ArrayList<>();
                    key2QueryIds.put(indexedKey, queryIdList);
                }
                queryIdList.add(id);
            }
        }

        public void processObject(SpatioTextualObject object) {
            long t1 = System.currentTimeMillis();
            Set<String> terms = object.getTerms();
            long t2 = System.currentTimeMillis();
            sumTimeCheckObjects += t2 - t1;

            t1 = System.currentTimeMillis();
            for (String term : terms) {
                List<Integer> queryIds = key2QueryIds.get(term);
                if (queryIds == null)
                    continue;
                List<Integer> idsQueryNeedUpdate =
                        queryIds.stream().map(id -> queryMap.get(id)).filter(q -> {
                            Set<String> keys = q.getKeywords().get();
                            return terms.containsAll(keys);
                        }).map(q -> q.getQueryId()).collect(Collectors.toList());
                for (Integer id : idsQueryNeedUpdate) {
                    Map<String, Integer> resultMap = queryId2Result.get(id);
                    String group = queryMap.get(id).getGroupBy();
                    String objectGroup = object.getGroupBy(group);

                    int val = resultMap.getOrDefault(objectGroup, 0) + 1;
                    resultMap.put(objectGroup, val);
                }
            }
            t2 = System.currentTimeMillis();
            sumTimeCheckQueries += t2 - t1;
        }

        public long getSumTimeCheckObjects() {
            return sumTimeCheckObjects;
        }

        public long getSumTimeCheckQueries() {
            return sumTimeCheckQueries;
        }
    }

    class QuadTree {
        private Map<Integer, WarehouseQuery> queryMap = new HashMap<>();
        private Map<Integer, Map<String, Integer>> queryId2Result = new HashMap<>();
        private long sumTimeCheckObjects = 0L;
        private long sumTimeCheckQueries = 0L;

        class Node {
            public int nodeId;
            public double latFrom;
            public double lonFrom;
            public double latTo;
            public double lonTo;
            public Node[] children = {null, null, null, null};
            public List<SpatioTextualObject> objects = new ArrayList<>();
            public List<Integer> queryIds = new ArrayList<>();

            public Node(int nodeId, double latFrom, double lonFrom, double latTo, double lonTo) {
                this.nodeId = nodeId;
                this.latFrom = latFrom;
                this.lonFrom = lonFrom;
                this.latTo = latTo;
                this.lonTo = lonTo;
            }

            public void insertQuery(WarehouseQuery query) {
                int id = query.getQueryId();
                queryIds.add(id);
            }

            public void insertObject(SpatioTextualObject object) {
                objects.add(object);
            }

            public void processingObject(SpatioTextualObject object,
                                         Set<Integer> idsQuery2Update) {
                processingObjectHelper(root, object, idsQuery2Update);
            }

            private void processingObjectHelper(Node node,
                                                SpatioTextualObject object,
                                                Set<Integer> idsQuery2Update) {
                if (node == null || !node.isInside(object.getCoord()))
                    return;

                if (node.isLeaf())
                    idsQuery2Update.addAll(node.queryIds);

                for (Node child : node.children)
                    processingObjectHelper(child, object, idsQuery2Update);
            }

            public void setChildren(Node[] children) {
                this.children[0] = children[0];
                this.children[1] = children[1];
                this.children[2] = children[2];
                this.children[3] = children[3];
            }

            public boolean isLeaf() {
                return children[0] == null;
            }

            public void clearQueries() {
                queryIds.clear();
            }

            public void clearObjects() {
                objects.clear();
            }

            public boolean isInside(Tuple2<Double, Double> point) {
                return (point._1() >= latFrom && point._1() <= latTo &&
                        point._2() >= lonFrom && point._2() <= lonTo);
            }

            public boolean isOverlap(Tuple4<Double, Double, Double, Double> range) {
                Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
                Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());
                double smallLat = cornerFrom._1();
                double smallLon = cornerFrom._2();
                double largeLat = cornerTo._1();
                double largeLon = cornerTo._2();
                boolean nonOverlapped = largeLat <= latFrom || smallLat >= latTo ||
                        largeLon <= lonFrom || smallLon >= lonTo;

                return !nonOverlapped;
            }
        }

        private Node root = new Node(0, Constants.MIN_LAT, Constants.MIN_LON,
                Constants.MAX_LAT, Constants.MAX_LON);
        private final int NODE_CAPACITY = 100;

        public QuadTree(List<WarehouseQuery> queries) {
            for (WarehouseQuery query : queries) {
                int id = query.getQueryId();
                queryMap.put(id, query);
                queryId2Result.put(id, new HashMap<>());
            }
        }

        public void insertQuery(WarehouseQuery query) {
            insertQueryHelper(root, query);
        }

        public void processObject(SpatioTextualObject object) {
            long t1 = System.currentTimeMillis();
            Tuple2<Double, Double> point = object.getCoord();
            long t2 = System.currentTimeMillis();
            sumTimeCheckObjects += t2 - t1;

            t1 = System.currentTimeMillis();
            updateQueryResult(object);
            t2 = System.currentTimeMillis();
            sumTimeCheckQueries += t2 - t1;
        }

        public long getSumTimeCheckObjects() {
            return sumTimeCheckObjects;
        }

        public long getSumTimeCheckQueries() {
            return sumTimeCheckQueries;
        }

        private void updateQueryResult(SpatioTextualObject object) {
            Set<Integer> idsQuery2Update = new HashSet<>();
            root.processingObject(object, idsQuery2Update);
            for (int id : idsQuery2Update) {
                Map<String, Integer> result = queryId2Result.get(id);
                String group = queryMap.get(id).getGroupBy();
                String objectGroup = object.getGroupBy(group);

                int val = result.getOrDefault(objectGroup, 0) + 1;
                result.put(objectGroup, val);
            }
        }

        private void insertQueryHelper(Node node, WarehouseQuery query) {
            if (node == null)
                return;

            if (node.isOverlap(query.getRange().get())) {
                if (node.isLeaf()) {
                    node.insertQuery(query);
                    adjustNodes(node);
                } else {
                    for (Node child : node.children)
                        insertQueryHelper(child, query);
                }
            }
        }

        private void adjustNodes(Node node) {
            if (node != null && node.queryIds.size() >= NODE_CAPACITY) {
                buildChildren(node);
                for (int id : node.queryIds) {
                    WarehouseQuery query = queryMap.get(id);
                    List<Node> targets = new ArrayList<>();
                    findChildren(node, query, targets);
                    for (Node child : targets)
                        child.insertQuery(query);
                }

                node.clearQueries();
                for (Node child : node.children)
                    adjustNodes(child);
            }
        }

        private void findChildren(Node node, WarehouseQuery query, List<Node> targets) {
            Tuple4<Double, Double, Double, Double> range = query.getRange().get();
            for (Node child : node.children)
                if (child.isOverlap(range))
                    targets.add(child);
        }

        private Node findChild(Node node, SpatioTextualObject object) {
            Node[] children = node.children;
            int i;
            for (i = 0; i < children.length; ++i) {
                if (children[i].isInside(object.getCoord()))
                    break;
            }

            return children[i];
        }

        private void buildChildren(Node node) {
            int nodeId = node.nodeId;
            double latFrom = node.latFrom;
            double lonFrom = node.lonFrom;
            double latTo = node.latTo;
            double lonTo = node.lonTo;
            double centerLat = (latFrom + latTo) / 2.0;
            double centerLon = (lonFrom + lonTo) / 2.0;
            Node[] children = {null, null, null, null};
            children[0] = new Node(++nodeId, latFrom, lonFrom, centerLat, centerLon);
            children[1] = new Node(++nodeId, centerLat, lonFrom, latTo, centerLon);
            children[2] = new Node(++nodeId, latFrom, centerLon, centerLat, lonTo);
            children[3] = new Node(++nodeId, centerLat, centerLon, latTo, lonTo);
            node.setChildren(children);
        }
    }

    public void evaluateMapBasedIndex(List<SpatioTextualObject> objects, List<WarehouseQuery> queries) {
        MapBasedIndex index = new MapBasedIndex(queries);
        for (SpatioTextualObject object : objects)
            index.processObject(object);

        long t1 = index.getSumTimeCheckObjects();
        long t2 = index.getSumTimeCheckQueries();
        System.out.printf("Time used in checking objects: %f seconds\n", (t1 / 1000.0));
        System.out.printf("Time used in checking queries: %f seconds\n", (t2 / 1000.0));
    }

    public void evaluateInvertedIndex(List<SpatioTextualObject> objects, List<WarehouseQuery> queries) {
        InvertedIndex index = new InvertedIndex(queries);
        for (SpatioTextualObject object : objects)
            index.processObject(object);

        long t1 = index.getSumTimeCheckObjects();
        long t2 = index.getSumTimeCheckQueries();
        System.out.printf("Time used in checking objects: %f seconds\n", (t1 / 1000.0));
        System.out.printf("Time used in checking queries: %f seconds\n", (t2 / 1000.0));
    }

    public void evaluateQuadTreeIndex(List<SpatioTextualObject> objects, List<WarehouseQuery> queries) {
        QuadTree index = new QuadTree(queries);
        for (SpatioTextualObject object : objects)
            index.processObject(object);

        long t1 = index.getSumTimeCheckObjects();
        long t2 = index.getSumTimeCheckQueries();
        System.out.printf("Time used in checking objects: %f seconds\n", (t1 / 1000.0));
        System.out.printf("Time used in checking queries: %f seconds\n", (t2 / 1000.0));
    }

    public static void main(String[] args) throws Exception {
        String fileName = "resources/sample_tweets.txt";
        List<SpatioTextualObject> objects = UtilFunctions.createObjects(fileName, 1000000);
        List<WarehouseQuery> queries = UtilFunctions.createQueries(objects, 10000,
                Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON, 0.01);
        CostModelBuilder builder = new CostModelBuilder();
        builder.evaluateInvertedIndex(objects, queries);
        builder.evaluateMapBasedIndex(objects, queries);
        builder.evaluateQuadTreeIndex(objects, queries);
    }
}
