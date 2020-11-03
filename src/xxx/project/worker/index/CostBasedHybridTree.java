package xxx.project.worker.index;

import com.sun.org.apache.xpath.internal.operations.Mult;
import xxx.project.router.index.QuadTree;
import xxx.project.util.*;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class CostBasedHybridTree implements Serializable {
    class TopicIndex implements Serializable {
        public Map<String, QuadTreeIndex> topic2QuadTree = null;
        private QuadTreeIndex noTopicQuadTree = null;

        public TopicIndex() throws IOException {
            topic2QuadTree = new HashMap<>();
            noTopicQuadTree = new QuadTreeIndex();
        }

        public QuadTreeIndex getNextLevelIndex(String topic) {
            if (topic.length() > 0)
                return topic2QuadTree.get(topic);
            else
                return noTopicQuadTree;
        }

        public List<QuadTreeIndex> getNextLevelIndex(SpatioTextualObject object) {
            List<QuadTreeIndex> nextIndices = new ArrayList<>();
            nextIndices.add(topic2QuadTree.get(object.getTopic()));
            nextIndices.add(noTopicQuadTree);

            return nextIndices;
        }

        public List<QuadTreeIndex> getQuadTrees() {
            return new ArrayList<>(topic2QuadTree.values());
        }

        public void addQuadTreeIndex(String topic) throws IOException {
            topic2QuadTree.put(topic, new QuadTreeIndex());
        }
    }

    class QuadTreeIndex implements Serializable {
        class Node {
            public int nodeId;
            public double latFrom;
            public double lonFrom;
            public double latTo;
            public double lonTo;
            public Node[] children = {null, null, null, null};

            public Node(int nodeId, double latFrom, double lonFrom, double latTo, double lonTo) {
                this.nodeId = nodeId;
                this.latFrom = latFrom;
                this.lonFrom = lonFrom;
                this.latTo = latTo;
                this.lonTo = lonTo;
            }

            public boolean isLeaf() {
                return children[0] == null;
            }

            public void setChildren(Node[] children) {
                this.children[0] = children[0];
                this.children[1] = children[1];
                this.children[2] = children[2];
                this.children[3] = children[3];
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

        private Node root = null;
        private Map<Integer, InvertedIndex> leafId2NextLevelIndex = new HashMap<>();
        private InvertedIndex noRangeInvertedIndex = new InvertedIndex();

        public QuadTreeIndex() throws IOException {
            String fileName = "resources/init_quadtree_structure.txt";
            FileInputStream inputStream = new FileInputStream(fileName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            ArrayList<String> nodeInfoList = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                nodeInfoList.add(line);
            }

            inputStream.close();
            reader.close();

            initQuadTree(nodeInfoList);
        }

        public List<InvertedIndex> getNextLevelIndex(Tuple4<Double, Double, Double, Double> range) {
            List<Integer> leafIds = getLeafNodeIdsByRange(range);
            return leafIds.stream().map(id -> leafId2NextLevelIndex.get(id)).
                    collect(Collectors.toList());
        }

        public List<InvertedIndex> getNextLevelIndex(SpatioTextualObject object) {
            List<InvertedIndex> nextIndices = new ArrayList<>();
            int leafId = getLeafNodeIdByPoint(object.getCoord());
            InvertedIndex leafInvertedIndex = leafId2NextLevelIndex.get(leafId);
            nextIndices.add(leafInvertedIndex);
            nextIndices.add(noRangeInvertedIndex);

            return nextIndices;
        }

        public InvertedIndex getNoRangeNextLevelIndex() {
            return noRangeInvertedIndex;
        }

        public List<InvertedIndex> addInvertedIndex2LeafNodes() {
            List<Integer> leafIds = getLeafNodeIds();
            List<InvertedIndex> invertedIndices = new ArrayList<>();
            for (Integer id : leafIds) {
                InvertedIndex invertedIndex = new InvertedIndex();
                leafId2NextLevelIndex.put(id, invertedIndex);
                invertedIndices.add(invertedIndex);
            }

            return invertedIndices;
        }

        private void initQuadTree(ArrayList<String> nodeInfoList) {
            int[] lineNo = {0};
            root = initQuadTreeHelper(nodeInfoList, lineNo);
        }

        private Node initQuadTreeHelper(ArrayList<String> nodeInfoList, int[] lineNo) {
            if (lineNo[0] >= nodeInfoList.size())
                return null;

            String nodeInfo = nodeInfoList.get(lineNo[0]++);
            String[] attrs = nodeInfo.split(" ");
            int nodeId = Integer.valueOf(attrs[0]);
            double latFrom = Double.valueOf(attrs[1]);
            double lonFrom = Double.valueOf(attrs[2]);
            double latTo = Double.valueOf(attrs[3]);
            double lonTo = Double.valueOf(attrs[4]);
            boolean hasChildren = attrs[5].equals("yes");

            Node node = new Node(nodeId, latFrom, lonFrom, latTo, lonTo);
            if (hasChildren) {
                Node[] children = {null, null, null, null};
                children[0] = initQuadTreeHelper(nodeInfoList, lineNo);
                children[1] = initQuadTreeHelper(nodeInfoList, lineNo);
                children[2] = initQuadTreeHelper(nodeInfoList, lineNo);
                children[3] = initQuadTreeHelper(nodeInfoList, lineNo);
                node.setChildren(children);
            }

            return node;
        }

        public List<Integer> getLeafNodeIdsByRange(Tuple4<Double, Double, Double, Double> range) {
            List<Integer> leafIds = new ArrayList<>();
            getLeafNodeIdsByRangeHelper(root, leafIds, range);

            return leafIds;
        }

        private void getLeafNodeIdsByRangeHelper(Node node, List<Integer> leafIds,
                                                 Tuple4<Double, Double, Double, Double> range) {
            if (node == null || !node.isOverlap(range))
                return;

            if (node.isLeaf())
                leafIds.add(node.nodeId);

            for (Node child : node.children)
                getLeafNodeIdsByRangeHelper(child, leafIds, range);
        }

        public int getLeafNodeIdByPoint(Tuple2<Double, Double> point) {
            int[] leafId = {0};
            getLeafNodeIdByPointHelper(root, point, leafId);

            return leafId[0];
        }

        private void getLeafNodeIdByPointHelper(Node node, Tuple2<Double, Double> point, int[] leafId) {
            if (node == null || !node.isInside(point))
                return;

            if (node.isLeaf()) {
                leafId[0] = node.nodeId;
                return;
            }

            for (Node child : node.children)
                getLeafNodeIdByPointHelper(child, point, leafId);
        }

        public List<Integer> getLeafNodeIds() {
            List<Integer> leafIds = new ArrayList<>();
            getLeafNodeIdsHelper(root, leafIds);

            return leafIds;
        }

        private void getLeafNodeIdsHelper(Node node, List<Integer> leafIds) {
            if (node == null)
                return;

            if (node.isLeaf())
                leafIds.add(node.nodeId);

            for (Node child : node.children)
                getLeafNodeIdsHelper(child, leafIds);
        }

    }

    class InvertedIndex implements Serializable {
        public Map<String, List<Integer>> key2QueryIds = new HashMap<>();
        public List<Integer> noKeyQueryIds = new ArrayList<>();

        public Map<String, MultiTimeWindowIndex> key2MTIndices = new HashMap<>();
        public MultiTimeWindowIndex noKeyMTIndex = new MultiTimeWindowIndex();

        public void insertQuery(Map<Integer, WarehouseQuery> queryMap,
                                int queryId, Optional<HashSet<String>> keys) {
            WarehouseQuery query = queryMap.get(queryId);
            if (query.getIntervalLength() == 0)
                insertQuery(queryId, keys);
            else {
                if (!keys.isPresent()) {
                    noKeyMTIndex.insertQuery(query);
                    return;
                }

                List<String> keyList = new ArrayList<>(keys.get());
                String indexedKey = keyList.get(0);
                MultiTimeWindowIndex mtIndex = key2MTIndices.get(indexedKey);
                if (mtIndex == null) {
                    mtIndex = new MultiTimeWindowIndex();
                    key2MTIndices.put(indexedKey, mtIndex);
                }
                mtIndex.insertQuery(query);
            }
        }

        private void insertQuery(int queryId, Optional<HashSet<String>> keys) {
            if (!keys.isPresent()) {
                noKeyQueryIds.add(queryId);
                return;
            }

            List<String> keyList = new ArrayList<>(keys.get());
            String indexedKey = keyList.get(0);

            List<Integer> queryIds = key2QueryIds.get(indexedKey);
            if (queryIds == null) {
                queryIds = new ArrayList<>();
                key2QueryIds.put(indexedKey, queryIds);
            }
            queryIds.add(queryId);
        }

        public void deleteQueries(Set<Integer> id2Delete) {
            for (List<Integer> idList : key2QueryIds.values()) {
                idList.removeAll(id2Delete);
            }
            noKeyQueryIds.removeAll(id2Delete);
        }

        public List<Integer> getCandidateQueryIds(SpatioTextualObject object, Map<Integer, WarehouseQuery> queryMap) {
            List<Integer> candQueryIds = new ArrayList<>();
            Set<String> objTerms = object.getTerms();
            for (String term : objTerms) {
                candQueryIds.addAll(key2QueryIds.get(term));
            }
            candQueryIds.addAll(noKeyQueryIds);

            return candQueryIds.stream().
                    filter(id -> verifyObject(queryMap.get(id), objTerms)).collect(Collectors.toList());
        }

        private boolean verifyObject(WarehouseQuery query, Set<String> objectTerms) {
            Optional<HashSet<String>> queryKeys = query.getKeywords();
            if (!queryKeys.isPresent())
                return true;

            return objectTerms.containsAll(queryKeys.get());
        }
    }

    private Map<Integer, WarehouseQuery> queryMap = new HashMap<>();
    private Set<Integer> inactiveQueryIds = new HashSet<>();
    private Map<Integer, Map<String, AggrValues>> queryId2Result = new HashMap<>();
    private TopicIndex index = null;
    private List<InvertedIndex> baseInvertedIndices = null;

    private final int CAPACITY = 100000;
    private final int INACTIVE_LIMIT = 50000;

    public CostBasedHybridTree() throws IOException {
        index = new TopicIndex();
        for (String topic : Constants.TEXT_TOPICS)
            index.addQuadTreeIndex(topic);
        for (QuadTreeIndex quadTree : index.getQuadTrees()) {
            baseInvertedIndices.addAll(quadTree.addInvertedIndex2LeafNodes());
        }
    }

    public void insertQuery(WarehouseQuery query) {
        QuadTreeIndex quadTree = index.getNextLevelIndex(query.getGroupBy());
        Optional<Tuple4<Double, Double, Double, Double>> range = query.getRange();

        if (range.isPresent()) {
            List<InvertedIndex> invertedIndices = quadTree.getNextLevelIndex(range.get());
            for (InvertedIndex invertedIndex : invertedIndices)
                invertedIndex.insertQuery(queryMap, query.getQueryId(), query.getKeywords());
        } else {
            InvertedIndex noRangeInvertedIndex = quadTree.getNoRangeNextLevelIndex();
            noRangeInvertedIndex.insertQuery(queryMap, query.getQueryId(), query.getKeywords());
        }
    }

    public void deleteQuery(WarehouseQuery query) {
        int id = query.getQueryId();
        for(InvertedIndex baseInvertedIndex : baseInvertedIndices) {
            baseInvertedIndex.noKeyMTIndex.deleteQuery(query);
            for (MultiTimeWindowIndex mtIndex : baseInvertedIndex.key2MTIndices.values())
                mtIndex.deleteQuery(query);
        }

        if (!queryMap.containsKey(id))
            return;

        inactiveQueryIds.add(id);
        if (inactiveQueryIds.size() >= INACTIVE_LIMIT)
            deleteInactiveQueries();

        for (int queryId : inactiveQueryIds)
            queryMap.remove(queryId);
        inactiveQueryIds.clear();
    }

    private void deleteInactiveQueries() {
        for (InvertedIndex baseIndex : baseInvertedIndices)
            baseIndex.deleteQueries(inactiveQueryIds);
    }

    public void processObject(SpatioTextualObject object) {
        List<QuadTreeIndex> quadTrees = index.getNextLevelIndex(object);
        for (QuadTreeIndex quadTree : quadTrees) {
            List<InvertedIndex> invertedIndices = quadTree.getNextLevelIndex(object);
            for (InvertedIndex invertedIndex : invertedIndices) {
                List<Integer> candQueryIds =
                        invertedIndex.getCandidateQueryIds(object, queryMap).stream().
                                filter(id -> !inactiveQueryIds.contains(id)).collect(Collectors.toList());
                updateQueryResult(candQueryIds, object);

                invertedIndex.noKeyMTIndex.processObject(object);
                for (MultiTimeWindowIndex mtIndex : invertedIndex.key2MTIndices.values())
                    mtIndex.processObject(object);
            }
        }

        return;
    }

    private void updateQueryResult(List<Integer> queryIds, SpatioTextualObject object) {
        for (int id : queryIds) {
            WarehouseQuery query = queryMap.get(id);
            Map<String, AggrValues> result = queryId2Result.get(id);
            if (result == null) {
                result = new HashMap<>();
                queryId2Result.put(id, result);
            }

            String aggrFunc = query.getMeasure();
            String group = query.getGroupBy();
            String objGroup = object.getGroupBy(group);

            AggrValues values = result.get(objGroup);
            if (values == null) {
                values = new AggrValues(aggrFunc, query.getkOfTopK());
                result.put(objGroup, values);
            }
            values.update(object);
        }

        return;
    }

    public String getQueryResult(int queryId) {
        if (queryMap.containsKey(queryId)) {
            Map<String, AggrValues> result = queryId2Result.get(queryId);
            WarehouseQuery query = queryMap.get(queryId);
            StringBuilder resultText = new StringBuilder();
            for (Map.Entry<String, AggrValues> entry : result.entrySet()) {
                resultText.append(entry.getKey() + ": " + entry.getValue().getResult(query.getkOfTopK()) + "\n");
            }

            return resultText.toString();
        } else {
            String result = "";
            for (InvertedIndex baseInvertedIndex : baseInvertedIndices) {
                result = baseInvertedIndex.noKeyMTIndex.getQueryResult(queryId);
                if (result.length() > 0)
                    break;

                for (MultiTimeWindowIndex mtIndex : baseInvertedIndex.key2MTIndices.values()) {
                    result = mtIndex.getQueryResult(queryId);
                    if (result.length() > 0)
                        return result;
                }
            }

            return result;
        }
    }

    public boolean isEmpty() {
        return queryMap.isEmpty();
    }

    public boolean isFull() {
        return queryMap.size() >= CAPACITY;
    }
}
