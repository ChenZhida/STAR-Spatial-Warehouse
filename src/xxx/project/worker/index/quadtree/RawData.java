package xxx.project.worker.index.quadtree;

import xxx.project.util.*;
import xxx.project.worker.index.InvertedIndex;
import xxx.project.worker.index.RawDataInterface;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RawData implements RawDataInterface, Serializable {
    public static final long serialVersionUID = 104L;

    class RawDataQuadTreeNode extends QuadTreeNode {
        public int id;
        public RawDataQuadTreeNode[] children = new RawDataQuadTreeNode[4];

        public RawDataQuadTreeNode(int id, int level, double minLat, double minLon, double maxLat, double maxLon) {
            super(level, minLat, minLon, maxLat, maxLon);
            this.id = id;
        }
    }

    class QuadTree implements Serializable {
        public static final long serialVersionUID = 105L;

        public RawDataQuadTreeNode root;
        private HashMap<Integer, InvertedIndex> leafNode2Index = new HashMap<>();
        private HashMap<Integer, List<Tuple4<Double, Double, Double, Double>>> leafNode2Log
                = new HashMap<>();
        private int nodeSizeThreshold;

        private int numObjects = 0;
        private int idCounter = 0;

        public QuadTree(int nodeSizeThreshold) {
            root = new RawDataQuadTreeNode(getNewNodeId(), 0, Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON);
            this.nodeSizeThreshold = nodeSizeThreshold;
        }

        public Optional<List<SpatioTextualObject>> getObjects() {
            List<SpatioTextualObject> objects = new ArrayList<>();
            for (InvertedIndex index : leafNode2Index.values())
                objects.addAll(index.getObjects().orElse(new ArrayList<>()));

            return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
        }

        public List<SpatioTextualObject> getObjectsInLeafNode(RawDataQuadTreeNode node) {
            List<SpatioTextualObject> objects = new ArrayList<>();
            InvertedIndex index = leafNode2Index.get(node.id);
            objects.addAll(index.getObjects().orElse(new ArrayList<>()));

            return objects;
        }

        public List<SpatioTextualObject> getObjectsInLeafNode(int nodeId) {
            List<SpatioTextualObject> objects = new ArrayList<>();
            InvertedIndex index = leafNode2Index.get(nodeId);
            objects.addAll(index.getObjects().orElse(new ArrayList<>()));

            return objects;
        }

        public List<Integer> getLeafNodeIdsByRange(Tuple2<Double, Double> cornerFrom,
                                                  Tuple2<Double, Double> cornerTo) {
            List<Integer> leafNodeIds = new ArrayList<>();
            if (!root.isOverlappedNodeRange(cornerFrom, cornerTo))
                return leafNodeIds;

            getLeafNodeIdsByRangeHelper(root, leafNodeIds, cornerFrom, cornerTo);
            return leafNodeIds;
        }

        private void getLeafNodeIdsByRangeHelper(RawDataQuadTreeNode node,
                                                 List<Integer> leafNodeIds,
                                                 Tuple2<Double, Double> cornerFrom,
                                                 Tuple2<Double, Double> cornerTo) {
            if (isLeaf(node)) {
                leafNodeIds.add(node.id);
                return;
            }

            for (RawDataQuadTreeNode child : node.children)
                if (child.isOverlappedNodeRange(cornerFrom, cornerTo))
                    getLeafNodeIdsByRangeHelper(child, leafNodeIds, cornerFrom, cornerTo);
        }

        public Optional<List<SpatioTextualObject>> getObjectsByRange(Tuple2<Double, Double> cornerFrom,
                                                                     Tuple2<Double, Double> cornerTo) {
            if (!root.isOverlappedNodeRange(cornerFrom, cornerTo))
                return Optional.empty();

            List<SpatioTextualObject> objects = new ArrayList<>();
            getObjectsByRange(root, cornerFrom, cornerTo, objects);

            return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
        }

        public Optional<List<SpatioTextualObject>> getObjectsByRange(Tuple2<Double, Double> cornerFrom,
                                                                     Tuple2<Double, Double> cornerTo,
                                                                     Set<Integer> unCachedLeafIds) {
            if (!root.isOverlappedNodeRange(cornerFrom, cornerTo))
                return Optional.empty();

            List<SpatioTextualObject> objects = new ArrayList<>();
            getObjectsByRangeHelper(root, cornerFrom, cornerTo, objects, unCachedLeafIds);

            return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
        }

        private void getObjectsByRangeHelper(RawDataQuadTreeNode node, Tuple2<Double, Double> cornerFrom,
                                             Tuple2<Double, Double> cornerTo,
                                             List<SpatioTextualObject> objects,
                                             Set<Integer> unCachedLeafIds) {
            if (isLeaf(node)) {
                if (unCachedLeafIds.contains(node.id)) {
                    InvertedIndex index = leafNode2Index.get(node.id);
                    if (index != null) {
                        List<SpatioTextualObject> os = index.getObjects().orElse(new ArrayList<>());
                        for (SpatioTextualObject object : os)
                            if (isInsideRange(object, cornerFrom, cornerTo))
                                objects.add(object);
                    }
                    List<Tuple4<Double, Double, Double, Double>> log =
                            leafNode2Log.get(node.id);
                    if (log == null) {
                        log = new ArrayList<>();
                        leafNode2Log.put(node.id, log);
                    }
                    log.add(new Tuple4<>(cornerFrom._1(), cornerFrom._2(),
                            cornerTo._1(), cornerTo._2()));
                }

                return;
            }

            for (RawDataQuadTreeNode child : node.children)
                if (child.isOverlappedNodeRange(cornerFrom, cornerTo))
                    getObjectsByRangeHelper(child, cornerFrom, cornerTo, objects, unCachedLeafIds);
        }

        private void getObjectsByRange(RawDataQuadTreeNode rawDataQuadTreeNode, Tuple2<Double, Double> cornerFrom,
                                       Tuple2<Double, Double> cornerTo, List<SpatioTextualObject> objects) {
            if (isLeaf(rawDataQuadTreeNode)) {
                InvertedIndex index = leafNode2Index.get(rawDataQuadTreeNode.id);
                if (index != null) {
                    List<SpatioTextualObject> os = index.getObjects().orElse(new ArrayList<>());
                    for (SpatioTextualObject object : os)
                        if (isInsideRange(object, cornerFrom, cornerTo))
                            objects.add(object);
                }
                List<Tuple4<Double, Double, Double, Double>> log =
                        leafNode2Log.get(rawDataQuadTreeNode.id);
                if (log == null) {
                    log = new ArrayList<>();
                    leafNode2Log.put(rawDataQuadTreeNode.id, log);
                }
                log.add(new Tuple4<>(cornerFrom._1(), cornerFrom._2(),
                        cornerTo._1(), cornerTo._2()));

                return;
            }

            for (RawDataQuadTreeNode child : rawDataQuadTreeNode.children)
                if (child.isOverlappedNodeRange(cornerFrom, cornerTo))
                    getObjectsByRange(child, cornerFrom, cornerTo, objects);
        }

        public Optional<List<SpatioTextualObject>> getObjectsByRange(Set<QuadTreeNode> checkedNodes,
                                                                     Tuple2<Double, Double> cornerFrom,
                                                                     Tuple2<Double, Double> cornerTo) {
            if (!root.isOverlappedNodeRange(cornerFrom, cornerTo))
                return Optional.empty();

            List<SpatioTextualObject> objects = new ArrayList<>();
            getObjectsByRange(root, checkedNodes, cornerFrom, cornerTo, objects);

            return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
        }

        public int getNumObjectsInLeafNode(int nodeId) {
            InvertedIndex index = leafNode2Index.get(nodeId);
            return index.getNumObjects();
        }

        public int getNumQueriesOverlapLeafNode(int nodeId) {
            List<Tuple4<Double, Double, Double, Double>> log = leafNode2Log.get(nodeId);
            return log.size();
        }

        public List<Tuple4<Double, Double, Double, Double>> getQueriesOverlapLeafNode(int nodeId) {
            return leafNode2Log.get(nodeId);
        }

        public List<RawDataQuadTreeNode> getLeafNodes() {
            List<RawDataQuadTreeNode> leafNodes = new ArrayList<>();
            getLeafNodesHelper(root, leafNodes);

            return leafNodes;
        }

        private void getLeafNodesHelper(RawDataQuadTreeNode node, List<RawDataQuadTreeNode> leafNodes) {
            if (isLeaf(node)) {
                leafNodes.add(node);
                return;
            }

            for (RawDataQuadTreeNode child : node.children)
                getLeafNodesHelper(child, leafNodes);
        }

        private void getObjectsByRange(RawDataQuadTreeNode rawDataQuadTreeNode, Set<QuadTreeNode> checkedNodes, Tuple2<Double, Double> cornerFrom,
                                       Tuple2<Double, Double> cornerTo, List<SpatioTextualObject> objects) {
            if (checkedNodes.contains(rawDataQuadTreeNode))
                return;

            if (isLeaf(rawDataQuadTreeNode)) {
                InvertedIndex index = leafNode2Index.get(rawDataQuadTreeNode.id);
                if (index != null) {
                    List<SpatioTextualObject> os = index.getObjects().orElse(new ArrayList<>());
                    for (SpatioTextualObject object : os)
                        if (isInsideRange(object, cornerFrom, cornerTo))
                            objects.add(object);
                }

                List<Tuple4<Double, Double, Double, Double>> log =
                        leafNode2Log.get(rawDataQuadTreeNode.id);
                if (log == null) {
                    log = new ArrayList<>();
                    leafNode2Log.put(rawDataQuadTreeNode.id, log);
                }
                log.add(new Tuple4<>(cornerFrom._1(), cornerFrom._2(),
                        cornerTo._1(), cornerTo._2()));

                return;
            }

            for (RawDataQuadTreeNode child : rawDataQuadTreeNode.children)
                if (child.isOverlappedNodeRange(cornerFrom, cornerTo))
                    getObjectsByRange(child, checkedNodes, cornerFrom, cornerTo, objects);
        }

        public Optional<List<SpatioTextualObject>> getObjectsByTerms(Set<String> terms) {
            List<SpatioTextualObject> objects = new ArrayList<>();
            for (InvertedIndex index : leafNode2Index.values())
                objects.addAll(index.getObjectsByTerms(terms).orElse(new ArrayList<>()));

            return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
        }

        public Optional<List<SpatioTextualObject>> getObjectsByRangeTerms(Tuple2<Double, Double> cornerFrom,
                                                                          Tuple2<Double, Double> cornerTo,
                                                                          Set<String> terms) {
            if (!root.isOverlappedNodeRange(cornerFrom, cornerTo))
                return Optional.empty();

            List<SpatioTextualObject> objects = new ArrayList<>();
            getObjectsByRangeTerms(root, cornerFrom, cornerTo, terms, objects);

            return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
        }

        private void getObjectsByRangeTerms(RawDataQuadTreeNode rawDataQuadTreeNode,
                                            Tuple2<Double, Double> cornerFrom,
                                            Tuple2<Double, Double> cornerTo,
                                            Set<String> terms, List<SpatioTextualObject> objects) {
            if (isLeaf(rawDataQuadTreeNode)) {
                InvertedIndex index = leafNode2Index.get(rawDataQuadTreeNode.id);
                if (index != null) {
                    List<SpatioTextualObject> os = index.getObjectsByTerms(terms).orElse(new ArrayList<>());
                    for (SpatioTextualObject object : os)
                        if (isInsideRange(object, cornerFrom, cornerTo))
                            objects.add(object);
                }

                List<Tuple4<Double, Double, Double, Double>> log =
                        leafNode2Log.get(rawDataQuadTreeNode.id);
                if (log == null) {
                    log = new ArrayList<>();
                    leafNode2Log.put(rawDataQuadTreeNode.id, log);
                }
                log.add(new Tuple4<>(cornerFrom._1(), cornerFrom._2(),
                        cornerTo._1(), cornerTo._2()));

                return;
            }

            for (RawDataQuadTreeNode child : rawDataQuadTreeNode.children)
                if (child.isOverlappedNodeRange(cornerFrom, cornerTo))
                    getObjectsByRangeTerms(child, cornerFrom, cornerTo, terms, objects);
        }

        public int addObject(SpatioTextualObject object) {
            if (root.isInsideNodeRange(object.getCoord()))
                return addObject(root, 0, object);

            return -1;
        }

        private int addObject(RawDataQuadTreeNode rawDataQuadTreeNode, int curLevel, SpatioTextualObject object) {
            if (isLeaf(rawDataQuadTreeNode)) {
                InvertedIndex index = leafNode2Index.get(rawDataQuadTreeNode.id);
                if (index == null) {
                    index = new InvertedIndex();
                    leafNode2Index.put(rawDataQuadTreeNode.id, index);
                }

                index.addObject(object);
                if (index.getNumObjects() >= nodeSizeThreshold)
                    distributeObjects2Children(rawDataQuadTreeNode, curLevel);

                ++numObjects;
                return rawDataQuadTreeNode.id;
            }

            for (RawDataQuadTreeNode child : rawDataQuadTreeNode.children)
                if (child.isInsideNodeRange(object.getCoord())) {
                    return addObject(child, curLevel + 1, object);
                }

            return -1;
        }

        private boolean isInsideRange(SpatioTextualObject object, Tuple2<Double, Double> cornerFrom,
                                      Tuple2<Double, Double> cornerTo) {
            Tuple2<Double, Double> coord = object.getCoord();
            double lat = coord._1();
            double lon = coord._2();

            return cornerFrom._1() <= lat && cornerTo._1() > lat &&
                    cornerFrom._2() <= lon && cornerTo._2() > lon;
        }

        private boolean isLeaf(RawDataQuadTreeNode rawDataQuadTreeNode) {
            return rawDataQuadTreeNode.children[0] == null;
        }

        private int getNewNodeId() {
            return idCounter++;
        }

        private void distributeObjects2Children(RawDataQuadTreeNode rawDataQuadTreeNode, int curLevel) {
            if (curLevel >= Constants.QUAD_TREE_MAX_LEVEL)
                return;

            double middleLat = (rawDataQuadTreeNode.minLat + rawDataQuadTreeNode.maxLat) / 2.0;
            double middleLon = (rawDataQuadTreeNode.minLon + rawDataQuadTreeNode.maxLon) / 2.0;

            rawDataQuadTreeNode.children[0] = new RawDataQuadTreeNode(getNewNodeId(), curLevel + 1, rawDataQuadTreeNode.minLat, rawDataQuadTreeNode.minLon, middleLat, middleLon);
            rawDataQuadTreeNode.children[1] = new RawDataQuadTreeNode(getNewNodeId(), curLevel + 1, rawDataQuadTreeNode.minLat, middleLon, middleLat, rawDataQuadTreeNode.maxLon);
            rawDataQuadTreeNode.children[2] = new RawDataQuadTreeNode(getNewNodeId(), curLevel + 1, middleLat, rawDataQuadTreeNode.minLon, rawDataQuadTreeNode.maxLat, middleLon);
            rawDataQuadTreeNode.children[3] = new RawDataQuadTreeNode(getNewNodeId(), curLevel + 1, middleLat, middleLon, rawDataQuadTreeNode.maxLat, rawDataQuadTreeNode.maxLon);

            InvertedIndex parentIndex = leafNode2Index.get(rawDataQuadTreeNode.id);
            List<SpatioTextualObject> objects = parentIndex.getObjects().get();

            for (SpatioTextualObject object : objects) {
                for (RawDataQuadTreeNode child : rawDataQuadTreeNode.children) {
                    if (child.isInsideNodeRange(object.getCoord())) {
                        InvertedIndex index = leafNode2Index.get(child.id);
                        if (index == null) {
                            index = new InvertedIndex();
                            leafNode2Index.put(child.id, index);
                        }
                        index.addObject(object);
                        break;
                    }
                }
            }
            leafNode2Index.remove(rawDataQuadTreeNode.id);

            for (RawDataQuadTreeNode child : rawDataQuadTreeNode.children) {
                InvertedIndex childIndex = leafNode2Index.get(child.id);
                if (childIndex != null && childIndex.getNumObjects() >= nodeSizeThreshold)
                    distributeObjects2Children(child, curLevel + 1);
            }
        }

        public void collectDrawInfo(List<Tuple4<Double, Double, Double, Double>> rects,
                                    List<Tuple2<Double, Double>> points) {
            collectDrawInfo(root, rects, points);
        }

        private void collectDrawInfo(RawDataQuadTreeNode rawDataQuadTreeNode, List<Tuple4<Double, Double, Double, Double>> rects,
                                     List<Tuple2<Double, Double>> points) {
            if (isLeaf(rawDataQuadTreeNode)) {
                rects.add(new Tuple4<>(rawDataQuadTreeNode.minLat, rawDataQuadTreeNode.minLon, rawDataQuadTreeNode.maxLat, rawDataQuadTreeNode.maxLon));
                InvertedIndex index = leafNode2Index.get(rawDataQuadTreeNode.id);
                if (index != null) {
                    for (SpatioTextualObject object : index.getObjects().get())
                        points.add(object.getCoord());
                }

                return;
            }

            for (RawDataQuadTreeNode child : rawDataQuadTreeNode.children)
                collectDrawInfo(child, rects, points);
        }

        public int getNumObjects() {
            return numObjects;
        }

        public void printInfo() {
            List<String> info = new ArrayList<>();
            int numLeafNodes = leafNode2Index.size();
            collectInfo(root, info);

            System.out.println("Number of leaf nodes: " + numLeafNodes);
            System.out.println("Information of leaf nodes:");
            info.forEach(System.out::println);
        }

        private void collectInfo(RawDataQuadTreeNode rawDataQuadTreeNode, List<String> info) {
            if (isLeaf(rawDataQuadTreeNode)) {
                String nodeInfo = rawDataQuadTreeNode.level + " " + rawDataQuadTreeNode.minLat + " " + rawDataQuadTreeNode.minLon + " " + rawDataQuadTreeNode.maxLat + " " + rawDataQuadTreeNode.maxLon;
                InvertedIndex index = leafNode2Index.get(rawDataQuadTreeNode.id);
                if (index != null)
                    nodeInfo += " " + index.getNumObjects();

                info.add(nodeInfo);
                return;
            }

            for (RawDataQuadTreeNode child : rawDataQuadTreeNode.children)
                collectInfo(child, info);
        }

        public HashMap<String, HashMap<QuadTreeNode, NodeStat>> getGroup2NodeStat(
                Map<String, List<WarehouseQuery>> log) {
            HashMap<String, HashMap<QuadTreeNode, NodeStat>> group2NodeStat = new HashMap<>();
            for (Map.Entry<String, List<WarehouseQuery>> entry : log.entrySet()) {
                String group = entry.getKey();
                HashMap<QuadTreeNode, NodeStat> nodeStats = new HashMap<>();
                List<WarehouseQuery> queries = entry.getValue();

                addNumObjects2NodeStat(root, nodeStats);
                addNumQueries2NodeStat(root, queries, nodeStats);
                addDistinctValues2NodeStat(group, nodeStats);
                group2NodeStat.put(group, nodeStats);
            }

            return group2NodeStat;
        }

        private int addNumObjects2NodeStat(RawDataQuadTreeNode rawDataQuadTreeNode, HashMap<QuadTreeNode, NodeStat> nodeStats) {
            NodeStat stat = nodeStats.get(rawDataQuadTreeNode);
            if (stat == null) {
                stat = new NodeStat();
                nodeStats.put(rawDataQuadTreeNode, stat);
            }

            if (isLeaf(rawDataQuadTreeNode)) {
                InvertedIndex index = leafNode2Index.get(rawDataQuadTreeNode.id);
                stat.numObjects = index != null ? index.getNumObjects() : 0;
                return stat.numObjects;
            }

            for (RawDataQuadTreeNode child : rawDataQuadTreeNode.children)
                stat.numObjects += addNumObjects2NodeStat(child, nodeStats);

            return stat.numObjects;
        }

        private void addNumQueries2NodeStat(RawDataQuadTreeNode rawDataQuadTreeNode, List<WarehouseQuery> queries,
                                            HashMap<QuadTreeNode, NodeStat> nodeStats) {
            NodeStat stat = nodeStats.get(rawDataQuadTreeNode);
            if (stat == null) {
                stat = new NodeStat();
                nodeStats.put(rawDataQuadTreeNode, stat);
            }

            for (WarehouseQuery query : queries)
                if (rawDataQuadTreeNode.isEnclosingNodeRange(query.getRange().get()))
                    ++stat.numQueries;

            if (isLeaf(rawDataQuadTreeNode))
                return;

            for (RawDataQuadTreeNode child : rawDataQuadTreeNode.children)
                addNumQueries2NodeStat(child, queries, nodeStats);
        }

        private void addDistinctValues2NodeStat(String group, HashMap<QuadTreeNode, NodeStat> nodeStats) {
            for (Map.Entry<QuadTreeNode, NodeStat> entry : nodeStats.entrySet()) {
                QuadTreeNode node = entry.getKey();
                NodeStat stat = entry.getValue();
                if (stat.numQueries == 0)
                    continue;

                Tuple2<Double, Double> cornerFrom = new Tuple2<>(node.minLat, node.minLon);
                Tuple2<Double, Double> cornerTo = new Tuple2<>(node.maxLat, node.maxLon);
                List<SpatioTextualObject> objects =
                        getObjectsByRange(cornerFrom, cornerTo).orElse(new ArrayList<>());

                for (SpatioTextualObject object : objects)
                    stat.distinctValues.add(object.getGroupBy(group));
            }
        }
    }

    public final int NODE_SIZE_THRESHOLD = Constants.QUAD_TREE_NODE_SIZE;
    public HashMap<Integer, QuadTree> hour2QuadTree = new HashMap<>();

    public void addObjects(List<SpatioTextualObject> objects) {
        for (SpatioTextualObject object : objects)
            addObject(object);
    }

    public void addObject(SpatioTextualObject object) {
        String hour = object.getHour();
        int key = Integer.valueOf(hour);
        QuadTree quadTree = hour2QuadTree.get(key);

        if (quadTree == null) {
            quadTree = new QuadTree(NODE_SIZE_THRESHOLD);
            hour2QuadTree.put(key, quadTree);
        }

        quadTree.addObject(object);
    }

    public void addObject(SpatioTextualObject object, int[] nodeId) {
        String hour = object.getHour();
        int key = Integer.valueOf(hour);
        QuadTree quadTree = hour2QuadTree.get(key);

        if (quadTree == null) {
            quadTree = new QuadTree(NODE_SIZE_THRESHOLD);
            hour2QuadTree.put(key, quadTree);
        }

        nodeId[0] = quadTree.addObject(object);
    }

    public Optional<List<SpatioTextualObject>> getObjects() {
        List<SpatioTextualObject> objects = new ArrayList<>();
        for (QuadTree quadTree : hour2QuadTree.values())
            objects.addAll(quadTree.getObjects().orElse(new ArrayList<>()));

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public List<SpatioTextualObject> getObjectsInLeafNode(int nodeId) {
        int newestHour = -1;
        for (int hour : hour2QuadTree.keySet())
            newestHour = Math.max(newestHour, hour);
        QuadTree newestQuadTree = hour2QuadTree.get(newestHour);

        return newestQuadTree.getObjectsInLeafNode(nodeId);
    }

    public int getNumObjectsInLeafNode(int nodeId) {
        int newestHour = -1;
        for (int hour : hour2QuadTree.keySet())
            newestHour = Math.max(newestHour, hour);
        QuadTree newestQuadTree = hour2QuadTree.get(newestHour);

        return newestQuadTree.getNumObjectsInLeafNode(nodeId);
    }

    public int getNumQueriesOverlapLeafNode(int nodeId) {
        int newestHour = -1;
        for (int hour : hour2QuadTree.keySet())
            newestHour = Math.max(newestHour, hour);
        QuadTree newestQuadTree = hour2QuadTree.get(newestHour);

        return newestQuadTree.getNumQueriesOverlapLeafNode(nodeId);
    }

    public List<Tuple4<Double, Double, Double, Double>> getQueriesOverlapLeafNode(int nodeId) {
        int newestHour = -1;
        for (int hour : hour2QuadTree.keySet())
            newestHour = Math.max(newestHour, hour);
        QuadTree newestQuadTree = hour2QuadTree.get(newestHour);

        return newestQuadTree.getQueriesOverlapLeafNode(nodeId);
    }

    public List<RawDataQuadTreeNode> getLeafNodes() {
        List<RawDataQuadTreeNode> leafNodes = new ArrayList<>();
        int newestHour = -1;

        for (int hour : hour2QuadTree.keySet())
            newestHour = Math.max(newestHour, hour);

        QuadTree newestQuadTree = hour2QuadTree.get(newestHour);
        return newestQuadTree.getLeafNodes();
    }

    public List<Integer> getLeafNodeIdsByRange(Tuple2<Double, Double> cornerFrom,
                                    Tuple2<Double, Double> cornerTo) {
        int newestHour = -1;
        for (int hour : hour2QuadTree.keySet())
            if (hour > newestHour)
                newestHour = hour;

        QuadTree newestQuadTree = hour2QuadTree.get(newestHour);
        return newestQuadTree.getLeafNodeIdsByRange(cornerFrom, cornerTo);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByRange(Tuple2<Double, Double> cornerFrom,
                                                                 Tuple2<Double, Double> cornerTo) {
        List<SpatioTextualObject> objects = new ArrayList<>();
        for (QuadTree quadTree : hour2QuadTree.values())
            objects.addAll(quadTree.getObjectsByRange(cornerFrom, cornerTo).orElse(new ArrayList<>()));

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByRange(Tuple2<Double, Double> cornerFrom,
                                                                 Tuple2<Double, Double> cornerTo,
                                                                 Set<Integer> unCachedLeafIds) {
        int newestHour = -1;
        for (int hour : hour2QuadTree.keySet())
            if (hour > newestHour)
                newestHour = hour;

        QuadTree quadTree = hour2QuadTree.get(newestHour);
        return quadTree.getObjectsByRange(cornerFrom, cornerTo, unCachedLeafIds);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByRange(Tuple2<Double, Double> cornerFrom,
                                                                 Tuple2<Double, Double> cornerTo,
                                                                 ObjectCacheManager objectCacheManager) {
        int newestHour = -1;
        for (int hour : hour2QuadTree.keySet())
            if (hour > newestHour)
                newestHour = hour;

        QuadTree quadTree = hour2QuadTree.get(newestHour);

        List<SpatioTextualObject> resultObjects = new ArrayList<>();
        List<Integer> leafNodeIds = quadTree.getLeafNodeIdsByRange(cornerFrom, cornerTo);
        Set<Integer> unCachedLeafIds = new HashSet<>();
        for (Integer leafId : leafNodeIds) {
            List<SpatioTextualObject> cachedObjects = objectCacheManager.
                    getObjectsUsingCache(this, leafId, cornerFrom, cornerTo);

            if (!cachedObjects.isEmpty())
                resultObjects.addAll(cachedObjects);
            else
                unCachedLeafIds.add(leafId);
        }

        if (!unCachedLeafIds.isEmpty()) {
            List<SpatioTextualObject> unCachedObjects = quadTree.getObjectsByRange(cornerFrom,
                    cornerTo, unCachedLeafIds).orElse(new ArrayList<>());
            resultObjects.addAll(unCachedObjects);
        }

        return Optional.of(resultObjects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByRange(Set<QuadTreeNode> checkedNodes,
                                                                 Tuple2<Double, Double> cornerFrom,
                                                                 Tuple2<Double, Double> cornerTo) {
        List<SpatioTextualObject> objects = new ArrayList<>();
        for (QuadTree quadTree : hour2QuadTree.values())
            objects.addAll(quadTree.getObjectsByRange(checkedNodes, cornerFrom, cornerTo).orElse(new ArrayList<>()));

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByTimeRange(String startTime, String endTime,
                                                                     Set<QuadTreeNode> checkedNodes,
                                                                     Tuple2<Double, Double> cornerFrom,
                                                                     Tuple2<Double, Double> cornerTo) {
        int start = Integer.valueOf(startTime);
        int end = Integer.valueOf(endTime);
        List<SpatioTextualObject> objects = new ArrayList<>();

        IntStream.rangeClosed(start, end).forEach(t -> {
            QuadTree quadTree = hour2QuadTree.get(t);
            if (quadTree != null) {
                Optional<List<SpatioTextualObject>> objsRange =
                        quadTree.getObjectsByRange(checkedNodes, cornerFrom, cornerTo);
                if (objsRange.isPresent())
                    objects.addAll(objsRange.get());
            }
        });

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByTerms(Set<String> terms) {
        List<SpatioTextualObject> objects = new ArrayList<>();
        for (QuadTree quadTree : hour2QuadTree.values())
            objects.addAll(quadTree.getObjectsByTerms(terms).orElse(new ArrayList<>()));

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByRangeTerms(Tuple2<Double, Double> cornerFrom,
                                                                      Tuple2<Double, Double> cornerTo,
                                                                      Set<String> terms) {
        List<SpatioTextualObject> objects = new ArrayList<>();
        for (QuadTree quadTree : hour2QuadTree.values())
            objects.addAll(quadTree.getObjectsByRangeTerms(cornerFrom, cornerTo, terms).orElse(new ArrayList<>()));

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByTime(String startTime, String endTime) {
        int start = Integer.valueOf(startTime);
        int end = Integer.valueOf(endTime);
        List<SpatioTextualObject> objects = new ArrayList<>();

        IntStream.rangeClosed(start, end).forEach(t -> {
            QuadTree quadTree = hour2QuadTree.get(t);
            if (quadTree != null) {
                Optional<List<SpatioTextualObject>> objsQuadTree =
                        quadTree.getObjects();
                if (objsQuadTree.isPresent())
                    objects.addAll(objsQuadTree.get());
            }
        });

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByTimeRange(String startTime,
                                                                     String endTime,
                                                                     Tuple2<Double, Double> cornerFrom,
                                                                     Tuple2<Double, Double> cornerTo) {
        int start = Integer.valueOf(startTime);
        int end = Integer.valueOf(endTime);
        List<SpatioTextualObject> objects = new ArrayList<>();

        IntStream.rangeClosed(start, end).forEach(t -> {
            QuadTree quadTree = hour2QuadTree.get(t);
            if (quadTree != null) {
                Optional<List<SpatioTextualObject>> objsRange =
                        quadTree.getObjectsByRange(cornerFrom, cornerTo);
                if (objsRange.isPresent())
                    objects.addAll(objsRange.get());
            }
        });

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByTimeTerms(String startTime,
                                                                     String endTime,
                                                                     Set<String> terms) {
        int start = Integer.valueOf(startTime);
        int end = Integer.valueOf(endTime);
        List<SpatioTextualObject> objects = new ArrayList<>();

        IntStream.rangeClosed(start, end).forEach(t -> {
            QuadTree quadTree = hour2QuadTree.get(t);
            if (quadTree != null) {
                Optional<List<SpatioTextualObject>> objsTerms =
                        quadTree.getObjectsByTerms(terms);
                if (objsTerms.isPresent())
                    objects.addAll(objsTerms.get());
            }
        });

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByTimeRangeTerms(String startTime,
                                                                          String endTime,
                                                                          Tuple2<Double, Double> cornerFrom,
                                                                          Tuple2<Double, Double> cornerTo,
                                                                          Set<String> terms) {
        int start = Integer.valueOf(startTime);
        int end = Integer.valueOf(endTime);
        List<SpatioTextualObject> objects = new ArrayList<>();

        IntStream.rangeClosed(start, end).forEach(t -> {
            QuadTree quadTree = hour2QuadTree.get(t);
            if (quadTree != null) {
                Optional<List<SpatioTextualObject>> objsRangeTerms =
                        quadTree.getObjectsByRangeTerms(cornerFrom, cornerTo, terms);
                if (objsRangeTerms.isPresent())
                    objects.addAll(objsRangeTerms.get());
            }
        });

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public int getNumObjects() {
        int numObjects = 0;
        for (QuadTree quadTree : hour2QuadTree.values())
            numObjects += quadTree.getNumObjects();

        return numObjects;
    }

    public int getNumObjectsByHour(String hour) {
        QuadTree quadTree = hour2QuadTree.get(Integer.valueOf(hour));
        return quadTree.getNumObjects();
    }

    public void removeDataAtHour(String hour) {
        hour2QuadTree.remove(Integer.valueOf(hour));
    }

    public List<String> getHours() {
        return hour2QuadTree.keySet().stream().map(hour -> String.valueOf(hour)).collect(Collectors.toList());
    }

    public void collectDrawInfoByTime(String time,
                                      List<Tuple4<Double, Double, Double, Double>> rects,
                                      List<Tuple2<Double, Double>> points) {
        QuadTree quadTree = hour2QuadTree.get(Integer.valueOf(time));
        if (quadTree != null)
            quadTree.collectDrawInfo(rects, points);
    }

    public void printIndexInfo() {
        hour2QuadTree.forEach((hour, tree) -> {
            System.out.println("--------------------------");
            System.out.println(hour + ":");
            tree.printInfo();
        });
    }

    public HashMap<String, HashMap<QuadTreeNode, NodeStat>> getGroup2NodeStatByHour(
            Map<String, List<WarehouseQuery>> log, int hour) {
        QuadTree quadTree = hour2QuadTree.get(hour);
        return quadTree.getGroup2NodeStat(log);
    }

    public QuadTree getQuadTreeByHour(int hour) {
        return hour2QuadTree.get(hour);
    }
}
