package xxx.project.router.index;

import xxx.project.util.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class QuadTree implements Serializable {
    private static final long serialVersionUID = 1L;

    private QuadTreeNode root = null;
    private int nodeLimit;
    private HashMap<QuadTreeNode, Integer> node2Partition = null;

    public QuadTree(double minLat, double minLon,
                    double maxLat, double maxLon, int nodeLimit,
                    List<Tuple2<Double, Double>> points) {
        this.root = new QuadTreeNode(minLat, minLon, maxLat, maxLon, points);
        this.nodeLimit = nodeLimit;
        this.node2Partition = new HashMap<>();
    }

    public void bulkLoad() {
        if (root.getNumPoints() > nodeLimit)
            bulkLoad(root);
    }

    private void bulkLoad(QuadTreeNode node) {
        Map<Integer, List<Tuple2<Double, Double>>> groupedPoints =
                node.points.stream().collect(Collectors.groupingBy(p -> node.getChildId(p)));
        node.makeChildren(groupedPoints);

        for (QuadTreeNode child : node.children) {
            if (child.getNumPoints() > nodeLimit)
                bulkLoad(child);
        }
        node.points = null;
    }

    public void partitionNodes(int numPartitions, int partitionLimit) {
        setNodeId2Nodes();

        int[] curPartAndSum = {0, 0};
        partitionNodes(root, curPartAndSum, partitionLimit);
        assert(curPartAndSum[0] == numPartitions - 1);
    }

    private void partitionNodes(QuadTreeNode node, int[] curPartAndSum, int partitionLimit) {
        if (node.isLeaf()) {
            node2Partition.put(node, curPartAndSum[0]);
            if (curPartAndSum[1] + node.getNumPoints() >= partitionLimit) {
                ++curPartAndSum[0];
                curPartAndSum[1] = 0;
            } else
                curPartAndSum[1] += node.getNumPoints();
        } else {
            for (QuadTreeNode child : node.children)
                partitionNodes(child, curPartAndSum, partitionLimit);
        }
    }

    private void setNodeId2Nodes() {
        int[] curId = {0};
        setNodeId2Nodes(root, curId);
    }

    private void setNodeId2Nodes(QuadTreeNode node, int[] curId) {
        node.setNodeId(curId[0]);
        ++curId[0];

        if (!node.isLeaf()) {
            for (QuadTreeNode child : node.children)
                setNodeId2Nodes(child, curId);
        }
    }

    public Optional<Integer> getPartition(Tuple2<Double, Double> point) {
        double lat = point._1();
        double lon = point._2();

        if (lat >= root.latFrom && lon >= root.lonFrom && lat < root.latTo && lon < root.lonTo)
            return Optional.of(getPartition(root, point));
        else
            return Optional.ofNullable(null);
    }

    private int getPartition(QuadTreeNode node, Tuple2<Double, Double> point) {
        if (node.isLeaf())
            return node2Partition.get(node);
        else {
            int childId = node.getChildId(point);
            QuadTreeNode child = node.children[childId];
            return getPartition(child, point);
        }
    }

    public void removePoints() {
        removePoints(root);
    }

    private void removePoints(QuadTreeNode node) {
        if (node.points != null)
            node.points.clear();
        if (!node.isLeaf()) {
            for (QuadTreeNode child : node.children)
                removePoints(child);
        }
    }

    public void printPartitionInfo() {
        System.out.println("Partition information:");
        Map<Integer, List<Map.Entry<QuadTreeNode, Integer>>> partitionMap
                = node2Partition.entrySet().stream().collect(Collectors.groupingBy(e -> e.getValue()));
        Set<Map.Entry<Integer, List<Map.Entry<QuadTreeNode, Integer>>>> entries = partitionMap.entrySet();
        entries.forEach(e -> {
            int sum = e.getValue().stream()
                    .map(a -> a.getKey().getNumPoints())
                    .reduce(0, Integer::sum);
            System.out.print(String.format("Partition %d (#points = %d): ", e.getKey(), sum));
            e.getValue().forEach(v -> System.out.print(" " + v.getKey().nodeId));
            System.out.println();
        });
    }

    public class QuadTreeNode implements Serializable {
        private static final long serialVersionUID = 1L;

        private int nodeId = 0;

        private double latFrom;
        private double lonFrom;
        private double latTo;
        private double lonTo;

        private double centerLat;
        private double centerLon;

        private QuadTreeNode[] children = {null, null, null, null};
        private transient List<Tuple2<Double, Double>> points = null;

        public QuadTreeNode(double latFrom, double lonFrom,
                            double latTo, double lonTo,
                            List<Tuple2<Double, Double>> points) {
            this.latFrom = latFrom;
            this.lonFrom = lonFrom;
            this.latTo = latTo;
            this.lonTo = lonTo;

            centerLat = (latFrom + latTo) / 2.0;
            centerLon = (lonFrom + lonTo) / 2.0;
            this.points = points;
        }

        private void setNodeId(int id) {
            nodeId = id;
        }

        public int getChildId(double lat, double lon) {
            int t1, t2;
            if (lat < centerLat)
                t1 = 0;
            else
                t1 = 1;

            if (lon < centerLon)
                t2 =0;
            else
                t2 = 2;

            return t1 + t2;
        }

        public int getChildId(Tuple2<Double, Double> coord) {
            return getChildId(coord._1(), coord._2());
        }

        public int getNumPoints() {
            return points == null ? 0 : points.size();
        }

        public void makeChildren(Map<Integer, List<Tuple2<Double, Double>>> groupedPoints) {
            children[0] = new QuadTreeNode(latFrom, lonFrom, centerLat, centerLon, groupedPoints.get(0));
            children[1] = new QuadTreeNode(centerLat, lonFrom, latTo, centerLon, groupedPoints.get(1));
            children[2] = new QuadTreeNode(latFrom, centerLon, centerLat, lonTo, groupedPoints.get(2));
            children[3] = new QuadTreeNode(centerLat, centerLon, latTo, lonTo, groupedPoints.get(3));
        }

        public boolean isLeaf() {
            return children[0] == null;
        }

        @Override
        public int hashCode() {
            return nodeId;
        }
    }
}
