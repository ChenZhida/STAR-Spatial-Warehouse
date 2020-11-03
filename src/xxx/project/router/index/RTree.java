package xxx.project.router.index;

import xxx.project.util.Tuple2;
import xxx.project.util.Tuple4;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RTree implements Serializable {

    private static final long serialVersionUID = 1L;

    private RTreeNode root = null;
    private int fanout;
    private HashMap<RTreeNode, Integer> node2Partition = null;

    public RTreeNode bulkLoad(List<Tuple2<Double, Double>> points, int numLeafNodes) {
        fanout = (int)Math.ceil(Math.sqrt(numLeafNodes));
        ArrayList<RTreeNode> leafNodes = createLeafNodes(points, numLeafNodes);
        root = createHigherNodes(leafNodes);

        return root;
    }

    private ArrayList<RTreeNode> createLeafNodes(List<Tuple2<Double, Double>> points, int numLeafNodes) {
        Collections.sort(points, (a, b) -> (new Double(a._1())).compareTo(b._1()));
        ArrayList<ArrayList<Tuple2<Double, Double>>> groupedPoints = new ArrayList<>();
        int numSlices = (int)Math.ceil(Math.sqrt(numLeafNodes));
        int sliceSize = (int)Math.ceil((double)points.size() / numSlices);

        IntStream.range(0, numSlices)
                .forEach(i -> groupedPoints.add(new ArrayList<>()));
        IntStream.range(0, points.size())
                .forEach(i -> {
                    int index = i / sliceSize;
                    groupedPoints.get(index).add(points.get(i));
                });

        ArrayList<RTreeNode> leafs = new ArrayList<>();
        int bucketSize = (int)Math.ceil((double)points.size() / numLeafNodes);
        for (ArrayList<Tuple2<Double, Double>> slice : groupedPoints) {
            Collections.sort(slice, (a, b) -> (new Double(a._2())).compareTo(b._2()));

            for (int i = 0; i < slice.size(); i += bucketSize) {
                int endIndex = Math.min(slice.size(), i + bucketSize);
                Tuple4<Double, Double, Double, Double> bound = new Tuple4<>(Double.MAX_VALUE,
                        Double.MAX_VALUE,
                        -Double.MAX_VALUE,
                        -Double.MAX_VALUE);
                bound = slice.subList(i, endIndex).stream().reduce(bound,
                        (bnd, a) ->
                                new Tuple4<>(Math.min(bnd._1(), a._1()),
                                        Math.min(bnd._2(), a._2()),
                                        Math.max(bnd._3(), a._1()),
                                        Math.max(bnd._4(), a._2())),
                        (bnd1, bnd2) ->
                                new Tuple4<>(Math.min(bnd1._1(), bnd2._1()),
                                        Math.min(bnd1._2(), bnd2._2()),
                                        Math.max(bnd1._3(), bnd2._3()),
                                        Math.max(bnd1._4(), bnd2._4())));
                leafs.add(new RTreeNode(bound._1(), bound._2(), bound._3(), bound._4(), null));
            }
        }

        return leafs;
    }

    private RTreeNode createHigherNodes(ArrayList<RTreeNode> children) {
        if (children.size() <= fanout)
            return new RTreeNode(children);

        Collections.sort(children, (a, b) -> (new Double(a.centerX)).compareTo(b.centerX));
        ArrayList<ArrayList<RTreeNode>> groupedNodes = new ArrayList<>();
        int numSlices = (int)Math.ceil(Math.sqrt(fanout));
        int sliceSize = (int)Math.ceil((double)children.size() / numSlices);

        IntStream.range(0, numSlices)
                .forEach(i -> groupedNodes.add(new ArrayList<>()));
        IntStream.range(0, children.size())
                .forEach(i -> {
                    int index = i / sliceSize;
                    groupedNodes.get(index).add(children.get(i));
                });

        ArrayList<RTreeNode> higherNodes = new ArrayList<>();
        int bucketSize = (int)Math.ceil((double)children.size() / fanout);
        for (ArrayList<RTreeNode> slice : groupedNodes) {
            Collections.sort(slice, (a, b) -> (new Double(a.centerY)).compareTo(b.centerY));

            for (int i = 0; i < slice.size(); i += bucketSize) {
                int endIndex = Math.min(slice.size(), i + bucketSize);
                RTreeNode higherNode = new RTreeNode(new ArrayList<>(slice.subList(i, endIndex)));
                higherNodes.add(higherNode);
            }
        }

        return createHigherNodes(higherNodes);
    }

    public void partitionNodes(int numPartitions) {
        node2Partition = new HashMap<>();
        setNodeId2Nodes();
        ArrayList<RTreeNode> leafNodes = getLeafNodes();

        int sizeCeil = (int)Math.ceil((double)leafNodes.size() / numPartitions);
        int sizeFloor = (int)Math.floor((double)leafNodes.size() / numPartitions);
        int numCeils;

        if (leafNodes.size() % numPartitions == 0)
            numCeils = numPartitions;
        else
            numCeils = leafNodes.size() % numPartitions;

        IntStream.range(0, leafNodes.size())
                .forEach(i -> {
                    int partition = i / sizeCeil;
                    if (i >= numCeils * sizeCeil)
                        partition = numCeils + (i - numCeils * sizeCeil) / sizeFloor;

                    node2Partition.put(leafNodes.get(i), partition);
                });
    }

    private void setNodeId2Nodes() {
        ArrayList<Integer> curId = new ArrayList<>();
        curId.add(0);
        setNodeId2Nodes(root, curId);
    }

    private void setNodeId2Nodes(RTreeNode node, ArrayList<Integer> curId) {
        node.setNodeId(curId.get(0));
        curId.set(0, curId.get(0) + 1);

        if (node.children != null) {
            for (RTreeNode child : node.children)
                setNodeId2Nodes(child, curId);
        }
    }

    public Optional<Integer> getPartition(Tuple2<Double, Double> point) {
        if (root.isInside(point)) {
            ArrayList<RTreeNode> resLeaf = new ArrayList<>();
            getLeafByPoint(root, point, resLeaf);
            int partitionNo = node2Partition.get(resLeaf.get(0)); // at most one leaf as the sample data is point

            return Optional.of(partitionNo);
        }
        else
            return Optional.ofNullable(null);
    }

    private void getLeafByPoint(RTreeNode node, Tuple2<Double, Double> point,
                                ArrayList<RTreeNode> resLeaf) {
        if (node.isLeaf()) {
            resLeaf.add(node);
            return;
        }

        List<RTreeNode> candidates = new ArrayList<>();
        for (RTreeNode child : node.children) {
            if (child.isInside(point))
                candidates.add(child);
        }

        if (candidates.isEmpty()) {
            RTreeNode optNode = null;
            double minExpandedArea = Double.MAX_VALUE;
            for (RTreeNode child : node.children) {
                double areaBefore = (node.xMax - node.xMin) * (node.yMax - node.yMin);

                double newXMin = Double.min(node.xMin, point._1());
                double newYMin = Double.min(node.yMin, point._2());
                double newXMax = Double.max(node.xMax, point._1());
                double newYMax = Double.max(node.yMax, point._2());
                double areaAfter = (newXMax - newXMin) * (newYMax - newYMin);
                double expansion = areaAfter - areaBefore;

                if (expansion < minExpandedArea) {
                    minExpandedArea = expansion;
                    optNode = child;
                }
            }

            candidates.add(optNode);
        }

        for (RTreeNode child : candidates)
            getLeafByPoint(child, point, resLeaf);
    }

    private ArrayList<RTreeNode> getLeafNodes() {
        ArrayList<RTreeNode> leafNodes = new ArrayList<>();
        collectLeafNodes(root, leafNodes);
        return leafNodes;
    }

    private void collectLeafNodes(RTreeNode node, ArrayList<RTreeNode> leafNodes) {
        if (node.isLeaf()) {
            leafNodes.add(node);
            return;
        }

        for (RTreeNode child : node.children)
            collectLeafNodes(child, leafNodes);
    }

    public void printPartitionInfo() {
        System.out.println("Partition information:");
        System.out.println("Leaf nodes information:");
        getLeafNodes().forEach(
                n -> System.out.print(String.format(" %d:(%f, %f, %f, %f)",
                        n.nodeId, n.xMin, n.yMin, n.xMax, n.yMax)));
        System.out.println();

        Map<Integer, List<Map.Entry<RTreeNode, Integer>>> partitionMap
                = node2Partition.entrySet().stream().collect(Collectors.groupingBy(e -> e.getValue()));
        Set<Map.Entry<Integer, List<Map.Entry<RTreeNode, Integer>>>> entries = partitionMap.entrySet();
        entries.forEach(e -> {
            System.out.print(String.format("Partition %d: ", e.getKey()));
            e.getValue().forEach(v -> System.out.print(" " + v.getKey().nodeId));
            System.out.println();
        });
    }

    public class RTreeNode implements Serializable {

        private static final long serialVersionUID = 1L;

        private int nodeId = 0;

        private double xMin;
        private double yMin;
        private double xMax;
        private double yMax;

        private double centerX;
        private double centerY;

        private ArrayList<RTreeNode> children = null;

        public RTreeNode(double xMin, double yMin, double xMax, double yMax, ArrayList<RTreeNode> children) {
            this.xMin = xMin;
            this.yMin = yMin;
            this.xMax = xMax;
            this.yMax = yMax;
            this.centerX = (xMin + xMax) / 2.0;
            this.centerY = (yMin + yMax) / 2.0;
            this.children = children;
        }

        public RTreeNode(ArrayList<RTreeNode> children) {
            Tuple4<Double, Double, Double, Double> bound = new Tuple4<>(Double.MAX_VALUE,
                    Double.MAX_VALUE,
                    -Double.MAX_VALUE,
                    -Double.MAX_VALUE);
            bound = children.stream().reduce(bound,
                    (bnd, a) ->
                            new Tuple4<>(Math.min(bnd._1(), a.xMin),
                                    Math.min(bnd._2(), a.yMin),
                                    Math.max(bnd._3(), a.xMax),
                                    Math.max(bnd._4(), a.yMax)),
                    (bnd1, bnd2) ->
                            new Tuple4<>(Math.min(bnd1._1(), bnd2._1()),
                                    Math.min(bnd1._2(), bnd2._2()),
                                    Math.max(bnd1._3(), bnd2._3()),
                                    Math.max(bnd1._4(), bnd2._4())));

            xMin = bound._1();
            yMin = bound._2();
            xMax = bound._3();
            yMax = bound._4();

            centerX = (xMin + xMax) / 2.0;
            centerY = (yMin + yMax) / 2.0;

            this.children = children;
        }

        private boolean isLeaf() {
            return children == null;
        }

        private boolean isInside(Tuple2<Double, Double> point) {
            return (point._1() >= xMin && point._1() <= xMax &&
                    point._2() >= yMin && point._2() <= yMax);
        }

        private void setNodeId(int id) {
            this.nodeId = id;
        }

    }
}
