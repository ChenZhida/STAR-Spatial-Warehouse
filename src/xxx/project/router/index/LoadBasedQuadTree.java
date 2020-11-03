package xxx.project.router.index;

import xxx.project.util.Constants;
import xxx.project.util.Tuple2;

import java.io.*;
import java.util.HashMap;
import java.util.Optional;

public class LoadBasedQuadTree implements Serializable {
    private static final long serialVersionUID = 1L;

    private QuadTreeNode root = null;
    private HashMap<Long, Integer> node2Partition = null;

    public LoadBasedQuadTree(double minLat, double minLon,
                             double maxLat, double maxLon) {
        this.root = new QuadTreeNode(0, minLat, minLon, maxLat, maxLon);
        this.node2Partition = new HashMap<>();
    }

    public void readPartitionsFromFile(String fileName) throws IOException {
        File input = new File(fileName);
        BufferedReader reader = new BufferedReader(new FileReader(input));
        String firstLine = reader.readLine();
        String secondLine = reader.readLine();

        node2Partition = new HashMap<>();
        for (String pair : secondLine.split(" ")) {
            String[] list = pair.split(":");
            long hashCode = Long.valueOf(list[0]);
            int partition = Integer.valueOf(list[1]);
            node2Partition.put(hashCode, partition);
        }

        HashMap<Long, QuadTreeNode> hashCode2Node = new HashMap<>();
        String line;
        while ((line = reader.readLine()) != null) {
            if (!line.isEmpty()) {
                String[] array = line.split(" ");
                long hashCode = Long.valueOf(array[0]);
                int level = Integer.valueOf(array[1]);
                double latFrom = Double.valueOf(array[2]);
                double lonFrom = Double.valueOf(array[3]);
                double latTo = Double.valueOf(array[4]);
                double lonTo = Double.valueOf(array[5]);
                QuadTreeNode node = new QuadTreeNode(level, latFrom, lonFrom, latTo, lonTo);
                hashCode2Node.put(hashCode, node);
            }
        }

        int[] index = {0};
        root = readNodes(firstLine.split(" "), hashCode2Node, index);

        reader.close();
    }

    private QuadTreeNode readNodes(String[] strArray, HashMap<Long, QuadTreeNode> hashCode2Node, int[] index) {
        int idx = index[0];
        long hashCode = Long.valueOf(strArray[idx]);
        QuadTreeNode node = hashCode2Node.get(hashCode);
        ++index[0];

        idx = index[0];
        if (idx >= strArray.length)
            return node;
        if (!strArray[idx].equals("("))
            return node;

        ++index[0];
        for (int i = 0; i < node.children.length; ++i)
            node.children[i] = readNodes(strArray, hashCode2Node, index);
        ++index[0];

        return node;
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
            return node2Partition.get(node.getHashCode());
        else {
            int childId = node.getChildId(point);
            QuadTreeNode child = node.children[childId];
            return getPartition(child, point);
        }
    }

    public class QuadTreeNode implements Serializable {
        private static final long serialVersionUID = 1L;

        private int level;
        private double latFrom;
        private double lonFrom;
        private double latTo;
        private double lonTo;

        private double centerLat;
        private double centerLon;

        private QuadTreeNode[] children = {null, null, null, null};

        public QuadTreeNode(int level, double latFrom, double lonFrom, double latTo, double lonTo) {
            this.level = level;
            this.latFrom = latFrom;
            this.lonFrom = lonFrom;
            this.latTo = latTo;
            this.lonTo = lonTo;

            this.centerLat = (latFrom + latTo) / 2.0;
            this.centerLon = (lonFrom + lonTo) / 2.0;
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

        public boolean isLeaf() {
            return children[0] == null;
        }

        public long getHashCode() {
            double numSideCells = Math.pow(2, level);
            double unitLat = (Constants.MAX_LAT - Constants.MIN_LAT) / numSideCells;
            double unitLon = (Constants.MAX_LON - Constants.MIN_LON) / numSideCells;
            long indexLat = (long)((latFrom - Constants.MIN_LAT) / unitLat);
            long indexLon = (long)((lonFrom - Constants.MIN_LON) / unitLon);

            long hashCode = ((long)level) << (2 * Constants.QUAD_TREE_MAX_LEVEL);
            hashCode += ((long)indexLat) << Constants.QUAD_TREE_MAX_LEVEL;
            hashCode += indexLon;

            return hashCode;
        }
    }
}
