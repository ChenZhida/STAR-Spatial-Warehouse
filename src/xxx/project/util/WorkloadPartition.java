package xxx.project.util;

import xxx.project.worker.index.grid.GridCubes;
import xxx.project.worker.index.quadtree.QuadTreeCubes;
import xxx.project.worker.index.quadtree.QuadTreeNode;
import xxx.project.util.*;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WorkloadPartition {
    private static final double globalMinLat = Constants.MIN_LAT;
    private static final double globalMinLon = Constants.MIN_LON;
    private static final double globalMaxLat = Constants.MAX_LAT;
    private static final double globalMaxLon = Constants.MAX_LON;
    private static final int numLatCells = Constants.GRID_NUM_LAT_CELLS;
    private static final int numLonCells = Constants.GRID_NUM_LON_CELLS;
    private static final int quadTreeMaxLevel = Constants.QUAD_TREE_MAX_LEVEL;
    private static final double balanceFactor = 1.2;
    private static final double weightObject = 1.0;
    private static final double weightQuery = 1.0;
    private static final double weightUpdateCube = 1.0;

    private static double getQueryLoad(int numQueries, int cardinality) {
        return numQueries * cardinality;
    }

    /*
    private static double getPartitionLoad(double c1, double c2, List<QueryView> views,
                                           List<SpatioTextualObject> objects,
                                           List<WarehouseQuery> queries) {
        double load = 0.0;
        for (SpatioTextualObject obj : objects)
            load += c1 * getNumViews2Update(obj, views);
        for (WarehouseQuery query : queries) {
            List<QueryView> compViews = new ArrayList<>();
            List<QueryView> partViews = new ArrayList<>();
            int numCheckedObjs = getQueryLoadUsingViews(query, views, compViews, partViews);
            load += c2 * (compViews.size() + partViews.size() + numCheckedObjs);
        }

        return load;
    }

    private static int getNumViews2Update(SpatioTextualObject object, List<QueryView> views) {
        List<QueryView> updatedViews = views.stream().
                filter(v -> v.satisfyConstraint(object)).collect(Collectors.toList());

        return updatedViews.size();
    }

    private static int getQueryLoadUsingViews(WarehouseQuery query, List<QueryView> views,
                                              List<QueryView> compViews, List<QueryView> partViews) {
    }
     */

    // Grid based partitioning
    private static double computeLoadForGridCell(CellStat cellStat) {
        if (cellStat == null)
            return 0.0;

        HashMap<String, Integer> group2Level = GridCubes.group2Level;
        Comparator<String> comparator =
                Comparator.comparingInt(group2Level::get);
        List<String> groups = cellStat.getGroups();
        Collections.sort(groups, comparator);

        double sumLoad = 0.0;
        int numObjects = cellStat.getNumObjects();
        sumLoad += weightObject * numObjects;
        HashSet<String> materializedGroups = new HashSet<>();

        for (String group : groups) {
            int numQueries = cellStat.getNumQueriesByGroup(group);
            String ancestorGroupMinCard = materializedGroups.stream()
                    .filter(e -> GridCubes.isAncestorOf(e, group))
                    .min((a, b) -> {
                        int cardOfA = cellStat.getCardinality(a);
                        int cardOfB = cellStat.getCardinality(b);
                        return Integer.compare(cardOfA, cardOfB);
                    }).orElse(null);

            if (ancestorGroupMinCard != null) {
                double loadAncestor = weightQuery *
                        getQueryLoad(numQueries, cellStat.getCardinality(ancestorGroupMinCard));
                double loadCurrentCube = weightQuery * getQueryLoad(numQueries, cellStat.getCardinality(group))
                        + weightUpdateCube * numObjects;
                if (loadCurrentCube < loadAncestor) {
                    materializedGroups.add(group);
                    sumLoad += loadCurrentCube;
                } else
                    sumLoad += loadAncestor;
            } else {
                double queryLoad = getQueryLoad(numQueries, cellStat.getCardinality(group));
                double loadCube = weightQuery * queryLoad + weightUpdateCube * numObjects;
                double loadRaw = weightQuery * getQueryLoad(numQueries, numObjects);
                if (loadCube < loadRaw) {
                    materializedGroups.add(group);
                    sumLoad += loadCube;
                } else
                    sumLoad += loadRaw;
            }
        }

        return sumLoad;
    }

    public static class CellStat {
        private int numObjects;
        private Map<String, Integer> group2NumQueries;
        private Map<String, HashSet<String>> group2CombinedDimValue;

        public CellStat() {
            numObjects = 0;
            group2NumQueries = new HashMap<>();
            group2CombinedDimValue = new HashMap<>();
        }

        public int getNumObjects() {
            return numObjects;
        }

        public int getCardinality(String group) {
            return group2CombinedDimValue.get(group).size();
        }

        public Set<String> getCombinedDimValues(String group) {
            return group2CombinedDimValue.get(group);
        }

        public List<String> getGroups() {
            return new ArrayList<>(group2NumQueries.keySet());
        }

        public int getNumQueriesByGroup(String group) {
            return group2NumQueries.get(group);
        }

        public Map<String, HashSet<String>> getGroup2CombinedDimValue() {
            return group2CombinedDimValue;
        }

        public void incrementObject(SpatioTextualObject object) {
            ++numObjects;
            for (Map.Entry<String, HashSet<String>> entry : group2CombinedDimValue.entrySet()) {
                String group = entry.getKey();
                HashSet<String> combinedDimValue = entry.getValue();
                combinedDimValue.add(object.getGroupBy(group));
            }
        }

        public void incrementQuery(String group, WarehouseQuery query) {
            int numQueries = group2NumQueries.getOrDefault(group, 0);
            group2NumQueries.put(group, numQueries + 1);
            group2CombinedDimValue.put(group, new HashSet<>());
        }

        public List<Tuple2<String, Integer>> getQueryStats() {
            return group2NumQueries.entrySet().stream()
                    .map(e -> new Tuple2<>(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
        }
    }

    public static class GridBasedPartitioner {
        private class Partition {
            int indexLatFrom;
            int indexLonFrom;
            int indexLatTo;
            int indexLonTo;
            double load;

            public Partition(int indexLatFrom, int indexLonFrom, int indexLatTo, int indexLonTo, double load) {
                this.indexLatFrom = indexLatFrom;
                this.indexLonFrom = indexLonFrom;
                this.indexLatTo = indexLatTo;
                this.indexLonTo = indexLonTo;
                this.load = load;
            }

            public double getLoad() {
                return load;
            }

            public boolean isFinestGranularity() {
                return (indexLatFrom == indexLatTo) && (indexLonFrom == indexLonTo);
            }
        }

        private List<SpatioTextualObject> objects;
        private Map<String, List<WarehouseQuery>> log;
        private int numPartitions;
        private Map<Integer, CellStat> cell2Stat;

        public void partition(List<SpatioTextualObject> objects,
                              Map<String, List<WarehouseQuery>> log,
                              int numPartitions) {
            this.objects = objects;
            this.log = log;
            this.cell2Stat = getCellStats();
            this.numPartitions = numPartitions;

            List<Partition> partitions = partition();
            System.out.printf("The %d partitions:\n", numPartitions);
            partitions.forEach(p ->
                    System.out.printf("Partition: [(%d, %d), (%d, %d)], Load = %f\n",
                            p.indexLatFrom, p.indexLonFrom, p.indexLatTo, p.indexLonTo, p.getLoad()));
        }

        private List<Partition> partition() {
            double cells[][] = new double[numLatCells][numLonCells];
            double sumLoad = 0.0;
            for (int i = 0; i < numLatCells; ++i) {
                for (int j = 0; j < numLonCells; ++j) {
                    int keyCell = GridFunctions.getKeyCellByIndexes(i, j);
                    CellStat cellStat = cell2Stat.get(keyCell);
                    cells[i][j] = computeLoadForGridCell(cellStat);
                    sumLoad += cells[i][j];
                }
            }

            List<Partition> partitions = new ArrayList<>();
            Partition root = new Partition(0, 0, numLatCells - 1, numLonCells - 1, sumLoad);
            Comparator<Partition> comparator = Comparator.comparing(Partition::getLoad);
            PriorityQueue<Partition> queue = new PriorityQueue<>(comparator.reversed());
            queue.add(root);

            while (partitions.size() + queue.size() < numPartitions) {
                Partition partition = queue.poll();
                if (partition.isFinestGranularity() || partition.getLoad() < 1.0) {
                    System.out.printf("One partition has reached to the finest granularity:" +
                            " [(%d, %d), (%d, %d)], Load = %f\n", partition.indexLatFrom, partition.indexLonFrom,
                            partition.indexLatTo, partition.indexLonTo, partition.getLoad());
                    partitions.add(partition);
                    continue;
                }

                double optRatio = Double.POSITIVE_INFINITY;
                List<Partition> optChildren = null;

                for (int separator = partition.indexLatFrom; separator < partition.indexLatTo; ++separator) {
                    List<Partition> children = getChildrenPartitionedByLat(cells, partition, separator);
                    double load1 = children.get(0).getLoad();
                    double load2 = children.get(1).getLoad();
                    double ratio = Math.max(load1, load2) / Math.min(load1, load2);
                    if (ratio <= optRatio) {
                        optRatio = ratio;
                        optChildren = children;
                    }
                }

                for (int separator = partition.indexLonFrom; separator < partition.indexLonTo; ++separator) {
                    List<Partition> children = getChildrenPartitionedByLon(cells, partition, separator);
                    double load1 = children.get(0).getLoad();
                    double load2 = children.get(1).getLoad();
                    double ratio = Math.max(load1, load2) / Math.min(load1, load2);
                    if (ratio <= optRatio) {
                        optRatio = ratio;
                        optChildren = children;
                    }
                }

                for (Partition child : optChildren)
                    queue.add(child);
            }
            partitions.addAll(queue);

            return partitions;
        }

        List<Partition> getChildrenPartitionedByLat(double[][] cells, Partition partition, int separator) {
            int indexLatFrom = partition.indexLatFrom;
            int indexLonFrom = partition.indexLonFrom;
            int indexLatTo = separator;
            int indexLonTo = partition.indexLonTo;
            double load = 0.0;

            for (int i = indexLatFrom; i <= indexLatTo; ++i)
                for (int j = indexLonFrom; j <= indexLonTo; ++j)
                    load += cells[i][j];

            Partition upperChild = new Partition(indexLatFrom, indexLonFrom, indexLatTo, indexLonTo, load);

            indexLatFrom = separator + 1;
            indexLatTo = partition.indexLatTo;
            load = 0.0;

            for (int i = indexLatFrom; i <= indexLatTo; ++i)
                for (int j = indexLonFrom; j <= indexLonTo; ++j)
                    load += cells[i][j];

            Partition lowerChild = new Partition(indexLatFrom, indexLonFrom, indexLatTo, indexLonTo, load);

            return Arrays.asList(upperChild, lowerChild);
        }

        List<Partition> getChildrenPartitionedByLon(double[][] cells, Partition partition, int separator) {
            int indexLatFrom = partition.indexLatFrom;
            int indexLonFrom = partition.indexLonFrom;
            int indexLatTo = partition.indexLatTo;
            int indexLonTo = separator;
            double load = 0.0;

            for (int i = indexLatFrom; i <= indexLatTo; ++i)
                for (int j = indexLonFrom; j <= indexLonTo; ++j)
                    load += cells[i][j];

            Partition leftChild = new Partition(indexLatFrom, indexLonFrom, indexLatTo, indexLonTo, load);

            indexLonFrom = separator + 1;
            indexLonTo = partition.indexLonTo;
            load = 0.0;

            for (int i = indexLatFrom; i <= indexLatTo; ++i)
                for (int j = indexLonFrom; j <= indexLonTo; ++j)
                    load += cells[i][j];

            Partition rightChild = new Partition(indexLatFrom, indexLonFrom, indexLatTo, indexLonTo, load);

            return Arrays.asList(leftChild, rightChild);
        }

        public void printStats(int numTopsListed) {
            Comparator<CellStat> comparator = Comparator.comparing(CellStat::getNumObjects).reversed();
            List<CellStat> stats = cell2Stat.values().stream().sorted(comparator).collect(Collectors.toList());

            System.out.printf("The %d most number of objects in the cells " +
                    "(#objects, (group, #queries), ...):\n", numTopsListed);
            IntStream.range(0, numTopsListed).forEach(i -> {
                CellStat stat = stats.get(i);
                System.out.print(stat.getNumObjects());
                List<Tuple2<String, Integer>> queryStats = stat.getQueryStats();
                queryStats.forEach(e -> System.out.printf(" (%s, %d)", e._1(), e._2()));
                System.out.println();
            });

            double average = stats.parallelStream().mapToDouble(e -> e.getNumObjects()).average().orElse(0.0);
            double variance = stats.parallelStream()
                    .mapToDouble(e -> (e.getNumObjects() - average) * (e.getNumObjects() - average))
                    .sum() / stats.size();
            System.out.printf("Number of cells having objects: %d\n", stats.size());
            System.out.printf("The average number of objects in those cells: %f\n", average);
            System.out.printf("The standard variance: %f\n", Math.sqrt(variance));

            System.out.printf("The combined dimension values in the top %d cells (group: v1, v2, ...):\n", numTopsListed);
            IntStream.range(0, numTopsListed).forEach(i -> {
                CellStat stat = stats.get(i);
                Map<String, HashSet<String>> group2CombinedDimValue = stat.getGroup2CombinedDimValue();
                System.out.println("-----------------------------");
                group2CombinedDimValue.forEach((group, values) -> {
                    System.out.printf("%s:", group);
                    values.forEach(v -> System.out.printf(" (%s)", v));
                    System.out.println();
                });
                System.out.println();
            });
        }

        private Map<Integer, CellStat> getCellStats() {
            Map<Integer, CellStat> stats = new HashMap<>();

            for (Map.Entry<String, List<WarehouseQuery>> entry : log.entrySet()) {
                String group = entry.getKey();
                List<WarehouseQuery> queries = entry.getValue();
                for (WarehouseQuery query : queries) {
                    List<Integer> cells = GridFunctions.getKeysCellsByRange(globalMinLat, globalMinLon, globalMaxLat, globalMaxLon,
                            numLatCells, numLonCells, query.getRange().get());
                    for (Integer cell : cells) {
                        CellStat cellStat = stats.get(cell);
                        if (cellStat == null) {
                            cellStat = new CellStat();
                            stats.put(cell, cellStat);
                        }
                        cellStat.incrementQuery(group, query);
                    }
                }
            }

            for (SpatioTextualObject object : objects) {
                Tuple2<Double, Double> coord = object.getCoord();
                int cell = GridFunctions.getKeyCell(globalMinLat, globalMinLon, globalMaxLat, globalMaxLon, numLatCells, numLonCells, coord);

                CellStat cellStat = stats.get(cell);
                if (cellStat == null) {
                    cellStat = new CellStat();
                    stats.put(cell, cellStat);
                }
                cellStat.incrementObject(object);
            }

            return stats;
        }
    }

    public static void runGridBasedPartition(List<SpatioTextualObject> objects,
                                             Map<String, List<WarehouseQuery>> log,
                                             int numPartitions) {
        GridBasedPartitioner partitioner = new GridBasedPartitioner();
        partitioner.partition(objects, log, numPartitions);
    }

    // QuadTree based partitioning
    public static class NodeStat {
        int numCoveredQueries = 0;
        HashSet<String> distinctValues = new HashSet<>();

        public int getCardinality() {
            return distinctValues.size();
        }
    }

    private static class Node extends QuadTreeNode {
        public Node[] children = {null, null, null, null};
        public List<SpatioTextualObject> objects = null;
        public List<WarehouseQuery> queries = null;
        public HashMap<String, List<WarehouseQuery>> log = null;
        public HashMap<String, NodeStat> group2Stat = null;

        public Node(int level, double minLat, double minLon, double maxLat, double maxLon) {
            super(level, minLat, minLon, maxLat, maxLon);
        }

        public Node(int level, double minLat, double minLon, double maxLat, double maxLon,
                    List<SpatioTextualObject> objects, List<WarehouseQuery> queries) {
            super(level, minLat, minLon, maxLat, maxLon);
            this.objects = objects;
            this.queries = queries;
        }

        public boolean isLeaf() {
            return children[0] == null;
        }

        public void addObject(SpatioTextualObject object) {
            if (objects == null)
                objects = new ArrayList<>();
            objects.add(object);
        }

        public List<SpatioTextualObject> getObjects() {
            return objects == null ? new ArrayList<>() : objects;
        }

        public int getNumObjects() {
            return objects == null ? 0 : objects.size();
        }

        public void addQuery(WarehouseQuery query) {
            if (queries == null)
                queries = new ArrayList<>();
            queries.add(query);

            if (log == null)
                log = new HashMap<>();
            String group = query.getGroupBy();

            List<WarehouseQuery> queryList = log.get(group);
            if (queryList == null) {
                queryList = new ArrayList<>();
                log.put(group, queryList);
            }
            queryList.add(query);
        }

        public List<WarehouseQuery> getQueries() {
            return queries == null ? new ArrayList<>() : queries;
        }

        public int getNumQueries() {
            return queries == null ? 0 : queries.size();
        }

        public int getNumQueriesByGroup(String group) {
            List<WarehouseQuery> queries = log.get(group);
            return queries == null ? 0 : queries.size();
        }

        public HashMap<String, NodeStat> computeStatistics() {
            if (group2Stat != null)
                return group2Stat;

            group2Stat = new HashMap<>();
            if (queries != null) {
                for (Map.Entry<String, List<WarehouseQuery>> entry : log.entrySet()) {
                    String group = entry.getKey();
                    List<WarehouseQuery> qs = entry.getValue();

                    NodeStat stat = new NodeStat();
                    group2Stat.put(group, stat);

                    for (WarehouseQuery q : qs)
                        if (isEnclosingNodeRange(q.getRange().get()))
                            ++stat.numCoveredQueries;
                }
            }

            if (objects != null) {
                for (Map.Entry<String, NodeStat> entry : group2Stat.entrySet()) {
                    String group = entry.getKey();
                    NodeStat stat = entry.getValue();

                    for (SpatioTextualObject object : objects) {
                        String value = object.getGroupBy(group);
                        stat.distinctValues.add(value);
                    }
                }
            }

            return group2Stat;
        }

        public static long getHashCode(int level, double minLat, double minLon) {
            double numSideCells = Math.pow(2, level);
            double unitLat = (Constants.MAX_LAT - Constants.MIN_LAT) / numSideCells;
            double unitLon = (Constants.MAX_LON - Constants.MIN_LON) / numSideCells;
            long indexLat = (long)((minLat - Constants.MIN_LAT) / unitLat);
            long indexLon = (long)((minLon - Constants.MIN_LON) / unitLon);

            long hashCode = ((long)level) << (2 * quadTreeMaxLevel);
            hashCode += ((long)indexLat) << quadTreeMaxLevel;
            hashCode += indexLon;

            return hashCode;
        }

        public void printNodeInfo() {
            System.out.println("---------Node Info----------");
            System.out.printf("Level = %d, minLat = %f, minLon = %f, maxLat = %f, maxLon = %f\n",
                    level, minLat, minLon, maxLat, maxLon);
            System.out.println("Number of objects = " + getNumObjects());
            System.out.println("Number of queries = " + getNumQueries());
            System.out.println("Group2Cardinality and numCoveredQueries: ");
            if (group2Stat != null) {
                group2Stat.forEach((k, v) -> System.out.printf("\"%s\"=(%d, %d) ",
                        k, v.getCardinality(), v.numCoveredQueries));
                System.out.println();
            }
        }
    }

    public static void runQuadTreeBasedPartition(List<SpatioTextualObject> objects,
                                                 List<WarehouseQuery> queries,
                                                 int numPartitions) {
        QuadTreeBasedPartitioner partitioner = new QuadTreeBasedPartitioner();
        String fileName = "resources/partitions_quadTree_" + numPartitions + ".txt";
        partitioner.partition(objects, queries, numPartitions, fileName);

        try {
            partitioner.readPartitionsFromFile(fileName);
        } catch (IOException e) {
            System.out.println("Cannot read partitions from file: " + fileName);
            System.exit(1);
        }
    }

    public static class QuadTreeBasedPartitioner {
        private List<SpatioTextualObject> objects;
        private List<WarehouseQuery> queries;
        private int numPartitions;

        private HashMap<Long, Node> existedNodes = new HashMap<>();
        private HashMap<Node, Double> node2Load = new HashMap<>();

        public void partition(List<SpatioTextualObject> objects,
                              List<WarehouseQuery> queries,
                              int numPartitions,
                              String fileName) {
            this.objects = objects;
            this.queries = queries;
            this.numPartitions = numPartitions;

            HashMap<Node, Integer> node2Partition = new HashMap<>();
            double[] partitionLoad = new double[numPartitions];
            for (int i = 0; i < numPartitions; ++i)
                partitionLoad[i] = 0.0;

            Node root = partition(node2Partition, partitionLoad);
            System.out.printf("The load of %d partitions:", numPartitions);
            for (double load : partitionLoad)
                System.out.printf(" %f", load);
            System.out.println();

            int[] numObjectsOfPartitions = new int[numPartitions];
            for (int i = 0; i < numPartitions; ++i)
                numObjectsOfPartitions[i] = 0;
            for (Map.Entry<Node, Integer> entry : node2Partition.entrySet()) {
                int partition = entry.getValue();
                int numObjects = entry.getKey().getNumObjects();
                numObjectsOfPartitions[partition] += numObjects;
            }
            System.out.printf("#objects in each partition:");
            Arrays.stream(numObjectsOfPartitions).forEach(n -> System.out.print(" " + n));
            System.out.println();

            System.out.println("Node2Partition:");
            node2Partition.forEach((node, partition) -> {
                System.out.printf("Partition %d: ", partition);
                System.out.printf("Level=%d, MinLat=%f, MinLon=%f, MaxLat=%f, MaxLon=%f, ",
                        node.level, node.minLat, node.minLon, node.maxLat, node.maxLon);
                System.out.printf("NumObjects=%d, NumQueries=%d\n", node.getNumObjects(), node.getNumQueries());
            });

            try {
                writePartitions2File(root, node2Partition, fileName);
                System.out.println("The information of the tree:");
                printNodes(root);
            } catch (IOException e) {
                System.err.println("Cannot write partitions to the file: " + fileName);
                System.exit(1);
            }
        }

        private Node partition(HashMap<Node, Integer> node2Partition, double[] partitionLoad) {
            Node root = new Node(0, globalMinLat, globalMinLon, globalMaxLat, globalMaxLon, objects, queries);
            ArrayList<Node> candidates = new ArrayList<>();
            ArrayList<Node> nodesCannotSplit = new ArrayList<>();

            candidates.add(root);
            while (candidates.size() + nodesCannotSplit.size() < 4 * numPartitions) {
                Node node2Split = null;
                if (candidates.size() + nodesCannotSplit.size() >= numPartitions) {
                    double[] minWorkerLoad = {0.0};
                    if (checkLoadBalance(root, candidates, nodesCannotSplit, minWorkerLoad)) {
                        System.out.println("Load balancing!");
                        break;
                    }

                    Node maxLoadNode = null;
                    double maxLoad = -Double.MAX_VALUE;

                    for (Node node: candidates) {
                        double load = computeLoadForQuadTreeNode(node);
                        if (load > maxLoad) {
                            maxLoad = load;
                            maxLoadNode = node;
                        }
                    }

                    if (maxLoad > balanceFactor * minWorkerLoad[0])
                        node2Split = maxLoadNode;
                }

                if (node2Split == null)
                    node2Split = pickNode2Split(candidates);

                int curLevel = node2Split.level;
                if (curLevel >= quadTreeMaxLevel) {
                    candidates.remove(node2Split);
                    nodesCannotSplit.add(node2Split);
                    continue;
                }

                List<Node> children = getChildrenNodes(node2Split);
                node2Split.children[0] = children.get(0);
                node2Split.children[1] = children.get(1);
                node2Split.children[2] = children.get(2);
                node2Split.children[3] = children.get(3);

                System.out.println("Splitting operation");
                System.out.println("The split node:");
                node2Split.printNodeInfo();
                System.out.println("The created nodes:");
                children.forEach(c -> c.printNodeInfo());

                candidates.remove(node2Split);
                for (Node child : node2Split.children)
                    candidates.add(child);
            }

            List<Node> nodes = new ArrayList<>(candidates);
            nodes.addAll(nodesCannotSplit);
            assignPartition2Node(root, nodes, node2Partition, partitionLoad);

            return root;
        }

        private void writePartitions2File(Node root, HashMap<Node, Integer> node2Partition,
                                          String fileName) throws IOException {
            File output = new File(fileName);
            BufferedWriter writer = new BufferedWriter(new FileWriter(output));

            ArrayList<String> strArray = new ArrayList<>();
            setFirstLine(root, strArray);
            StringBuilder strBuf = new StringBuilder();
            for (String str : strArray)
                strBuf.append(str + " ");
            strBuf.append("\n");

            for (Map.Entry<Node, Integer> entry : node2Partition.entrySet()) {
                Node node = entry.getKey();
                int partition = entry.getValue();
                long hashCode = node.getHashCode(node.level, node.minLat, node.minLon);
                strBuf.append(hashCode + ":");
                strBuf.append(partition + " ");
            }
            strBuf.append("\n");

            strArray.clear();
            setRemainLines(root, strArray);
            for (String str : strArray)
                strBuf.append(str + "\n");

            writer.write(strBuf.toString());
            writer.close();
        }

        public void readPartitionsFromFile(String fileName) throws IOException {
            File input = new File(fileName);
            BufferedReader reader = new BufferedReader(new FileReader(input));
            String firstLine = reader.readLine();
            String secondLine = reader.readLine();

            HashMap<Long, Integer> hashCode2Partition = new HashMap<>();
            for (String pair : secondLine.split(" ")) {
                String[] list = pair.split(":");
                long hashCode = Long.valueOf(list[0]);
                int partition = Integer.valueOf(list[1]);
                hashCode2Partition.put(hashCode, partition);
            }

            HashMap<Long, Node> hashCode2Node = new HashMap<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    String[] array = line.split(" ");
                    long hashCode = Long.valueOf(array[0]);
                    int level = Integer.valueOf(array[1]);
                    double minLat = Double.valueOf(array[2]);
                    double minLon = Double.valueOf(array[3]);
                    double maxLat = Double.valueOf(array[4]);
                    double maxLon = Double.valueOf(array[5]);
                    Node node = new Node(level, minLat, minLon, maxLat, maxLon);
                    hashCode2Node.put(hashCode, node);
                }
            }

            int[] index = {0};
            Node root = readNodes(firstLine.split(" "), hashCode2Node, index);
            System.out.println("The information of the tree");
            printNodes(root);

            reader.close();
        }

        private Node readNodes(String[] strArray, HashMap<Long, Node> hashCode2Node, int[] index) {
            int idx = index[0];
            long hashCode = Long.valueOf(strArray[idx]);
            Node node = hashCode2Node.get(hashCode);
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

        private void setFirstLine(Node node, ArrayList<String> strArray) {
            long hashCode = Node.getHashCode(node.level, node.minLat, node.minLon);
            strArray.add(String.valueOf(hashCode));

            if (node.isLeaf())
                return;
            else {
                strArray.add("(");
                Node[] children = node.children;
                setFirstLine(children[0], strArray);
                setFirstLine(children[1], strArray);
                setFirstLine(children[2], strArray);
                setFirstLine(children[3], strArray);
                strArray.add(")");
            }
        }

        private void setRemainLines(Node node, ArrayList<String> strArray) {
            long hashCode = Node.getHashCode(node.level, node.minLat, node.minLon);
            String str = hashCode + " " + node.level + " " + node.minLat + " " + node.minLon + " " + node.maxLat + " " + node.maxLon;
            strArray.add(str);

            if (node.isLeaf())
                return;
            else {
                Node[] children = node.children;
                setRemainLines(children[0], strArray);
                setRemainLines(children[1], strArray);
                setRemainLines(children[2], strArray);
                setRemainLines(children[3], strArray);
            }
        }

        private Node pickNode2Split(List<Node> candidates) {
            if (candidates.size() == 1)
                return candidates.get(0);

            Node node2Split = null;
            double optDeltaLoad = -Double.MAX_VALUE;

            for (Node node : candidates) {
                double loadBefore = computeLoadForQuadTreeNode(node);
                List<Node> children = getChildrenNodes(node);

                double loadAfter = 0.0;
                for (Node child : children)
                    loadAfter += computeLoadForQuadTreeNode(child);

                double deltaLoad = loadBefore - loadAfter;
                if (deltaLoad > optDeltaLoad) {
                    optDeltaLoad = deltaLoad;
                    node2Split = node;
                }
            }

            return node2Split;
        }

        private double computeLoadForQuadTreeNode(Node node) {
            Double load = node2Load.get(node);
            if (load != null)
                return load;

            HashMap<String, NodeStat> group2Stats = node.computeStatistics();

            HashMap<String, Integer> group2Level = GridCubes.group2Level;
            Comparator<String> comparator =
                    Comparator.comparingInt(group2Level::get);
            List<String> groups = new ArrayList<>(group2Stats.keySet());
            Collections.sort(groups, comparator);

            double sumLoad = 0.0;
            int numObjects = node.getNumObjects();
            sumLoad += weightObject * numObjects;
            HashSet<String> materializedGroups = new HashSet<>();

            for (String group : groups) {
                NodeStat stat = group2Stats.get(group);
                int numQueries = node.getNumQueriesByGroup(group);
                String ancestorGroupMinCard = materializedGroups.stream()
                        .filter(e -> GridCubes.isAncestorOf(e, group))
                        .min((a, b) -> {
                            int cardOfA = group2Stats.get(a).getCardinality();
                            int cardOfB = group2Stats.get(b).getCardinality();
                            return Integer.compare(cardOfA, cardOfB);
                        }).orElse(null);

                if (ancestorGroupMinCard != null) {
                    double loadAncestor = weightQuery *
                            getQueryLoad(numQueries, group2Stats.get(ancestorGroupMinCard).getCardinality());
                    double loadCurrentCube = weightQuery * getQueryLoad(numQueries, stat.getCardinality())
                            + weightUpdateCube * numObjects;
                    if (loadCurrentCube < loadAncestor) {
                        materializedGroups.add(group);
                        sumLoad += loadCurrentCube;
                    } else
                        sumLoad += loadAncestor;
                } else {
                    double queryLoad = getQueryLoad(numQueries, stat.getCardinality());
                    double loadCube = weightQuery * queryLoad + weightUpdateCube * numObjects;
                    double loadRaw = weightQuery * getQueryLoad(numQueries, numObjects);
                    if (loadCube < loadRaw) {
                        materializedGroups.add(group);
                        sumLoad += loadCube;
                    } else
                        sumLoad += loadRaw;
                }
            }

            node2Load.put(node, sumLoad);
            return sumLoad;
        }

        private List<Node> getChildrenNodes(Node node) {
            int curLevel = node.level;
            double middleLat = (node.minLat + node.maxLat) / 2.0;
            double middleLon = (node.minLon + node.maxLon) / 2.0;

            List<Node> children = new ArrayList<>();
            long hashCode = Node.getHashCode(curLevel + 1, node.minLat, node.minLon);
            Node child = existedNodes.get(hashCode);

            if (child != null) {
                children.add(child);

                hashCode = Node.getHashCode(curLevel + 1, node.minLat, middleLon);
                children.add(existedNodes.get(hashCode));

                hashCode = Node.getHashCode(curLevel + 1, middleLat, node.minLon);
                children.add(existedNodes.get(hashCode));

                hashCode = Node.getHashCode(curLevel + 1, middleLat, middleLon);
                children.add(existedNodes.get(hashCode));
            }
            else {
                child = new Node(curLevel + 1, node.minLat, node.minLon, middleLat, middleLon);
                hashCode = Node.getHashCode(curLevel + 1, node.minLat, node.minLon);
                existedNodes.put(hashCode, child);
                children.add(child);

                child = new Node(curLevel + 1, node.minLat, middleLon, middleLat, node.maxLon);
                hashCode = Node.getHashCode(curLevel + 1, node.minLat, middleLon);
                existedNodes.put(hashCode, child);
                children.add(child);

                child = new Node(curLevel + 1, middleLat, node.minLon, node.maxLat, middleLon);
                hashCode = Node.getHashCode(curLevel + 1, middleLat, node.minLon);
                existedNodes.put(hashCode, child);
                children.add(child);

                child = new Node(curLevel + 1, middleLat, middleLon, node.maxLat, node.maxLon);
                hashCode = Node.getHashCode(curLevel + 1, middleLat, middleLon);
                existedNodes.put(hashCode, child);
                children.add(child);

                for (SpatioTextualObject object : node.getObjects()) {
                    for (Node ch : children) {
                        if (ch.isInsideNodeRange(object.getCoord()))
                            ch.addObject(object);
                    }
                }

                for (WarehouseQuery query : node.getQueries()) {
                    for (Node ch : children) {
                        if (ch.isOverlappedNodeRange(query.getRange().get()))
                            ch.addQuery(query);
                    }
                }
            }

            return children;
        }

        private boolean checkLoadBalance(Node root, List<Node> candidates,
                                         List<Node> nodesCannotSplit,
                                         double[] minWorkerLoad) {
            double sumLoad = 0.0;
            for (Node node : candidates)
                sumLoad += computeLoadForQuadTreeNode(node);
            for (Node node : nodesCannotSplit)
                sumLoad += computeLoadForQuadTreeNode(node);

            double partitionLimit = sumLoad / numPartitions;
            int[] partitionNo = {0};
            double[] partitionLoad = new double[numPartitions];
            for (int i = 0; i < numPartitions; ++i)
                partitionLoad[i] = 0.0;

            HashMap<Node, Integer> node2Partition = new HashMap<>();
            partitionNodes(root, partitionLimit, node2Partition, partitionNo, partitionLoad);

            double maxLoad = Arrays.stream(partitionLoad).max().getAsDouble();
            double minLoad = Arrays.stream(partitionLoad).min().getAsDouble();
            minWorkerLoad[0] = minLoad;
            return (maxLoad / minLoad) <= balanceFactor;
        }

        private void assignPartition2Node(Node root, List<Node> nodes,
                                          HashMap<Node, Integer> node2Partition,
                                          double[] partitionLoad) {
            double sumLoad = 0.0;
            for (Node node : nodes)
                sumLoad += computeLoadForQuadTreeNode(node);

            double partitionLimit = sumLoad / numPartitions;
            int[] partitionNo = {0};
            for (int i = 0; i < numPartitions; ++i)
                partitionLoad[i] = 0.0;

            partitionNodes(root, partitionLimit, node2Partition, partitionNo, partitionLoad);
        }

        public void partitionNodes(Node node, double partitionLimit, HashMap<Node, Integer> node2Partition,
                                   int[] partitionNo, double[] partitionLoad) {
            if (node.isLeaf()) {
                node2Partition.put(node, partitionNo[0]);
                double nodeLoad = computeLoadForQuadTreeNode(node);
                partitionLoad[partitionNo[0]] += nodeLoad;
                if (partitionLoad[partitionNo[0]] > partitionLimit)
                    ++partitionNo[0];

                return;
            }

            for (Node child : node.children)
                partitionNodes(child, partitionLimit, node2Partition, partitionNo, partitionLoad);
        }

        public void printNodes(Node node) {
            if (node == null)
                return;

            long hashCode = node.getHashCode(node.level, node.minLat, node.minLon);
            System.out.printf("%d: %d %f %f %f %f\n", hashCode, node.level, node.minLat, node.minLon, node.maxLat, node.maxLon);
            for (Node child : node.children)
                printNodes(child);
        }
    }

    public static class PartitionerQuadTree {
        public static class Node implements Cloneable, Serializable {
            public static final long serialVersionUID = 101L;

            private int nodeId;
            private double latFrom;
            private double lonFrom;
            private double latTo;
            private double lonTo;
            private double centerLat;
            private double centerLon;
            private int numObjectsPassed = 0;
            private Node[] children = {null, null, null, null};
            private final int NODE_CAPACITY = 1000;
            public int level;
            public HashMap<String, NodeStat> group2Stat = null;
            public HashMap<String, List<WarehouseQuery>> log = null;

            private List<SpatioTextualObject> objects = new ArrayList<>();
            private List<WarehouseQuery> queries = new ArrayList<>();
            private Map<String, List<Integer>> term2ObjectIds = null;
            private Set<String> candidateViews = new HashSet<>();
            private Map<String, Integer> group2NumQueries = new HashMap<>();
            private Map<String, Integer> view2Size = new HashMap<>();

            public Node(int nodeId, double latFrom, double lonFrom,
                        double latTo, double lonTo, int level) {
                this.nodeId = nodeId;
                this.latFrom = latFrom;
                this.lonFrom = lonFrom;
                this.latTo = latTo;
                this.lonTo = lonTo;
                this.centerLat = (latFrom + latTo) / 2.0;
                this.centerLon = (lonFrom + lonTo) / 2.0;
                this.level = level;
            }

            public Node getCopy() {
                return new Node(nodeId, latFrom, lonFrom, latTo, lonTo, level);
            }

            public Object clone() {
                Node o = null;
                try {
                    o = (Node)super.clone();
                } catch (CloneNotSupportedException e) {
                    System.out.println(e.toString());
                }

                return o;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null) return false;

                Node that = (Node)o;
                return nodeId == that.nodeId;
            }

            @Override
            public int hashCode() {
                return nodeId;
            }

            public int getNodeId() {
                return nodeId;
            }

            public Map<String, Integer> getGroup2NumQueries() {
                return group2NumQueries;
            }

            public double getLatFrom() {
                return latFrom;
            }

            public double getLonFrom() {
                return lonFrom;
            }

            public double getLatTo() {
                return latTo;
            }

            public double getLonTo() {
                return lonTo;
            }

            public double getCenterLat() {
                return centerLat;
            }

            public double getCenterLon() {
                return centerLon;
            }

            public Node[] getChildren() {
                return children;
            }

            public List<SpatioTextualObject> getObjects() {
                return objects;
            }

            public List<WarehouseQuery> getQueries() {
                return queries;
            }

            public Set<String> getCandidateViews() {
                return candidateViews;
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

            public boolean isFull() {
                return  objects.size() >= NODE_CAPACITY;
            }

            public boolean isInsideNodeRange(Tuple2<Double, Double> coord) {
                double lat = coord._1();
                double lon = coord._2();

                return latFrom <= lat && latTo > lat &&
                        lonFrom <= lon && lonTo > lon;
            }

            public boolean isInsideNodeRange(Tuple4<Double, Double, Double, Double> range) {
                return latFrom < range._1() && lonFrom < range._2() &&
                        latTo > range._3() && lonTo > range._4();
            }

            public boolean isOverlappedNodeRange(Tuple2<Double, Double> cornerFrom,
                                                 Tuple2<Double, Double> cornerTo) {
                double smallLat = cornerFrom._1();
                double smallLon = cornerFrom._2();
                double largeLat = cornerTo._1();
                double largeLon = cornerTo._2();
                boolean nonOverlapped = largeLat <= latFrom || smallLat >= latTo ||
                        largeLon <= lonFrom || smallLon >= lonTo;

                return !nonOverlapped;
            }

            public boolean isOverlappedNodeRange(Tuple4<Double, Double, Double, Double> range) {
                Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
                Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());

                return isOverlappedNodeRange(cornerFrom, cornerTo);
            }

            public boolean isEnclosingNodeRange(Tuple2<Double, Double> cornerFrom,
                                                Tuple2<Double, Double> cornerTo) {
                double latFrom = cornerFrom._1();
                double lonFrom = cornerFrom._2();
                double latTo = cornerTo._1();
                double lonTo = cornerTo._2();

                return latFrom <= this.latFrom && latTo >= this.latTo &&
                        lonFrom <= this.lonFrom && lonTo >= this.lonTo;
            }

            public boolean isEnclosingNodeRange(Tuple4<Double, Double, Double, Double> range) {
                Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
                Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());

                return isEnclosingNodeRange(cornerFrom, cornerTo);
            }

            public int getNumObjectsPassed() {
                return numObjectsPassed;
            }

            public int getNumQueriesByGroup(String group) {
                List<WarehouseQuery> queries = log.get(group);
                return queries == null ? 0 : queries.size();
            }

            public void insertObject(SpatioTextualObject object) {
                objects.add(object);
                ++numObjectsPassed;
            }

            public void insertQuery(WarehouseQuery query) {
                queries.add(query);
                String group = query.getGroupBy();
                List<String> ancestorGroups = Arrays.stream(Constants.GROUPS).
                        filter(g -> GridCubes.isAncestorOf(g, group)).
                        collect(Collectors.toList());
                candidateViews.addAll(ancestorGroups);

                int numQueries = group2NumQueries.getOrDefault(group, 0) + 1;
                group2NumQueries.put(group, numQueries);

                if (log == null)
                    log = new HashMap<>();

                List<WarehouseQuery> queryList = log.get(group);
                if (queryList == null) {
                    queryList = new ArrayList<>();
                    log.put(group, queryList);
                }
                queryList.add(query);
            }

            public void clearObjects() {
                objects.clear();
                if (term2ObjectIds != null) {
                    term2ObjectIds.clear();
                    term2ObjectIds = null;
                }
            }

            public int getNumObjects() {
                return objects == null ? 0 : objects.size();
            }

            public boolean containQueries() {
                return !queries.isEmpty();
            }

            public void buildViewSize(List<SpatioTextualObject> remainObjs) {
                for (String group : candidateViews) {
                    Set<String> valSet = new HashSet<>();
                    for (SpatioTextualObject object : remainObjs) {
                        String val = object.getGroupBy(group);
                        valSet.add(val);
                    }
                    view2Size.put(group, valSet.size());
                }
            }

            public Map<String, Integer> getView2Size() {
                return view2Size;
            }

            public HashMap<String, NodeStat> computeStatistics() {
                if (group2Stat != null)
                    return group2Stat;

                group2Stat = new HashMap<>();
                if (queries != null) {
                    for (Map.Entry<String, List<WarehouseQuery>> entry : log.entrySet()) {
                        String group = entry.getKey();
                        List<WarehouseQuery> qs = entry.getValue();

                        NodeStat stat = new NodeStat();
                        group2Stat.put(group, stat);

                        for (WarehouseQuery q : qs)
                            if (isEnclosingNodeRange(q.getRange().get()))
                                ++stat.numCoveredQueries;
                    }
                }

                if (objects != null) {
                    for (Map.Entry<String, NodeStat> entry : group2Stat.entrySet()) {
                        String group = entry.getKey();
                        NodeStat stat = entry.getValue();

                        for (SpatioTextualObject object : objects) {
                            String value = object.getGroupBy(group);
                            stat.distinctValues.add(value);
                        }
                    }
                }

                return group2Stat;
            }

            public static long getHashCode(int level, double minLat, double minLon) {
                double numSideCells = Math.pow(2, level);
                double unitLat = (Constants.MAX_LAT - Constants.MIN_LAT) / numSideCells;
                double unitLon = (Constants.MAX_LON - Constants.MIN_LON) / numSideCells;
                long indexLat = (long)((minLat - Constants.MIN_LAT) / unitLat);
                long indexLon = (long)((minLon - Constants.MIN_LON) / unitLon);

                long hashCode = ((long)level) << (2 * quadTreeMaxLevel);
                hashCode += ((long)indexLat) << quadTreeMaxLevel;
                hashCode += indexLon;

                return hashCode;
            }

            private void buildObjectIndex() {
                term2ObjectIds = new HashMap<>();
                for (int i = 0; i < objects.size(); ++i) {
                    SpatioTextualObject obj = objects.get(i);
                    Set<String> terms = obj.getTerms();
                    for (String term : terms) {
                        List<Integer> objIds = term2ObjectIds.get(term);
                        if (objIds == null) {
                            objIds = new ArrayList<>();
                            term2ObjectIds.put(term, objIds);
                        }
                        objIds.add(i);
                    }
                }
            }
        }

        public Node root = null;
        private List<SpatioTextualObject> objects = null;

        public void buildQuadTree(List<SpatioTextualObject> objects) {
            this.objects = new ArrayList<>(objects);
            int nodeId = 0;
            int level = 0;
            root = new Node(nodeId++, Constants.MIN_LAT, Constants.MIN_LON,
                    Constants.MAX_LAT, Constants.MAX_LON, level);
            for (SpatioTextualObject object : objects)
                insertObject(object);
        }

        public void insertQueries(List<WarehouseQuery> queries) {
            for (WarehouseQuery query : queries) {
                insertQuery(root, query);
            }
        }

        public void insertObject(SpatioTextualObject object) {
            Node node = root;
            while (!node.isLeaf())
                node = findChild(node, object);

            node.insertObject(object);
            adjustNodes(node);
        }

        public void buildCandidateViewSizes() {
            buildCandViewSizesHelper(root);
        }

        public List<QueryView> buildCandidateViews(Map<Integer, Node> viewId2Node) {
            List<QueryView> candViews = new ArrayList<>();
            List<Integer> viewId = new ArrayList<>();
            viewId.add(0);
            buildCandViewsHelper(root, viewId, candViews, viewId2Node);

            return candViews;
        }

        private void buildCandViewSizesHelper(Node node) {
            if (node == null)
                return;

            if (node.containQueries()) {
                List<SpatioTextualObject> remainObjs = objects.stream().
                        filter(obj -> node.isInsideNodeRange(obj.getCoord())).
                        collect(Collectors.toList());
                node.buildViewSize(remainObjs);
            }

            Node[] children = node.getChildren();
            for (Node child : children)
                buildCandViewSizesHelper(child);
        }

        private void buildCandViewsHelper(Node node, List<Integer> viewId,
                                          List<QueryView> candViews,
                                          Map<Integer, Node> viewId2Node) {
            if (node == null)
                return;

            if (node.containQueries()) {
                Set<String> viewSet = new HashSet<>();
                for (WarehouseQuery query : node.getQueries()) {
                    String group = query.getGroupBy();
                    String aggrFunc = query.getMeasure();

                    if (!viewSet.contains(group + aggrFunc)) {
                        QueryView view = new QueryView(viewId.get(0), aggrFunc, group);
                        candViews.add(view);
                        viewId2Node.put(viewId.get(0), node);
                        viewId.set(0, viewId.get(0) + 1);

                        if (aggrFunc.equals(Constants.AGGR_COUNT)) {
                            Set<String> valSet = new HashSet<>();
                            for (SpatioTextualObject object : objects) {
                                String val = object.getGroupBy(group);
                                valSet.add(val);
                            }
                            view.setViewSize(valSet.size());
                        } else { // topk
                            int counterSize = Constants.TOPK_COUNTER_SIZE;
                            int viewSize = counterSize *
                                    (Constants.AVG_TERM_SIZE + Constants.TERM_FREQUENCY_SIZE);
                            view.setViewSize(viewSize);
                        }
                    }
                }
            }

            Node[] children = node.getChildren();
            for (Node child : children)
                buildCandViewsHelper(child, viewId, candViews, viewId2Node);
        }

        public int getNumCheckedObjects(WarehouseQuery query) {
            int[] numChecked = {0};
            getNumCheckedObjectsHelper(root, numChecked, query);

            return numChecked[0];
        }

        private void getNumCheckedObjectsHelper(Node node, int[] numChecked, WarehouseQuery query) {
            Tuple4<Double, Double, Double, Double> range = query.getRange().
                    orElse(new Tuple4<>(Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON));
            if (node == null || !node.isOverlappedNodeRange(range))
                return;

            if (node.isLeaf() && !node.isInsideNodeRange(range) && node.isOverlappedNodeRange(range))
                numChecked[0] += node.getNumObjectsPassed();

            for (Node child : node.getChildren())
                getNumCheckedObjectsHelper(child, numChecked, query);
        }

        private void adjustNodes(Node node) {
            if (node != null && node.isFull()) {
                buildChildren(node);
                List<SpatioTextualObject> objects = node.getObjects();
                for (SpatioTextualObject object : objects) {
                    Node child = findChild(node, object);
                    child.insertObject(object);
                }

                // node.clearObjects();
                for (Node child : node.getChildren())
                    adjustNodes(child);
            }
        }

        private void insertQuery(Node node, WarehouseQuery query) {
            if (node.isLeaf()) {
                node.insertQuery(query);
                return;
            }

            Tuple4<Double, Double, Double, Double> range =
                    query.getRange().orElse(
                            new Tuple4<>(Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON));
            Node child = findChild(node, query);
            insertQuery(child, query);
        }

        private Node findChild(Node node, SpatioTextualObject object) {
            Node[] children = node.getChildren();
            int i;
            for (i = 0; i < children.length; ++i) {
                if (children[i].isInsideNodeRange(object.getCoord()))
                    break;
            }

            return children[i];
        }

        private Node findChild(Node node, WarehouseQuery query) {
            Tuple4<Double, Double, Double, Double> range =
                    query.getRange().orElse(
                            new Tuple4<>(Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON));
            Node[] children = node.getChildren();
            int i;
            for (i = 0; i < children.length; ++i) {
                if (children[i].isInsideNodeRange(range))
                    break;
            }

            return children[i];
        }

        public void buildChildren(Node node) {
            int nodeId = node.getNodeId();
            int level = node.level;
            double latFrom = node.getLatFrom();
            double lonFrom = node.getLonFrom();
            double latTo = node.getLatTo();
            double lonTo = node.getLonTo();
            double centerLat = node.getCenterLat();
            double centerLon = node.getCenterLon();
            Node[] children = {null, null, null, null};
            children[0] = new Node(++nodeId, latFrom, lonFrom, centerLat, centerLon, level + 1);
            children[1] = new Node(++nodeId, centerLat, lonFrom, latTo, centerLon, level + 1);
            children[2] = new Node(++nodeId, latFrom, centerLon, centerLat, lonTo, level + 1);
            children[3] = new Node(++nodeId, centerLat, centerLon, latTo, lonTo, level + 1);
            node.setChildren(children);
        }
    }

    public static class QuadTreeBasedPartitioner2{
        private List<SpatioTextualObject> objects;
        private List<WarehouseQuery> queries;
        private PartitionerQuadTree quadTree;
        private int numPartitions;
        private final long MEM_CAPACITY = Constants.MEM_CAPACITY;

        private Map<Long, PartitionerQuadTree.Node> existedNodes = new HashMap<>();
        private Map<PartitionerQuadTree.Node, Double> node2Load = new HashMap<>();

        public void partition(List<SpatioTextualObject> objects,
                              List<WarehouseQuery> queries,
                              int numPartitions,
                              String fileName) {
            this.objects = objects;
            this.queries = queries;
            this.numPartitions = numPartitions;
            this.quadTree = new PartitionerQuadTree();
            loadObjects(objects);
            loadQueries(queries);
//            buildCandidateViewSizes();
            Map<Integer, Integer> queryId2NumChecked = getQueryId2NumCheckedObjects(queries);
            List<Tuple3<String, String, PartitionerQuadTree.Node>> materializedViews =
                    selectViewAlg();

            Map<PartitionerQuadTree.Node, Integer> node2Partition = new HashMap<>();
            double[] partitionLoad = new double[numPartitions];
            Arrays.fill(partitionLoad, 0.0);
            PartitionerQuadTree.Node root = partitionHelper(node2Partition, partitionLoad);

            try {
                writePartitions2File(root, node2Partition, fileName);
            } catch (IOException e) {
                System.err.println("Cannot write partitions to the file:" + fileName);
                System.exit(1);
            }
        }

        private void writePartitions2File(PartitionerQuadTree.Node root,
                                          Map<PartitionerQuadTree.Node, Integer> node2Partition,
                                          String fileName) throws IOException {
            File output = new File(fileName);
            BufferedWriter writer = new BufferedWriter(new FileWriter(output));

            ArrayList<String> strArray = new ArrayList<>();
            setFirstLine(root, strArray);
            StringBuilder strBuf = new StringBuilder();
            for (String str : strArray)
                strBuf.append(str + " ");
            strBuf.append("\n");

            for (Map.Entry<PartitionerQuadTree.Node, Integer> entry : node2Partition.entrySet()) {
                PartitionerQuadTree.Node node = entry.getKey();
                int partition = entry.getValue();
                long hashCode = node.getHashCode(node.level, node.latFrom, node.lonFrom);
                strBuf.append(hashCode + ":");
                strBuf.append(partition + " ");
            }
            strBuf.append("\n");

            strArray.clear();
            setRemainLines(root, strArray);
            for (String str : strArray)
                strBuf.append(str + "\n");

            writer.write(strBuf.toString());
            writer.close();
        }

        private void setFirstLine(PartitionerQuadTree.Node node, ArrayList<String> strArray) {
            long hashCode = PartitionerQuadTree.Node.getHashCode(node.level, node.latFrom, node.lonFrom);
            strArray.add(String.valueOf(hashCode));

            if (node.isLeaf())
                return;
            else {
                strArray.add("(");
                PartitionerQuadTree.Node[] children = node.children;
                setFirstLine(children[0], strArray);
                setFirstLine(children[1], strArray);
                setFirstLine(children[2], strArray);
                setFirstLine(children[3], strArray);
                strArray.add(")");
            }
        }

        private void setRemainLines(PartitionerQuadTree.Node node, ArrayList<String> strArray) {
            long hashCode = PartitionerQuadTree.Node.getHashCode(node.level, node.latFrom, node.lonFrom);
            String str = hashCode + " " + node.level + " " + node.latFrom + " " + node.lonFrom + " " + node.latTo + " " + node.lonTo;
            strArray.add(str);

            if (node.isLeaf())
                return;
            else {
                PartitionerQuadTree.Node[] children = node.children;
                setRemainLines(children[0], strArray);
                setRemainLines(children[1], strArray);
                setRemainLines(children[2], strArray);
                setRemainLines(children[3], strArray);
            }
        }

        private PartitionerQuadTree.Node partitionHelper(Map<PartitionerQuadTree.Node, Integer> node2Partition, double[] partitionLoad) {
            PartitionerQuadTree.Node root = new PartitionerQuadTree.Node(0, Constants.MIN_LAT, Constants.MIN_LON,
                    Constants.MAX_LAT, Constants.MAX_LON, 0);
            ArrayList<PartitionerQuadTree.Node> candidates = new ArrayList<>();
            ArrayList<PartitionerQuadTree.Node> nodesCannotSplit = new ArrayList<>();

            candidates.add(root);
            while (candidates.size() + nodesCannotSplit.size() < 4 * numPartitions) {
                PartitionerQuadTree.Node node2Split = null;
                if (candidates.size() + nodesCannotSplit.size() >= numPartitions) {
                    double[] minWorkerLoad = {0.0};
                    if (checkLoadBalance(root, candidates, nodesCannotSplit, minWorkerLoad)) {
                        System.out.println("Load balancing!");
                        break;
                    }

                    PartitionerQuadTree.Node maxLoadNode = null;
                    double maxLoad = -Double.MAX_VALUE;

                    for (PartitionerQuadTree.Node node: candidates) {
                        double load = computeLoadForQuadTreeNode(node);
                        if (load > maxLoad) {
                            maxLoad = load;
                            maxLoadNode = node;
                        }
                    }

                    if (maxLoad > balanceFactor * minWorkerLoad[0])
                        node2Split = maxLoadNode;
                }

                if (node2Split == null)
                    node2Split = pickNode2Split(candidates);

                int curLevel = node2Split.level;
                if (curLevel >= quadTreeMaxLevel) {
                    candidates.remove(node2Split);
                    nodesCannotSplit.add(node2Split);
                    continue;
                }

                List<PartitionerQuadTree.Node> children = getChildrenNodes(node2Split);
                node2Split.children[0] = children.get(0);
                node2Split.children[1] = children.get(1);
                node2Split.children[2] = children.get(2);
                node2Split.children[3] = children.get(3);

                candidates.remove(node2Split);
                for (PartitionerQuadTree.Node child : node2Split.children)
                    candidates.add(child);
            }

            List<PartitionerQuadTree.Node> nodes = new ArrayList<>(candidates);
            nodes.addAll(nodesCannotSplit);
            assignPartition2Node(root, nodes, node2Partition, partitionLoad);

            return root;
        }

        private PartitionerQuadTree.Node pickNode2Split(List<PartitionerQuadTree.Node> candidates) {
            if (candidates.size() == 1)
                return candidates.get(0);

            PartitionerQuadTree.Node node2Split = null;
            double optDeltaLoad = -Double.MAX_VALUE;

            for (PartitionerQuadTree.Node node : candidates) {
                double loadBefore = computeLoadForQuadTreeNode(node);
                List<PartitionerQuadTree.Node> children = getChildrenNodes(node);

                double loadAfter = 0.0;
                for (PartitionerQuadTree.Node child : children)
                    loadAfter += computeLoadForQuadTreeNode(child);

                double deltaLoad = loadBefore - loadAfter;
                if (deltaLoad > optDeltaLoad) {
                    optDeltaLoad = deltaLoad;
                    node2Split = node;
                }
            }

            return node2Split;
        }

        private List<PartitionerQuadTree.Node> getChildrenNodes(PartitionerQuadTree.Node node) {
            int curLevel = node.level;
            double middleLat = (node.latFrom + node.latTo) / 2.0;
            double middleLon = (node.lonFrom + node.lonTo) / 2.0;

            List<PartitionerQuadTree.Node> children = new ArrayList<>();
            long hashCode = PartitionerQuadTree.Node.getHashCode(curLevel + 1, node.latFrom, node.lonFrom);
            PartitionerQuadTree.Node child = existedNodes.get(hashCode);

            if (child != null) {
                children.add(child);

                hashCode = PartitionerQuadTree.Node.getHashCode(curLevel + 1, node.latFrom, middleLon);
                children.add(existedNodes.get(hashCode));

                hashCode = PartitionerQuadTree.Node.getHashCode(curLevel + 1, middleLat, node.lonFrom);
                children.add(existedNodes.get(hashCode));

                hashCode = PartitionerQuadTree.Node.getHashCode(curLevel + 1, middleLat, middleLon);
                children.add(existedNodes.get(hashCode));
            }
            else {
                int nodeId = node.getNodeId();
                child = new PartitionerQuadTree.Node(++nodeId, node.latFrom, node.lonFrom,
                        middleLat, middleLon, curLevel + 1);
                hashCode = Node.getHashCode(curLevel + 1, node.latFrom, node.lonFrom);
                existedNodes.put(hashCode, child);
                children.add(child);

                child = new PartitionerQuadTree.Node(++nodeId, node.latFrom, middleLon,
                        middleLat, node.lonFrom, curLevel + 1);
                hashCode = PartitionerQuadTree.Node.getHashCode(curLevel + 1, node.latFrom, middleLon);
                existedNodes.put(hashCode, child);
                children.add(child);

                child = new PartitionerQuadTree.Node(++nodeId, middleLat, node.lonFrom,
                        node.latTo, middleLon, curLevel + 1);
                hashCode = Node.getHashCode(curLevel + 1, middleLat, node.latFrom);
                existedNodes.put(hashCode, child);
                children.add(child);

                child = new PartitionerQuadTree.Node(++nodeId, middleLat, middleLon,
                        node.latTo, node.lonTo, curLevel + 1);
                hashCode = PartitionerQuadTree.Node.getHashCode(curLevel + 1, middleLat, middleLon);
                existedNodes.put(hashCode, child);
                children.add(child);

                for (SpatioTextualObject object : node.getObjects()) {
                    for (PartitionerQuadTree.Node ch : children) {
                        if (ch.isInsideNodeRange(object.getCoord()))
                            ch.insertObject(object);
                    }
                }

                for (WarehouseQuery query : node.getQueries()) {
                    for (PartitionerQuadTree.Node ch : children) {
                        if (ch.isOverlappedNodeRange(query.getRange().get()))
                            ch.insertQuery(query);
                    }
                }
            }

            return children;
        }

        private double computeLoadForQuadTreeNode(PartitionerQuadTree.Node node) {
            Double load = node2Load.get(node);
            if (load != null)
                return load;

            HashMap<String, NodeStat> group2Stats = node.computeStatistics();

            HashMap<String, Integer> group2Level = GridCubes.group2Level;
            Comparator<String> comparator =
                    Comparator.comparingInt(group2Level::get);
            List<String> groups = new ArrayList<>(group2Stats.keySet());
            Collections.sort(groups, comparator);

            double sumLoad = 0.0;
            int numObjects = node.getNumObjects();
            sumLoad += weightObject * numObjects;
            HashSet<String> materializedGroups = new HashSet<>();

            for (String group : groups) {
                NodeStat stat = group2Stats.get(group);
                int numQueries = node.getNumQueriesByGroup(group);
                String ancestorGroupMinCard = materializedGroups.stream()
                        .filter(e -> GridCubes.isAncestorOf(e, group))
                        .min((a, b) -> {
                            int cardOfA = group2Stats.get(a).getCardinality();
                            int cardOfB = group2Stats.get(b).getCardinality();
                            return Integer.compare(cardOfA, cardOfB);
                        }).orElse(null);

                if (ancestorGroupMinCard != null) {
                    double loadAncestor = weightQuery *
                            getQueryLoad(numQueries, group2Stats.get(ancestorGroupMinCard).getCardinality());
                    double loadCurrentCube = weightQuery * getQueryLoad(numQueries, stat.getCardinality())
                            + weightUpdateCube * numObjects;
                    if (loadCurrentCube < loadAncestor) {
                        materializedGroups.add(group);
                        sumLoad += loadCurrentCube;
                    } else
                        sumLoad += loadAncestor;
                } else {
                    double queryLoad = getQueryLoad(numQueries, stat.getCardinality());
                    double loadCube = weightQuery * queryLoad + weightUpdateCube * numObjects;
                    double loadRaw = weightQuery * getQueryLoad(numQueries, numObjects);
                    if (loadCube < loadRaw) {
                        materializedGroups.add(group);
                        sumLoad += loadCube;
                    } else
                        sumLoad += loadRaw;
                }
            }

            node2Load.put(node, sumLoad);
            return sumLoad;
        }

        private boolean checkLoadBalance(PartitionerQuadTree.Node root, List<PartitionerQuadTree.Node> candidates,
                                         List<PartitionerQuadTree.Node> nodesCannotSplit,
                                         double[] minWorkerLoad) {
            double sumLoad = 0.0;
            for (PartitionerQuadTree.Node node : candidates)
                sumLoad += computeLoadForQuadTreeNode(node);
            for (PartitionerQuadTree.Node node : nodesCannotSplit)
                sumLoad += computeLoadForQuadTreeNode(node);

            double partitionLimit = sumLoad / numPartitions;
            int[] partitionNo = {0};
            double[] partitionLoad = new double[numPartitions];
            for (int i = 0; i < numPartitions; ++i)
                partitionLoad[i] = 0.0;

            HashMap<PartitionerQuadTree.Node, Integer> node2Partition = new HashMap<>();
            partitionNodes(root, partitionLimit, node2Partition, partitionNo, partitionLoad);

            double maxLoad = Arrays.stream(partitionLoad).max().getAsDouble();
            double minLoad = Arrays.stream(partitionLoad).min().getAsDouble();
            minWorkerLoad[0] = minLoad;
            return (maxLoad / minLoad) <= balanceFactor;
        }

        private void assignPartition2Node(PartitionerQuadTree.Node root, List<PartitionerQuadTree.Node> nodes,
                                          Map<PartitionerQuadTree.Node, Integer> node2Partition,
                                          double[] partitionLoad) {
            double sumLoad = 0.0;
            for (PartitionerQuadTree.Node node : nodes)
                sumLoad += computeLoadForQuadTreeNode(node);

            double partitionLimit = sumLoad / numPartitions;
            int[] partitionNo = {0};
            for (int i = 0; i < numPartitions; ++i)
                partitionLoad[i] = 0.0;

            partitionNodes(root, partitionLimit, node2Partition, partitionNo, partitionLoad);
        }

        public void partitionNodes(PartitionerQuadTree.Node node, double partitionLimit,
                                   Map<PartitionerQuadTree.Node, Integer> node2Partition,
                                   int[] partitionNo, double[] partitionLoad) {
            if (node.isLeaf()) {
                node2Partition.put(node, partitionNo[0]);
                double nodeLoad = computeLoadForQuadTreeNode(node);
                partitionLoad[partitionNo[0]] += nodeLoad;
                if (partitionLoad[partitionNo[0]] > partitionLimit)
                    ++partitionNo[0];

                return;
            }

            for (PartitionerQuadTree.Node child : node.children)
                partitionNodes(child, partitionLimit, node2Partition, partitionNo, partitionLoad);
        }

        private List<Tuple3<String, String, PartitionerQuadTree.Node>> selectViewAlg() {
            Map<Integer, PartitionerQuadTree.Node> viewId2Node = new HashMap<>();
            List<QueryView> candViews = quadTree.buildCandidateViews(viewId2Node);
            Map<Integer, Integer> queryId2NumChecked = getQueryId2NumCheckedObjects(queries);

            Map<Integer, List<Integer>> view2Ancestors = new HashMap<>();
            for (QueryView view : candViews) {
                for (QueryView ancestor : candViews) {
                    PartitionerQuadTree.Node viewNode = viewId2Node.get(view.getId());
                    PartitionerQuadTree.Node ancestorNode = viewId2Node.get(ancestor.getId());
                    String viewAggrFunc = view.getAggrFunc();
                    String ancestorAggrFunc = ancestor.getAggrFunc();
                    if (viewNode.getNodeId() == ancestorNode.getNodeId() &&
                            viewAggrFunc.equals(ancestorAggrFunc)) {
                        if (GridCubes.isAncestorOf(ancestor.getGroupBy(), view.getGroupBy())) {
                            List<Integer> ancestors = view2Ancestors.get(view.getId());
                            if (ancestors == null) {
                                ancestors = new ArrayList<>();
                                view2Ancestors.put(view.getId(), ancestors);
                            }
                            ancestors.add(ancestor.getId());
                        }
                    }
                }
            }

            Set<Integer> viewsCanRemove = new HashSet<>();
            for (QueryView view : candViews) {
                int viewId = view.getId();
                List<Integer> ancestorIds = view2Ancestors.get(viewId);
                Optional<Integer> xx = ancestorIds.stream().filter(e -> {
                    QueryView ancestor = candViews.get(e);
                    List<Double> costs = getCosts(ancestor, view, viewId2Node);
                    return costs.get(0) >= costs.get(1);
                }).findAny();
                if (xx.isPresent())
                    viewsCanRemove.add(viewId);
            }

            long sumMemoryCost = 0L;
            for (QueryView view : candViews) {
                if (viewsCanRemove.contains(view.getId()))
                    continue;
                sumMemoryCost += view.getViewSize();
            }

            if (sumMemoryCost <= MEM_CAPACITY) {
                List<Tuple3<String, String, PartitionerQuadTree.Node>> materializedViews = new ArrayList<>();
                for (QueryView view : candViews) {
                    if (viewsCanRemove.contains(view.getId()))
                        continue;
                    String group = view.getGroupBy();
                    String aggrFunc = view.getAggrFunc();
                    PartitionerQuadTree.Node node = viewId2Node.get(view.getId());
                    Tuple3<String, String, PartitionerQuadTree.Node> tuple3 =
                            new Tuple3<>(group, aggrFunc, node);
                    materializedViews.add(tuple3);
                }

                return materializedViews;
            }

            int memUsed = 0;
            List<QueryView> selectedViews = new ArrayList<>();
            Set<Integer> selectedIds = new HashSet<>();
            while (memUsed <= MEM_CAPACITY) {
                QueryView nextView = pickMaterializedView(candViews, selectedViews, selectedIds,
                        viewId2Node, view2Ancestors, viewsCanRemove);
                if (nextView == null)
                    break;
                memUsed += nextView.getViewSize();
                selectedViews.add(nextView);
                selectedIds.add(nextView.getId());
            }

            List<Tuple3<String, String, PartitionerQuadTree.Node>> materializedViews = new ArrayList<>();
            for (QueryView view : selectedViews) {
                String group = view.getGroupBy();
                String aggrFunc = view.getAggrFunc();
                PartitionerQuadTree.Node node = viewId2Node.get(view.getId());
                Tuple3<String, String, PartitionerQuadTree.Node> tuple3 =
                        new Tuple3<>(group, aggrFunc, node);
                materializedViews.add(tuple3);
            }

            return materializedViews;
        }

        private QueryView pickMaterializedView(List<QueryView> candidateViews,
                                               List<QueryView> selectedViews,
                                               Set<Integer> selectedIds,
                                               Map<Integer, PartitionerQuadTree.Node> viewId2Node,
                                               Map<Integer, List<Integer>> view2Ancestors,
                                               Set<Integer> viewsCanRemove) {
            QueryView optView = null;
            double optBenefitPerSpace = -1;
            for (QueryView candView : candidateViews) {
                int candId = candView.getId();
                if (viewsCanRemove.contains(candId) || selectedIds.contains(candId))
                    continue;

                PartitionerQuadTree.Node node = viewId2Node.get(candId);
                double sumCostsBefore = Integer.MAX_VALUE;
                for (QueryView selectedView : selectedViews) {
                    PartitionerQuadTree.Node selectedNode = viewId2Node.get(selectedView.getId());
                    if (selectedNode.getNodeId() != node.getNodeId())
                        continue;
                    String group = selectedView.getGroupBy();
                    List<String> groups = Arrays.stream(Constants.GROUPS).filter(e -> {
                        return !selectedNode.getGroup2NumQueries().isEmpty() &&
                                QuadTreeCubes.isAncestorOf(group, e);}).
                            collect(Collectors.toList());
                    groups.add(group);

                    double sumCosts = 0.0;
                    for (String g : groups)
                        sumCosts += selectedView.getViewSize() * node.getGroup2NumQueries().get(g);
                    sumCostsBefore = Math.min(sumCostsBefore, sumCosts);
                }

                double sumCostsAfter = 0.0;
                String group = candView.getGroupBy();
                List<String> groups = selectedViews.stream().filter(v -> {
                    if (viewId2Node.get(v.getId()).getNodeId() != node.getNodeId())
                        return false;
                    return QuadTreeCubes.isAncestorOf(group, v.getGroupBy());
                }).map(v -> v.getGroupBy()).collect(Collectors.toList());
                groups.add(group);

                for (String g : groups)
                    sumCostsAfter += candView.getViewSize() * node.getGroup2NumQueries().get(g);

                double queryBenefit = Constants.WEIGHT_QUERY *
                        Math.max(0, sumCostsBefore - sumCostsAfter);
                if (Constants.WEIGHT_MAINTAIN * node.getNumObjectsPassed() >= queryBenefit)
                    viewsCanRemove.add(candId);
                else {
                    double benefit = queryBenefit -
                            Constants.WEIGHT_MAINTAIN * node.getNumObjectsPassed();
                    double benefitPerSpace = benefit / candView.getViewSize();
                    if (benefitPerSpace > optBenefitPerSpace) {
                        optBenefitPerSpace = benefitPerSpace;
                        optView = candView;
                    }
                }
            }

            return optView;
        }

        private Map<Integer, Integer> getQueryId2NumCheckedObjects(List<WarehouseQuery> queries) {
            Map<Integer, Integer> queryId2NumChecked = new HashMap<>();
            for (WarehouseQuery query : queries) {
                int numChecked = quadTree.getNumCheckedObjects(query);
                queryId2NumChecked.put(query.getQueryId(), numChecked);
            }

            return queryId2NumChecked;
        }

        private List<Double> getCosts(QueryView ancestor, QueryView child,
                                      Map<Integer, PartitionerQuadTree.Node> viewId2Node) {
            double weightMaintain = Constants.WEIGHT_MAINTAIN, weightQuery = Constants.WEIGHT_QUERY;
            int childViewId = child.getId();
            PartitionerQuadTree.Node node = viewId2Node.get(childViewId);
            Map<String, Integer> view2Size = node.getView2Size();

            int numObjects = node.getNumObjectsPassed();
            int numChildQueries = node.getGroup2NumQueries().get(child.getGroupBy());
            int childViewSize = view2Size.get(child.getGroupBy());
            int ancestorViewSize = view2Size.get(ancestor.getGroupBy());

            double costUsingAncestor = numObjects * ancestorViewSize;
            double maintainCost = numObjects;
            double queryCostUsingChild = childViewSize * node.getGroup2NumQueries().get(numChildQueries);
            double costUsingChild = maintainCost * weightMaintain + queryCostUsingChild * weightQuery;

            return Arrays.asList(costUsingAncestor, costUsingChild);
        }

        private void loadObjects(List<SpatioTextualObject> objects) {
            quadTree.buildQuadTree(objects);
        }

        private void loadQueries(List<WarehouseQuery> queries) {
            quadTree.insertQueries(queries);
        }

        private void buildCandidateViewSizes() {
            quadTree.buildCandidateViewSizes();
        }
    }

    public static void runQuadTreeBasedPartition2(List<SpatioTextualObject> objects,
                                                 List<WarehouseQuery> queries,
                                                 int numPartitions) {
        QuadTreeBasedPartitioner2 partitioner = new QuadTreeBasedPartitioner2();
        String fileName = "resources/partitions_quadTree_" + numPartitions + ".txt";
        partitioner.partition(objects, queries, numPartitions, fileName);
    }

    public static void main(String[] args) throws Exception {
//        String dataPath = args[0];
//        int numObjects = Integer.valueOf(args[1]);
//        int numQueries = Integer.valueOf(args[2]);
//        double rangeRatio = Double.valueOf(args[3]);

        String dataPath = "resources/sql_tweets.txt";
        int numObjects = 100000;
        int numQueries = 1000;
        double rangeRatio = 0.1;

        System.out.println("Configuration:");
        System.out.println("\tdataPath = " + dataPath);
        System.out.println("\tnumObjects = " + numObjects);
        System.out.println("\tnumQueries = " + numQueries);
        System.out.println("\trangeRatio = " + rangeRatio);

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
        System.out.println("Running the partitioning algorithm...");
        runGridBasedPartition(objects, log, 8);
        runQuadTreeBasedPartition(objects, queries, 8);
        runQuadTreeBasedPartition2(objects, queries, 8);
        t2 = System.currentTimeMillis();
        System.out.printf("Done! It takes %f seconds\n", (t2-t1)/1000.0);
    }
}
