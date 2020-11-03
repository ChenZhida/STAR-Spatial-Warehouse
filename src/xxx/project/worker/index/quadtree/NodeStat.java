package xxx.project.worker.index.quadtree;

import java.util.HashSet;

class NodeStat {
    public int numQueries = 0;
    public int numObjects = 0;
    public HashSet<String> distinctValues = new HashSet<>();

    public int getCardinality() {
        return distinctValues.size();
    }
}

