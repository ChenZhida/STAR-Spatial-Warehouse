package xxx.project.router;

import org.apache.storm.tuple.Tuple;
import xxx.project.bolt.ParserBolt;
import xxx.project.router.index.RTree;
import xxx.project.util.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class RTreeRouter implements Router, Serializable {
    private static final long serialVersionUID = 1L;

    private RTree rTree = null;
    private int numTasks;

    public RTreeRouter(int parallelism) {
        this.numTasks = parallelism;
    }

    public void bulkLoad(List<Tuple2<Double, Double>> points) {
        int numLeafNodes = numTasks * 8;
        rTree = new RTree();
        rTree.bulkLoad(points, numLeafNodes);
        rTree.partitionNodes(numTasks);
        rTree.printPartitionInfo();
    }

    public List<Integer> route(Tuple tuple) {
        String latKey = ParserBolt.PARSED_FIEDLS[1];
        String lonKey = ParserBolt.PARSED_FIEDLS[2];

        double lat = tuple.getDoubleByField(latKey);
        double lon = tuple.getDoubleByField(lonKey);
        Tuple2<Double, Double> coord = new Tuple2<>(lat, lon);

        Optional<Integer> index = rTree.getPartition(coord);
        return index.isPresent() ? Arrays.asList(index.get()) : new ArrayList<>();
    }
}
