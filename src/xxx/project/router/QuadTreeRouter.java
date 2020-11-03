package xxx.project.router;

import org.apache.storm.tuple.Tuple;
import xxx.project.bolt.ParserBolt;
import xxx.project.router.index.QuadTree;
import xxx.project.util.Constants;
import xxx.project.util.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class QuadTreeRouter implements Router, Serializable {

    private static final long serialVersionUID = 1L;

    private final double minLat = Constants.MIN_LAT;
    private final double minLon = Constants.MIN_LON;
    private final double maxLat = Constants.MAX_LAT;
    private final double maxLon = Constants.MAX_LON;

    private QuadTree quadTree = null;
    private int numTasks;

    public QuadTreeRouter(int parallelism) {
        this.numTasks = parallelism;
    }

    public void bulkLoad(ArrayList<Tuple2<Double, Double>> points) {
        int partitionLimit = points.size() / numTasks;
        int nodeLimit = partitionLimit / 8;

        quadTree = new QuadTree(minLat, minLon, maxLat, maxLon, nodeLimit, points);
        quadTree.bulkLoad();
        quadTree.partitionNodes(numTasks, partitionLimit);

        System.out.println(String.format("[minLat, minLon, maxLat, maxLon]: [%f, %f, %f, %f]",
                minLat, minLon, maxLat, maxLon));
        System.out.println("Partition limit: " + partitionLimit);
        quadTree.printPartitionInfo();

        quadTree.removePoints();
    }

    public List<Integer> route(Tuple tuple) {
        String latKey = ParserBolt.PARSED_FIEDLS[1];
        String lonKey = ParserBolt.PARSED_FIEDLS[2];

        double lat = tuple.getDoubleByField(latKey);
        double lon = tuple.getDoubleByField(lonKey);
        Tuple2<Double, Double> coord = new Tuple2<>(lat, lon);

        Optional<Integer> index = quadTree.getPartition(coord);
        return index.isPresent() ? Arrays.asList(index.get()) : new ArrayList<>();
    }
}
