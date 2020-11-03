package xxx.project.router;

import org.apache.storm.tuple.Tuple;
import xxx.project.bolt.ParserBolt;
import xxx.project.router.index.LoadBasedQuadTree;
import xxx.project.util.Constants;
import xxx.project.util.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class LoadBasedQuadTreeRouter implements Router, Serializable {

    private static final long serialVersionUID = 1L;

    private final double minLat = Constants.MIN_LAT;
    private final double minLon = Constants.MIN_LON;
    private final double maxLat = Constants.MAX_LAT;
    private final double maxLon = Constants.MAX_LON;

    private LoadBasedQuadTree quadTree = null;
    private int numTasks;

    public LoadBasedQuadTreeRouter(int parallelism) {
        this.numTasks = parallelism;
    }

    public void readPartitionsFromFile(String fileName) {
        quadTree = new LoadBasedQuadTree(minLat, minLon, maxLat, maxLon);
        try {
            quadTree.readPartitionsFromFile(fileName);
        } catch (IOException e) {
            System.err.println("Cannot open the partition file: " + fileName);
            System.exit(1);
        }
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
