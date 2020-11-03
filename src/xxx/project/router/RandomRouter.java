package xxx.project.router;

import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class RandomRouter implements Router, Serializable {

    private static final long serialVersionUID = 1L;

    private int numTasks;

    public RandomRouter(int parallelism) {
        this.numTasks = parallelism;
    }

    public List<Integer> route(Tuple tuple) {
        return Arrays.asList(tuple.hashCode() % numTasks);
    }
}
