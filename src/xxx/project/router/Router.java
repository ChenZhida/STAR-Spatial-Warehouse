package xxx.project.router;

import org.apache.storm.tuple.Tuple;

import java.util.List;

public interface Router {
    List<Integer> route(Tuple tuple);
}
