package xxx.project.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import xxx.project.router.Router;
import xxx.project.util.Constants;

import java.util.List;
import java.util.Map;

public class RouterBolt extends BaseRichBolt {
    OutputCollector _collector;

    private Router router = null;
    private List<Integer> downstreamTasks = null;

    private int numTuplesReceived = 0;
    private int[] arrayNumTuplesSent = null;

    public RouterBolt(Router router) {
        this.router = router;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        downstreamTasks = context.getComponentTasks(Constants.BOLT_WORKER);
        arrayNumTuplesSent = new int[downstreamTasks.size()];
        for (int i = 0; i < downstreamTasks.size(); i++) {
            arrayNumTuplesSent[i] = 0;
        }
    }

    public void execute(Tuple tuple) {
        try {
            List<Integer> indexArray = router.route(tuple);
            for (Integer i : indexArray) {
                _collector.emitDirect(downstreamTasks.get(i), tuple, tuple.getValues());
                ++arrayNumTuplesSent[i];
            }
            _collector.ack(tuple);

            ++numTuplesReceived;
        } catch (Exception e) {
            e.printStackTrace();
        }
        /*
        if (numTuplesReceived % 1000 == 0) {
            StringBuilder str = new StringBuilder();
            for (int i = 0; i < arrayNumTuplesSent.length; ++i)
                str.append(String.format("Sent %d tuples to worker %d\n", arrayNumTuplesSent[i], i));
            System.out.println(str);
        }
         */
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(true, new Fields(ParserBolt.PARSED_FIEDLS));
    }
}
