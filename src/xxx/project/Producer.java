package xxx.project;

import xxx.project.producer.ObjectProducer;
import xxx.project.producer.QueryProducer;
import xxx.project.util.Constants;
import xxx.project.util.SpatioTextualObject;
import xxx.project.util.UtilFunctions;
import xxx.project.util.WarehouseQuery;

import java.util.List;
import java.util.Properties;

public class Producer {
    public static void runLocalProducer(String filePath) throws Exception {
        String broker = "localhost:9092";
        Properties props =
                ObjectProducer.configProperties(broker, filePath);
        (new ObjectProducer(props)).produceMessages(10000);
        Thread.sleep(10000);

        List<SpatioTextualObject> objects = UtilFunctions.createObjects(filePath, 10000);
        List<WarehouseQuery> queries = UtilFunctions.createQueries(objects, 10,
                Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON, 0.01);
        props = QueryProducer.configProperties(broker);
        (new QueryProducer(props)).produceMessages(queries);
    }

    public static void main(String[] args) throws Exception {
//        String filePath = "resources/sql_tweets.txt";
//        runLocalProducer(filePath);
        (new ObjectProducer()).produceMessages(100000);
    }
}
