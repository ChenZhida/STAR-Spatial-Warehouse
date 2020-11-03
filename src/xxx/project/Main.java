package xxx.project;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;

import xxx.project.bolt.*;
import xxx.project.router.*;
import xxx.project.util.*;
import xxx.project.worker.index.Index;
import xxx.project.worker.index.grid.GridCubes;
import xxx.project.worker.index.naive.NaiveIndex;
import xxx.project.worker.index.quadtree.QuadTreeCache;

public class Main {
    public static Properties configProperties(String confFilePath) throws IOException {
        File configFile = new File(confFilePath);
        BufferedReader reader = new BufferedReader(new FileReader(configFile));
        Properties props = new Properties();

        while (true) {
            String line = reader.readLine();
            if (line == null)
                break;

            String key = line.split("=")[0];
            String value = line.split("=")[1];
            props.put(key, value);
        }

        reader.close();
        return props;
    }

    public static ArrayList<Tuple2<Double, Double>> getSamplePoints(String fileName) {
        Reader reader;
        ArrayList<Tuple2<Double, Double>> points = new ArrayList<>();

        try {
            reader = new InputStreamReader(new FileInputStream(fileName));
            char[] tempChars = new char[1000];
            int index = 0, charRead = 0;

            while ((charRead = reader.read()) != -1) {
                if ((char)charRead != '\n')
                    tempChars[index++] = (char)charRead;
                else {
                    try {
                        String line = new String(tempChars, 0, index);
                        String[] attrs = line.split(String.valueOf(Constants.SEPARATOR));
                        double lat = Double.valueOf(attrs[5]);
                        double lon = Double.valueOf(attrs[6]);
                        Tuple2<Double, Double> point = new Tuple2<>(lat, lon);
                        points.add(point);
                    } catch (NumberFormatException ex) {}

                    index = 0;
                }
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return points;
    }

    public static Router getRouter(Properties props) {
        String routerType = (String)props.get(Constants.ROUTER_TYPE_CONFIG);
        String parallelismWorker = (String)props.get(Constants.PARALLELISM_BOLT_WORKER_CONFIG);
        Router router = null;

        if (routerType.equals(Constants.RANDOM_ROUTER))
            router = new RandomRouter(Integer.valueOf(parallelismWorker));
        else if (routerType.equals(Constants.QUADTREE_ROUTER)) {
            router = new QuadTreeRouter(Integer.valueOf(parallelismWorker));
            ArrayList<Tuple2<Double, Double>> points = getSamplePoints((String)props.get(Constants.SAMPLE_PATH_CONFIG));
            System.out.println("Number of tweets in the sample: " + points.size());
            ((QuadTreeRouter)router).bulkLoad(points);
        } else if (routerType.equals(Constants.RTREE_ROUTER)) {
            router = new RTreeRouter(Integer.valueOf(parallelismWorker));
            ArrayList<Tuple2<Double, Double>> points = getSamplePoints((String)props.get(Constants.SAMPLE_PATH_CONFIG));
            System.out.println("Number of tweets in the sample: " + points.size());
            ((RTreeRouter)router).bulkLoad(points);
        } else if (routerType.equals(Constants.LOAD_BASED_QUADTREE_ROUTER)) {
            router = new LoadBasedQuadTreeRouter(Integer.valueOf(parallelismWorker));
            String partitionFile = (String)props.get(Constants.PARTITION_PATH_CONFIG);
            System.out.println("Reading the partition file: " + partitionFile);
            ((LoadBasedQuadTreeRouter)router).readPartitionsFromFile(partitionFile);
        }

        return router;
    }

    public static Index getWorkerIndex(Properties props) throws IOException, ClassNotFoundException {
        String workerIndex = (String)props.get(Constants.WORKER_INDEX_TYPE_CONFIG);
        String cubeFilePath = (String)props.get(Constants.CUBE_FILE_PATH_CONFIG);
        Index index = null;

        if (workerIndex.equals(Constants.NAIVE_WORKER)) {
            index = new NaiveIndex();
        } else if (workerIndex.equals(Constants.QUAD_TREE_WORKER)) {
            index = new QuadTreeCache();
            index.readMaterializedCubesFromFile(cubeFilePath);
        } else if (workerIndex.equals(Constants.GRID_WORKER)) {
            index = new GridCubes();
            index.readMaterializedCubesFromFile(cubeFilePath);
        } else {
            index = new QuadTreeCache();
            index.readMaterializedCubesFromFile(cubeFilePath);
        }

        return index;
    }

    public static KafkaSpout getObjectSpout(String server) {
//        SpoutConfig spoutConfig = new SpoutConfig(hosts,
//                Constants.KAFKA_OBJECT_TOPIC, "/" + Constants.KAFKA_OBJECT_TOPIC, UUID.randomUUID().toString());
//        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
//        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
//        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        KafkaSpout objectSpout = new KafkaSpout(spoutConfig);
//        KafkaSpout<> spoutConfig = new KafkaSpout<>(KafkaSpoutConfig.builder(
//                "127.0.0.1" + port,
//                Constants.KAFKA_OBJECT_TOPIC).build());

//        return new KafkaSpout<>(KafkaSpoutConfig.builder(server, Constants.KAFKA_OBJECT_TOPIC).build());

        KafkaSpoutConfig.Builder<String, Object> spoutBuilder =
                new KafkaSpoutConfig.Builder<>(server, Constants.KAFKA_OBJECT_TOPIC);
        spoutBuilder.setProp("group.id", "object-spout");
        spoutBuilder.setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        spoutBuilder.setProp("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        spoutBuilder.setProp("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaSpoutConfig<String, Object> spoutConfig = spoutBuilder.build();

        return new KafkaSpout<>(spoutConfig);
    }

    public static KafkaSpout getQuerySpout(String server) {
//        SpoutConfig spoutConfig = new SpoutConfig(hosts,
//                Constants.KAFKA_QUERY_TOPIC, "/" + Constants.KAFKA_QUERY_TOPIC, UUID.randomUUID().toString());
//        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
//        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        KafkaSpout querySpout = new KafkaSpout(spoutConfig);
//
//        return querySpout;

        KafkaSpoutConfig.Builder<String, Object> spoutBuilder =
                new KafkaSpoutConfig.Builder<>(server, Constants.KAFKA_QUERY_TOPIC);
        spoutBuilder.setProp("group.id", "query-spout");
        spoutBuilder.setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        spoutBuilder.setProp("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        spoutBuilder.setProp("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaSpoutConfig<String, Object> spoutConfig = spoutBuilder.build();

        return new KafkaSpout<>(spoutConfig);

//        return new KafkaSpout<>(KafkaSpoutConfig.builder(
//                "127.0.0.1" + port, Constants.KAFKA_QUERY_TOPIC).build());
    }

    public static KafkaSpout getContinuousQueryRequestSpout(String server) {
        KafkaSpoutConfig.Builder<String, Object> spoutBuilder =
                new KafkaSpoutConfig.Builder<>(server, Constants.KAFKA_CONTINUOUS_QUERY_TOPIC);
        spoutBuilder.setProp("group.id", "continuous-query-request-spout");
        spoutBuilder.setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        spoutBuilder.setProp("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        spoutBuilder.setProp("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaSpoutConfig<String, Object> spoutConfig = spoutBuilder.build();

        return new KafkaSpout<>(spoutConfig);
    }

    public static void main(String[] args) throws Exception {
//        SysOutOverSLF4J.registerLoggingSystem("org.apache.logging.slj4j");
//        SysOutOverSLF4J.sendSystemOutAndErrToSLF4J();
        KafkaSpout objectSpout = getObjectSpout(Constants.KAFKA_BROKER);
        KafkaSpout querySpout = getQuerySpout(Constants.KAFKA_BROKER);
        KafkaSpout continuousRequestSpout = getContinuousQueryRequestSpout(Constants.KAFKA_BROKER);

        TopologyBuilder builder = new TopologyBuilder();
        int parallelismObjectSpout = 1;
        int parallelismQuerySpout = 1;
        int parallelismContinuousRequestSpout = 1;
        int parallelismParser = 1;
        int parallelismRouter = 4;
        int parallelismWorker = 8;
        int parallelismMerger = 1;
        int parallelismQueryAnalyzer = 1;
        int parallelismContinuousWorker = 8;
        Properties props = new Properties();
        props.put(Constants.ROUTER_TYPE_CONFIG, Constants.LOAD_BASED_QUADTREE_ROUTER);
        props.put(Constants.WORKER_INDEX_TYPE_CONFIG, Constants.QUAD_TREE_WORKER);
        props.put(Constants.PARALLELISM_BOLT_WORKER_CONFIG, String.valueOf(parallelismWorker));
        props.put(Config.TOPOLOGY_WORKERS, String.valueOf(parallelismWorker));
        props.put(Constants.SAMPLE_PATH_CONFIG, "resources/sql_tweets.txt");
        props.put(Constants.PARTITION_PATH_CONFIG, "resources/partitions_quadTree_8.txt");
        props.put(Constants.PARALLELISM_BOLT_CONTINUOUS_WORKER_CONFIG, parallelismContinuousWorker);

        builder.setSpout(Constants.OBJECT_SPOUT, objectSpout, parallelismObjectSpout);
        builder.setSpout(Constants.QUERY_SPOUT, querySpout, parallelismQuerySpout);
        builder.setSpout(Constants.CONTINUOUS_QUERY_REQUEST_SPOUT, continuousRequestSpout,
                parallelismContinuousRequestSpout);

        builder.setBolt(Constants.BOLT_PARSER,
                new ParserBolt(),
                parallelismParser)
                .shuffleGrouping(Constants.OBJECT_SPOUT);
        builder.setBolt(Constants.BOLT_ROUTER,
                new RouterBolt(getRouter(props)),
                parallelismRouter)
                .shuffleGrouping(Constants.BOLT_PARSER);
        builder.setBolt(Constants.BOLT_QUERY_ANALYZER,
                new QueryAnalyzerBolt(),
                parallelismQueryAnalyzer)
                .shuffleGrouping(Constants.QUERY_SPOUT);
        builder.setBolt(Constants.BOLT_WORKER,
                new WorkerBolt(getWorkerIndex(props)),
                parallelismWorker)
                .directGrouping(Constants.BOLT_ROUTER)
                .allGrouping(Constants.BOLT_QUERY_ANALYZER);
        builder.setBolt(Constants.BOLT_MERGER,
                new MergerBolt(),
                parallelismMerger)
                .shuffleGrouping(Constants.BOLT_WORKER)
                .shuffleGrouping(Constants.BOLT_QUERY_ANALYZER);
        builder.setBolt(Constants.CONTINUOUS_QUERY_WORKER_BOLT,
                new ContinuousWorkerBolt(),
                parallelismContinuousWorker)
                .shuffleGrouping(Constants.BOLT_QUERY_ANALYZER)
                .allGrouping(Constants.BOLT_PARSER)
                .allGrouping(Constants.CONTINUOUS_QUERY_REQUEST_SPOUT);

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(4);
        conf.setNumAckers(0);

        // remote cluster
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());

        // local cluster
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("star-warehouse", conf, builder.createTopology());
//        Thread.sleep(30000);
//        System.out.println("The producer starts to produce messages.");
//        (new ObjectProducer()).produceMessages(100000);
//        Thread.sleep(120000);
//        cluster.killTopology("star-warehouse");
//        System.exit(0);
    }
}
