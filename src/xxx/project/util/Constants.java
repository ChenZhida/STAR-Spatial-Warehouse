package xxx.project.util;

public class Constants {
    public static final char SEPARATOR = '\t';
    public static final char TEXT_SEPARATOR = ',';
    public static final int MSG_LEN_LIMIT = 102400;

    public static final double MIN_LAT = -90.0;
    public static final double MIN_LON = -180.0;
    public static final double MAX_LAT = 90.0;
    public static final double MAX_LON = 180.0;

    public static int NUM_HOURS_DATA_KEPT = 10;
    public static long MEM_CAPACITY = 120000L;
    public static int OBJECT_CACHE_CAPACITY = 1000000;
    public static double WEIGHT_MAINTAIN = 1.0;
    public static double WEIGHT_QUERY = 1.0;
    public static final int QUAD_TREE_MAX_LEVEL = 20;
    public static final int QUAD_TREE_NODE_SIZE = 1000;
    public static final int GRID_NUM_LAT_CELLS = 1000;
    public static final int GRID_NUM_LON_CELLS = 1000;

    public static final String AGGR_COUNT = "count";
    public static final String AGGR_TOPK = "topk";
    public static final int TOPK_COUNTER_SIZE = 10;
    public static final int AVG_TERM_SIZE = 3;
    public static final int TERM_FREQUENCY_SIZE = 1;

    public static final String ZOOKEEPER_HOST = "localhost:2181";

    public static final String KAFKA_BROKER = "localhost:9092";
//    public static final String KAFKA_BROKER = "172.31.11.81:9092";
    public static final String KAFKA_OBJECT_TOPIC = "object";
    public static final String KAFKA_QUERY_TOPIC = "query";
    public static final String KAFKA_QUERY_RESULT_TOPIC = "query-result";
    public static final String KAFKA_CONTINUOUS_QUERY_TOPIC = "continuous-query";
    public static final String KAFKA_CONTINUOUS_QUERY_RESULT_TOPIC = "continuous-query-result";

    public static final String OBJECT_SPOUT = "objectSpout";
    public static final String QUERY_SPOUT = "querySpout";
    public static final String BOLT_PARSER = "parser";
    public static final String BOLT_QUERY_ANALYZER = "queryAnalyzer";
    public static final String BOLT_ROUTER = "router";
    public static final String BOLT_WORKER = "worker";
    public static final String BOLT_MERGER = "merger";
    public static final String CONTINUOUS_QUERY_WORKER_BOLT = "continuousQueryWorker";
    public static final String CONTINUOUS_QUERY_REQUEST_SPOUT = "continuousQueryRequestSpout";

    public static final String ROUTER_TYPE_CONFIG = "router.type";
    public static final String RANDOM_ROUTER = "random";
    public static final String QUADTREE_ROUTER = "quadtree";
    public static final String RTREE_ROUTER = "r-tree";
    public static final String LOAD_BASED_QUADTREE_ROUTER = "load.quadtree";

    public static final String WORKER_INDEX_TYPE_CONFIG = "worker.index.type";
    public static final String NAIVE_WORKER = "naive-index";
    public static final String QUAD_TREE_WORKER = "quadtree-index";
    public static final String GRID_WORKER = "grid-index";

    public static final String SAMPLE_PATH_CONFIG = "sample.file.path";
    public static final String PARTITION_PATH_CONFIG = "partition.file.path";
    public static final String CUBE_FILE_PATH_CONFIG = "cube.file.path";

    public static final String PARALLELISM_OBJECT_SPOUT_CONFIG = "parallelism.object.spout";
    public static final String PARALLELISM_QUERY_SPOUT_CONFIG = "parallelism.query.spout";
    public static final String PARALLELISM_BOLT_PARSER_CONFIG = "parallelism.bolt.parser";
    public static final String PARALLELISM_BOLT_QUERY_ANALYZER_CONFIG = "parallelism.bolt.query.analyzer";
    public static final String PARALLELISM_BOLT_ROUTER_CONFIG = "parallelism.bolt.router";
    public static final String PARALLELISM_BOLT_WORKER_CONFIG = "parallelism.bolt.worker";
    public static final String PARALLELISM_BOLT_MERGER_CONFIG = "parallelism.bolt.merger";
    public static final String PARALLELISM_BOLT_CONTINUOUS_WORKER_CONFIG = "parallelism.bolt.continuous.worker";

    public static final String STOPWORDS = "a,able,about,above,according,accordingly,across,actually,after,afterwards,again,against,all,allow,allows,almost,alone,along,already,also,although,always,am,among,amongst,an,and,another,any,anybody,anyhow,anyone,anything,anyway,anyways,anywhere,apart,appear,appreciate,appropriate,are,around,as,aside,ask,asking,associated,at,available,away,awfully,b,be,became,because,become,becomes,becoming,been,before,beforehand,behind,being,believe,below,beside,besides,best,better,between,beyond,both,brief,but,by,c,came,can,cannot,cant,cause,causes,certain,certainly,changes,clearly,co,com,come,comes,concerning,consequently,consider,considering,contain,containing,contains,corresponding,could,course,currently,d,definitely,described,despite,did,different,do,does,doing,done,down,downwards,during,e,each,edu,eg,eight,either,else,elsewhere,enough,entirely,especially,et,etc,even,ever,every,everybody,everyone,everything,everywhere,ex,exactly,example,except,f,far,few,fifth,first,five,followed,following,follows,for,former,formerly,forth,four,from,further,furthermore,g,get,gets,getting,given,gives,go,goes,going,gone,got,gotten,greetings,h,had,happens,hardly,has,have,having,he,hello,help,hence,her,here,hereafter,hereby,herein,hereupon,hers,herself,hi,him,himself,his,hither,hopefully,how,howbeit,however,i,ie,if,ignored,immediate,in,inasmuch,inc,indeed,indicate,indicated,indicates,inner,insofar,instead,into,inward,is,it,its,itself,j,just,k,keep,keeps,kept,know,knows,known,l,last,lately,later,latter,latterly,least,less,lest,let,like,liked,likely,little,look,looking,looks,ltd,m,mainly,many,may,maybe,me,mean,meanwhile,merely,might,more,moreover,most,mostly,much,must,my,myself,n,name,namely,nd,near,nearly,necessary,need,needs,neither,never,nevertheless,new,next,nine,no,nobody,non,none,noone,nor,normally,not,nothing,novel,now,nowhere,o,obviously,of,off,often,oh,ok,okay,old,on,once,one,ones,only,onto,or,other,others,otherwise,ought,our,ours,ourselves,out,outside,over,overall,own,p,particular,particularly,per,perhaps,placed,please,plus,possible,presumably,probably,provides,q,que,quite,qv,r,rather,rd,re,really,reasonably,regarding,regardless,regards,relatively,respectively,right,s,said,same,saw,say,saying,says,second,secondly,see,seeing,seem,seemed,seeming,seems,seen,self,selves,sensible,sent,serious,seriously,seven,several,shall,she,should,since,six,so,some,somebody,somehow,someone,something,sometime,sometimes,somewhat,somewhere,soon,sorry,specified,specify,specifying,still,sub,such,sup,sure,t,take,taken,tell,tends,th,than,thank,thanks,thanx,that,thats,the,their,theirs,them,themselves,then,thence,there,thereafter,thereby,therefore,therein,theres,thereupon,these,they,think,third,this,thorough,thoroughly,those,though,three,through,throughout,thru,thus,to,together,too,took,toward,towards,tried,tries,truly,try,trying,twice,two,u,un,under,unfortunately,unless,unlikely,until,unto,up,upon,us,use,used,useful,uses,using,usually,uucp,v,value,various,very,via,viz,vs,w,want,wants,was,way,we,welcome,well,went,were,what,whatever,when,whence,whenever,where,whereafter,whereas,whereby,wherein,whereupon,wherever,whether,which,while,whither,who,whoever,whole,whom,whose,why,will,willing,wish,with,within,without,wonder,would,x,y,yes,yet,you,your,yours,yourself,yourselves,z,zero";
    public static final String[] GROUPS = {"country", "city", "month", "week", "day", "hour", "topic",
            "country month", "country week", "country day", "country hour", "country topic",
            "city month", "city week", "city day", "city hour", "city topic",
            "month topic", "week topic", "day topic", "hour topic",
            "country month topic", "country week topic", "country day topic", "country hour topic",
            "city month topic", "city week topic", "city day topic", "city hour topic", "none"};
    public static final String[] TEXT_TOPICS = {"sport", "music", "movie", "it", "nba", "football", "nfl", "baseball"};
    public static final String ROOT = "city hour topic";

    public static final int SNAPSHOT_QUERY = 0;
    public static final int CONTINUOUS_QUERY = 1;
}
