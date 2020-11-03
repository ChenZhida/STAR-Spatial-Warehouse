package xxx.project.util;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.tuple.Tuple;

import java.time.LocalDate;
import java.util.*;

public class WarehouseQuery {
    public static String MEASURE = "measure";
    public static String GROUPBY = "group by";
    public static String RANGE = "range";
    public static String KEYWORDS = "keywords";

    public static String MEASURE_COUNT = "count";
    public static String MEASURE_WORD_COUNT = "word count";
    public static String GROUPBY_ALL = "*";

    private int queryId; // It is meaningful for continuous queries only
    private int type;
    private int syncFreq = 60; // seconds: default value is 60
    private boolean needSort = false; // whether need to sort the results
    private String sortByColumn = ""; // sort the results by which column
    private String sortOrder = ""; // descending or ascending
    private int kOfTopK = 10; // k of topK(k)
    private String measure = MEASURE_COUNT; // default value is count
    private String groupBy = GROUPBY_ALL; // default value is *
    private Tuple4<Double, Double, Double, Double> range = null;
    private HashSet<String> keywords = null;
    private LocalDate afterDate = null;
    private String topic = "";
    private int intervalLength = 0;

    public WarehouseQuery() {}

    public WarehouseQuery(Tuple request) {
        String requestText = request.getString(4);
        int lastIndex = requestText.lastIndexOf('\n');
        int secondLastIndex = requestText.substring(0, lastIndex).lastIndexOf('\n');
        String rangeText = requestText.substring(secondLastIndex + 1, lastIndex);
        String sqlText = requestText.substring(0, secondLastIndex).replaceAll("\n", " ");

        Random rand = new Random(0);
        int randIndex = rand.nextInt(Constants.TEXT_TOPICS.length);
        topic = Constants.TEXT_TOPICS[randIndex];

        queryId = Integer.valueOf(requestText.substring(lastIndex + 1));

        QueryParser queryParser = new QueryParser();
        StringBuilder parsedSQL = new StringBuilder();

        queryParser.parseSQLQuery(sqlText, parsedSQL);
        buildFromParsedText(parsedSQL.toString(), rangeText);
    }

    @Deprecated
    public WarehouseQuery(String str) {
        String[] attrs = str.split("" + Constants.SEPARATOR);
        HashMap<String, String> query = new HashMap<>();
        for (String attr: attrs) {
            System.out.println(attr);
            String[] pair = attr.split(":");
            String k = pair[0], v = pair[1];
            query.put(k, v);
        }

        measure = query.getOrDefault(MEASURE, MEASURE_COUNT);
        groupBy = query.getOrDefault(GROUPBY, GROUPBY_ALL);
        String rangeStr = query.getOrDefault(RANGE, null);
        String keywordsStr = query.getOrDefault(KEYWORDS, null);

        if (rangeStr != null) {
            String[] strList = rangeStr.split(" ");
            double latFrom = Double.valueOf(strList[0]);
            double lonFrom = Double.valueOf(strList[1]);
            double latTo = Double.valueOf(strList[2]);
            double lonTo = Double.valueOf(strList[3]);
            range = new Tuple4<>(latFrom, lonFrom, latTo, lonTo);
        }

        if (keywordsStr != null) {
            String[] strList = keywordsStr.split(" ");
            keywords = new HashSet<>();
            keywords.addAll(Arrays.asList(strList));
        }
    }

    public WarehouseQuery(String measure,
                          String groupBy,
                          Tuple4<Double, Double, Double, Double> range,
                          HashSet<String> keywords) {
        this.measure = measure;
        this.groupBy = groupBy;
        this.range = range;
        this.keywords = keywords;
    }

    public WarehouseQuery(String measure,
                          String groupBy,
                          Tuple4<Double, Double, Double, Double> range,
                          HashSet<String> keywords,
                          String topic) {
        this.measure = measure;
        this.groupBy = groupBy;
        this.range = range;
        this.keywords = keywords;
        this.topic = topic;
    }

    public WarehouseQuery(String measure,
                          String groupBy,
                          Tuple4<Double, Double, Double, Double> range,
                          HashSet<String> keywords,
                          String topic,
                          int intervalLength) {
        this.measure = measure;
        this.groupBy = groupBy;
        this.range = range;
        this.keywords = keywords;
        this.topic = topic;
        this.intervalLength = intervalLength;
    }

    public void buildFromParsedText(String text, String rangeText) {
        String[] lines = text.split("\n");
        for (String line : lines) {
            if (line.startsWith("Query type:"))
                type = line.indexOf("snapshot") != -1 ? Constants.SNAPSHOT_QUERY : Constants.CONTINUOUS_QUERY;
            else if (line.startsWith(QueryParser.SYNC)) {
                String[] strArray = line.substring(QueryParser.SYNC.length() + 1).trim().split(" ");
                if (strArray[1].equals(QueryParser.SECOND))
                    syncFreq = Integer.valueOf(strArray[0]);
                else if (strArray[1].equals(QueryParser.MINUTE))
                    syncFreq = Integer.valueOf(strArray[0]) * 60;
                else
                    syncFreq = Integer.valueOf(strArray[0]) * 3600;
            } else if (line.startsWith(QueryParser.AGGR_TEXT)) {
                int leftBracketIndex = line.lastIndexOf("(");
                int rightBracketIndex = line.lastIndexOf(")");
                String aggr = line.substring(QueryParser.AGGR_TEXT.length() + 1, leftBracketIndex);
                if (aggr.equals(QueryParser.AGGR_FUNCS[0]))
                    measure = MEASURE_COUNT;
                else if (aggr.equals(QueryParser.AGGR_FUNCS[1])) {
                    measure = MEASURE_WORD_COUNT;
                    kOfTopK = Integer.valueOf(line.substring(leftBracketIndex + 1, rightBracketIndex));
                }
            } else if (line.startsWith(QueryParser.LOC)) {
                String[] strList = rangeText.split(" ");
                double latFrom = Double.valueOf(strList[0]);
                double lonFrom = Double.valueOf(strList[1]);
                double latTo = Double.valueOf(strList[2]);
                double lonTo = Double.valueOf(strList[3]);
                range = new Tuple4<>(latFrom, lonFrom, latTo, lonTo);
            } else if (line.startsWith(QueryParser.TEXT)) {
                int keyStIndex = line.indexOf(QueryParser.CONTAINS.toUpperCase()) + QueryParser.CONTAINS.length() + 1;
                String keysStr = line.substring(keyStIndex);
                String[] keys = keysStr.split(QueryParser.AND);
                keywords = new HashSet<>();
                for (String key: keys)
                    keywords.add(key.trim());
            } else if (line.startsWith(QueryParser.TIME)) {
                if (line.indexOf("ago") > 0) {
                    String[] splitText = line.split(" ");
                    intervalLength = Integer.valueOf(splitText[2]);
                } else {
                    int timeStIndex = line.lastIndexOf(" ") + 1;
                    String[] nums = line.substring(timeStIndex).split("-");
                    int year = Integer.valueOf(nums[0]);
                    int month = Integer.valueOf(nums[1]);
                    int day = Integer.valueOf(nums[2]);
                    afterDate = LocalDate.of(year, month, day);
                }
            }  else if (line.startsWith(QueryParser.TOPIC)) {
              int topicStartIndex = line.indexOf("=") + 1;
              topic = line.substring(topicStartIndex).trim();
            } else if (line.startsWith(QueryParser.GROUP_BY)) {
                groupBy = line.substring(QueryParser.GROUP_BY.length() + 1);
            } else if (line.startsWith(QueryParser.ORDER_BY)) {
                if (line.indexOf("null") == -1) {
                    needSort = true;
                    int firstSpaceIndex = line.indexOf(" ");
                    int lastSpaceIndex = line.lastIndexOf(" ");
                    sortByColumn = line.substring(firstSpaceIndex + 4, lastSpaceIndex);
                    sortOrder = line.substring(lastSpaceIndex + 1);
                }
            }
        }
    }

    public int getQueryId() {
        return queryId;
    }

    public String getQueryInfo() {
        StringBuilder out = new StringBuilder();
        out.append("Query type: " +
                ((type == Constants.SNAPSHOT_QUERY) ? "snapshot" : "continuous") + "\n");
        if (type == Constants.CONTINUOUS_QUERY)
            out.append("Sync frequency: " + syncFreq + " seconds\n");
        out.append("Aggregation function: " + measure + "\n");
        if (measure.equals(MEASURE_WORD_COUNT))
            out.append("The parameter k: " + kOfTopK + "\n");
        if (range != null)
            out.append("Range constraint: [(" + range._1() + ", " +
                    range._2() + "), (" + range._3() + ", " + range._4() + ")]\n");
        if (keywords != null) {
            out.append("Keyword constraint:");
            for (String key : keywords)
                out.append(" " + key);
            out.append("\n");
        }
        if (afterDate != null)
            out.append("Time constraint: after " + afterDate.getYear() +
                    "-" + afterDate.getMonth() + "-" + afterDate.getDayOfMonth() + "\n");
        out.append("Group by: " + groupBy + "\n");
        if (needSort) {
            out.append("Order by: " + sortByColumn + " " + sortOrder + "\n");
        }

        return out.toString();
    }

    public void printQueryInfo() {
        System.out.print(getQueryInfo());
    }

    public String getMeasure() {
        return measure;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public String getTopic() {
        return topic;
    }

    public int getIntervalLength() {
        return intervalLength;
    }

    public Optional<Tuple4<Double, Double, Double, Double>> getRange() {
        return Optional.ofNullable(range);
    }

    public boolean needSort() {
        return needSort;
    }

    public String getSortOrder() {
        return sortOrder;
    }

    public Optional<HashSet<String>> getKeywords() {
        return Optional.ofNullable(keywords);
    }

    public int getkOfTopK() {
        return kOfTopK;
    }

    public boolean isSnapshotQuery() {
        return type == Constants.SNAPSHOT_QUERY;
    }

    public boolean isContinuousQuery() {
        return type == Constants.CONTINUOUS_QUERY;
    }

    public static boolean verify(WarehouseQuery query, SpatioTextualObject object) {
        Tuple2<Double, Double> coord = object.getCoord();
        double lat = coord._1(), lon = coord._2();
        Tuple4<Double, Double, Double, Double> range = query.getRange().
                orElse(new Tuple4<>(-90.0, -180.0, 90.0, 180.0));

        if (lat < range._1() || lat > range._3() ||
                lon < range._2() || lon > range._4())
            return false;
        if (query.afterDate != null &&
                query.afterDate.isAfter(object.getDate()))
            return false;

        Optional<HashSet<String>> keywords = query.getKeywords();
        if (keywords.isPresent()) {
            List<String> objectTags = Arrays.asList(object.getText().split(""+ Constants.TEXT_SEPARATOR));
            HashSet<String> tagSet = new HashSet<>();
            for (String tag: objectTags)
                tagSet.add(tag.toLowerCase());

            return keywords.get().stream().allMatch(tagSet::contains);
        } else
            return true;
    }

    public static String groupBy(WarehouseQuery query, SpatioTextualObject object) {
        String groupBy = query.getGroupBy();
        return object.getGroupBy(groupBy);
//        if (groupBy.equals("country"))
//            return object.getCountry();
//        else if (groupBy.equals("city"))
//            return object.getCity();
//        else if (groupBy.equals("year"))
//            return object.getYear();
//        else if (groupBy.equals("month"))
//            return object.getMonth();
//        else if (groupBy.equals("day"))
//            return object.getDay();
//        else if (groupBy.equals("hour"))
//            return object.getHour();
//        else
//            return GROUPBY_ALL;
    }
}
