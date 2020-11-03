package xxx.project.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class QueryParser implements Serializable {
    public static final long serialVersionUID = 600132L;

    public static final String SELECT = "select";
    public static final String WHERE = "where";
    public static final String GROUP_BY = "group by";
    public static final String SYNC = "sync";
    public static final String FROM = "from";
    public static final String[] AGGR_FUNCS = {"count", "topk"};
    public static final String AGGR_TEXT = "aggregation";
    public static final String[] GROUP_BY_ATTRS = Constants.GROUPS;
    public static final String ORDER_BY = "order by";
    public static final String DESC = "desc";
    public static final String ASC = "asc";
    public static final String LOC = "loc";
    public static final String INSIDE = "inside";
    public static final String R = "r";
    public static final String TEXT = "text";
    public static final String TOPIC = "topic";
    public static final String CONTAINS = "contains";
    public static final String TIME = "time";
    public static final String AFTER = "after";
    public static final String SECOND = "second";
    public static final String MINUTE = "minute";
    public static final String HOUR = "hour";
    public static final String AND = "and";
    public static final String[] LOC_GROUPS = {"country", "city"};
    public static final String[] TIME_GROUPS = {"month", "week", "day", "hour"};
    public static final String[] TEXT_GROUPS = {"topic"};

    private Set<String> aggrFuncs = null;
    private Set<String> groupByAttrsSet = null;
    private Set<String> locGroups = null;
    private Set<String> timeGroups = null;
    private Set<String> textGroups = null;

    public QueryParser() {
        aggrFuncs = new HashSet<>(Arrays.asList(AGGR_FUNCS));
        groupByAttrsSet = new HashSet<>(Arrays.asList(GROUP_BY_ATTRS));
        locGroups = new HashSet<>(Arrays.asList(LOC_GROUPS));
        timeGroups = new HashSet<>(Arrays.asList(TIME_GROUPS));
        textGroups = new HashSet<>(Arrays.asList(TEXT_GROUPS));
    }

    public boolean parseSQLQuery(String sqlText, StringBuilder formattedQuery) {
        String sql = sqlText.trim().replaceAll(" +", " ").toLowerCase();
        int selectIndex = sql.indexOf(SELECT);
        int fromIndex = sql.indexOf(FROM);
        int whereIndex = sql.indexOf(WHERE);
        int groupByIndex = sql.indexOf(GROUP_BY);
        int syncIndex = sql.indexOf(SYNC);

        if (selectIndex < 0 || fromIndex < 0) {
            printSQLErrorInfo();
            return false;
        }

        String selectText = sql.substring(selectIndex, fromIndex);
        if (syncIndex > 0) {
            String syncText = sql.substring(syncIndex);
            int secondIndex = syncText.indexOf(SECOND);
            int minIndex = syncText.indexOf(MINUTE);
            int hourIndex = syncText.indexOf(HOUR);
            if (secondIndex != -1 || minIndex != -1 || hourIndex != -1) {
                formattedQuery.append("Query type: continuous\n");
                formattedQuery.append(SYNC + ":");
                if (hourIndex != -1)
                    formattedQuery.append(syncText.substring(SYNC.length() + 1, hourIndex + HOUR.length()) + "\n");
                else if (minIndex != -1)
                    formattedQuery.append(syncText.substring(SYNC.length() + 1, minIndex + MINUTE.length()) + "\n");
                else
                    formattedQuery.append(syncText.substring(SYNC.length() + 1,
                            secondIndex + SECOND.length()) + "\n");
            } else {
                printSQLErrorInfo();
                return false;
            }
        }
        else {
            formattedQuery.append("Query type: snapshot\n");
        }

        if (!parseSelectText(formattedQuery, selectText)) {
            printSQLErrorInfo();
            return false;
        }

        if (whereIndex != -1) {
            // where exists
            String whereText = sql.substring(whereIndex,
                    (groupByIndex != -1) ? groupByIndex : sql.length());

            if (!parseWhereText(formattedQuery, whereText)) {
                printSQLErrorInfo();
                return false;
            }
        }

        if (groupByIndex != -1) {
            // group by exists
            String groupByText = (syncIndex > 0) ?
                    sql.substring(groupByIndex, syncIndex) : sql.substring(groupByIndex);

            if (!parseGroupByText(formattedQuery, groupByText)) {
                printSQLErrorInfo();
                return false;
            }
        } else {
            formattedQuery.append("group by: *\n");
        }

        return true;
    }

    private boolean parseSelectText(StringBuilder formattedQuery, String text) {
        String[] words = text.replaceAll(",", "").split(" ");
        String aggr_func = null;

        int numBrackets = 0;
        for (String word : words) {
            int bracketIndex = word.indexOf("(");
            if (bracketIndex != -1 && aggrFuncs.contains(word.substring(0, bracketIndex))) {
                aggr_func = word;
                ++numBrackets;
            }
        }

        if (aggr_func != null)
            formattedQuery.append(AGGR_TEXT + ":" + aggr_func + "\n");

        return aggr_func != null && numBrackets == 1;
    }

    private boolean parseWhereText(StringBuilder formattedQuery, String text) {
        String parsedText = text.substring(6).replaceAll("'", "");
        String[] conditions = parsedText.split(AND);

        StringBuilder rangeCon = new StringBuilder();
        StringBuilder textCon = new StringBuilder();
        StringBuilder timeCon = new StringBuilder();

        for (String cond : conditions) {
            String trimmedStr = cond.trim();
            if (trimmedStr.startsWith(LOC)) {
                String range = trimmedStr.substring(
                        LOC.length() + 1 + INSIDE.length() + 1);
                rangeCon.append("loc INSIDE " + range);
            } else if (trimmedStr.startsWith(TEXT)) {
                String key = trimmedStr.substring(
                        TEXT.length() + 1 + CONTAINS.length() + 1);
                if (textCon.length() == 0)
                    textCon.append("text CONTAINS " + key);
                else
                    textCon.append(" AND " + key);
            } else if (trimmedStr.startsWith(TIME)) {
                String afterTime = trimmedStr.substring(
                        TIME.length() + 1 + AFTER.length() + 1);
                timeCon.append("time AFTER " + afterTime);
            }
        }

        if (rangeCon.length() > 0)
            formattedQuery.append(LOC + ":" + rangeCon + "\n");
        if (textCon.length() > 0)
            formattedQuery.append(TEXT + ":" + textCon + "\n");
        if (timeCon.length() > 0)
            formattedQuery.append(TIME + ":" + timeCon + "\n");

        return true;
    }

    private boolean parseGroupByText(StringBuilder formattedQuery, String text) {
        String parsedText = text.replaceAll(",", "");
        int orderByIndex = parsedText.indexOf(ORDER_BY);
        String groupByAttrs = null;
        String orderBy = "null";

        int st = parsedText.indexOf(GROUP_BY) + 9;
        if (orderByIndex > 0) {
            // need to sort the results
            groupByAttrs = parsedText.substring(st, orderByIndex).trim();
            orderBy = parsedText.substring(orderByIndex + 8).replace("()", "").trim();
        } else {
            groupByAttrs = parsedText.substring(st);
        }

        groupByAttrs = sortGroupByAttrs(groupByAttrs);
        if (groupByAttrs == null || !groupByAttrsSet.contains(groupByAttrs))
            return false;

        formattedQuery.append(GROUP_BY + ":" + groupByAttrs + "\n");
        formattedQuery.append(ORDER_BY + ":" + orderBy + "\n");

        return true;
    }

    private String sortGroupByAttrs(String attrs) {
        String[] attrArray = attrs.split(" ");
        StringBuilder sortedStr = new StringBuilder();

        for (String attr : attrArray) {
            if (locGroups.contains(attr))
                sortedStr.append(attr + " ");
        }

        for (String attr : attrArray) {
            if (timeGroups.contains(attr))
                sortedStr.append(attr + " ");
        }

        for (String attr : attrArray) {
            if (textGroups.contains(attr))
                sortedStr.append(attr + " ");
        }

        if (sortedStr.length() == 0)
            return null;
        else
            return sortedStr.substring(0, sortedStr.length() - 1);
    }

    private void printSQLErrorInfo() {
        System.out.println("The format of the SQL sentence is incorrect!");
        System.out.println("The correct syntax should be:");
        System.out.println("SELECT aggr_func() FROM data");
        System.out.println("[WHERE condition(s)]");
        System.out.println("[GROUP BY attribute(s)]");
        System.out.println("[SYNC freq]");
    }

    public static void main(String[] args) {
        QueryParser parser = new QueryParser();
        String[] sqls = { "SELECT count(), date FROM data WHERE Loc INSIDE R    GROUP    by day  ",
                "SELECT count(10), date FROM data    ",
                "SELECT count(), date FROM data  WHERE loc inside R  ",
                "SELECT count(), date FROM data GROUP BY CITY  ",
                "SELECT count(), date FROM data WHERE Loc INSIDE R AND TEXT CONTAINS 'KEY1' and text contains 'key2' and text contains 'key3'   ",
                "SELECT count(), date FROM data WHERE Loc INSIDE R AND TEXT CONTAINS 'KEY1' and text contains 'key2' and text contains 'key3' group by city, hour   ",
                "SELECT count(), date FROM data WHERE Loc INSIDE R AND TEXT CONTAINS 'KEY1' and text contains 'key2' and text contains 'key3' group by city, hour order by count() asc   ",
                "SELECT count(), date FROM data WHERE TEXT CONTAINS 'KEY1' and text contains 'key2' and text contains 'key3' group by city, hour order by count() asc   ",
                "SELECT count(), date FROM data WHERE Loc INSIDE R    GROUP    by country,,  day  ",
                "SELECT count(), date FROM data WHERE Loc INSIDE R    GROUP    by day,,  country  ",
                "SELECT   count(), date from data WHERE Loc INSIDE R GROUP by city, day  ",
                "SELECT   count(), date from data WHERE Loc INSIDE R GROUP by country  ",
                "SELECT   count(), date from data WHERE Loc INSIDE R GROUP by date,  country  order by date desc",
                "SELECT   count(), date from data WHERE Loc INSIDE R GROUP by date,  country  order by hour desc sync 1 day",
                "SELECT count(), date FROM data WHERE Loc INSIDE R GROUP by hour,year order by count() ASC  ",
                "count(), date FROM data WHERE Loc INSIDE R GROUP by year,, country order by date ASC  ",
                "selecT count(10), date FROM data Loc INSIDE R    GROUP by city, hour order by date ASC  ",
                "selecT count(10), date FROM data Loc INSIDE R     hour order by hour ASC",
                "sele cT count(9), date FROM data Loc INSIDE R     hour, topic, city order by min ASC",
                "SELECT topk(10), city FROM data WHERE Loc INSIDE R GROUP by topic order by text ASC",
                "SELECT topxxk(20), count(), city FROM data WHERE Loc INSIDE R GROUP by country,    hour order by day ASC",
                "SELECT topk(5), city, topic FROM stTREAM wheRe Loc INSIDE R AND TEXT CONTAINS 'key' AND time AFTER '2019-11-02' GROUP by hour order by count() ASC",
                "SELECT topk(10), city, topic FROM stTREAM wheRe Loc INSIDE R AND TEXT CONTAINS 'key' AND time AFTER '2019-11-02' GROUP by hour order by count ASC  sync 1 minute",
                "SELECT topk(1000), city, topic FROM stTREAM wheRe Loc INSIDE R AND TEXT CONTAINS 'key' AND time AFTER '2019-10-1' GROUP by hour order by count ASC  sync   1    hour",
                "SELECT topk(92), city, topic FROM stTREAM wheRe Loc INSIDE R AND TEXT CONTAINS 'key' AND text contains 'test' AND time AFTER '2019-10-1' GROUP by hour order by count ASC  sync   1    hour",
                "SELECT topk(199), city, topic FROM stTREAM wheRe Loc INSIDE R AND TEXT CONTAINS 'key' AND time AFTER '2019-09-2' GROUP by hour order by topk()    ASC    SYNC 1 minute",
                "SELECT count() from DATA where text contains 'key'",
                /*
                "SELECT count() from DATA where text contains 'key' AND TIME AFTER '10 minute ago'",
                "SELECT count() from DATA where text contains 'key' AND TIME AFTER 10 minute ago",
                 */
                "SELECT topk(3), city, hour from data group by city, hour",
        };
        for (String sql : sqls) {
            StringBuilder parsedSQL = new StringBuilder();
            System.out.println("Input query text: " + sql);
            boolean isValid = parser.parseSQLQuery(sql, parsedSQL);
            System.out.println("Parsing..." + (isValid ? "Valid" : "Not valid"));
            if (isValid) {
                System.out.println(parsedSQL);
                System.out.println("Building a query...");
                WarehouseQuery q = new WarehouseQuery();
                q.buildFromParsedText(parsedSQL.toString(), "128.0 -99.3 129 -33");
                q.printQueryInfo();
                System.out.println();
            }
        }

        String requestText = "SELECT count(), city FROM DATA WHERE loc INSIDE R GROUP BY city\n44.49 -78.649 44.599 -78.443\n0";
        int firstLineEndIndex = requestText.indexOf('\n');
        int secondLineEndIndex = requestText.lastIndexOf('\n');
        String rangeText = requestText.substring(firstLineEndIndex + 1, secondLineEndIndex);
        String sqlText = requestText.substring(0, firstLineEndIndex);

        int queryId = Integer.valueOf(requestText.substring(secondLineEndIndex + 1));

        QueryParser queryParser = new QueryParser();
        StringBuilder parsedSQL = new StringBuilder();

        queryParser.parseSQLQuery(sqlText, parsedSQL);
//        buildFromParsedText(parsedSQL.toString(), rangeText);
    }
}
