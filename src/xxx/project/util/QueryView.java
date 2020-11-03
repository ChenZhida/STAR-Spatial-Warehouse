package xxx.project.util;

import java.util.*;
import java.util.stream.Collectors;

public class QueryView {
    private int id;
    private double latFrom = Constants.MIN_LAT;
    private double lonFrom = Constants.MIN_LON;
    private double latTo = Constants.MAX_LAT;
    private double lonTo = Constants.MAX_LON;
    private Set<String> keys = null;
    private String aggrFunc;
    private String groupBy;
    private int viewSize;
    private Map<String, AggrValues> aggrMap = new HashMap<>();
    private int kOfTopK = 5;
    private int topKCounterSize = Constants.TOPK_COUNTER_SIZE;

    public QueryView(int id, String aggrFunc, String groupBy) {
        this.id = id;
        this.aggrFunc = aggrFunc;
        this.groupBy = groupBy;
    }

    public QueryView(int id, String aggrFunc, String groupBy, double latFrom, double lonFrom, double latTo, double lonTo) {
        this.id = id;
        this.aggrFunc = aggrFunc;
        this.groupBy = groupBy;
        this.latFrom = latFrom;
        this.lonFrom = lonFrom;
        this.latTo = latTo;
        this.lonTo = lonTo;
    }

    public QueryView(int id, String aggrFunc, String groupBy, Set<String> keys) {
        this.id = id;
        this.aggrFunc = aggrFunc;
        this.groupBy = groupBy;
        this.keys = new HashSet<>(keys);
    }

    public QueryView(int id, String aggrFunc, String groupBy,
                     double latFrom, double lonFrom,
                     double latTo, double lonTo,
                     Set<String> keys) {
        this.id = id;
        this.aggrFunc = aggrFunc;
        this.groupBy = groupBy;
        this.latFrom = latFrom;
        this.lonFrom = lonFrom;
        this.latTo = latTo;
        this.lonTo = lonTo;
        this.keys = new HashSet<>(keys);
    }

    public int getId() {
        return id;
    }

    public String getAggrFunc() {
        return aggrFunc;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public int getViewSize() {
        return viewSize;
    }

    public void setViewSize(int viewSize) {
        this.viewSize = viewSize;
    }

    public Map<String, AggrValues> getResult() {
        return aggrMap;
    }

    public void setkOfTopK(int k) {
        kOfTopK = k;
    }

    public void setTopKCounterSize(int counterSize) {
        this.topKCounterSize = counterSize;
    }

    public void updateView(SpatioTextualObject object) {
        if (!satisfyConstraint(object))
            return;

        String objGroup = object.getGroupBy(groupBy);
        AggrValues aggrValues = aggrMap.get(objGroup);
        if (aggrValues == null) {
            aggrValues = new AggrValues(aggrFunc, topKCounterSize);
            aggrMap.put(objGroup, aggrValues);
        }
        aggrValues.update(object);
    }

    public boolean satisfyConstraint(SpatioTextualObject object) {
        if (!isInsideRange(object, new Tuple2<>(latFrom, lonFrom), new Tuple2<>(latTo, lonTo)) ||
                !containKeys(object))
            return false;

        return true;
    }

    private boolean isInsideRange(SpatioTextualObject object, Tuple2<Double, Double> cornerFrom,
                                  Tuple2<Double, Double> cornerTo) {
        Tuple2<Double, Double> coord = object.getCoord();
        double lat = coord._1();
        double lon = coord._2();

        return cornerFrom._1() <= lat && cornerTo._1() > lat &&
                cornerFrom._2() <= lon && cornerTo._2() > lon;
    }

    private boolean containKeys(SpatioTextualObject object) {
        if (keys == null)
            return true;

        Set<String> terms = object.getTerms();
        for (String term : terms)
            if (!keys.contains(term))
                return false;

        return true;
    }
}

