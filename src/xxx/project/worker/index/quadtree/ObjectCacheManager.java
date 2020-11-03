package xxx.project.worker.index.quadtree;

import org.apache.storm.tuple.Tuple;
import xxx.project.util.Constants;
import xxx.project.util.SpatioTextualObject;
import xxx.project.util.Tuple2;
import xxx.project.util.Tuple4;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class ObjectCacheManager implements Serializable {
    public static final long serialVersionUID = 103L;
    private class Cache implements Serializable {
        public int cacheId;
        public int cacheType;
        public int nodeId;
        public double latFrom;
        public double lonFrom;
        public double latTo;
        public double lonTo;
        public List<SpatioTextualObject> objects = new ArrayList<>();

        public Cache(int cacheType, int nodeId, double latFrom, double lonFrom,
                     double latTo, double lonTo) {
            this.cacheType = cacheType;
            this.nodeId = nodeId;
            this.latFrom = latFrom;
            this.lonFrom = lonFrom;
            this.latTo = latTo;
            this.lonTo = lonTo;

            this.cacheId = cacheType << 16 + nodeId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (!(obj instanceof Cache))
                return false;

            return ((Cache) obj).cacheId == this.cacheId;
        }

        public boolean isCoverRange(Tuple4<Double, Double, Double, Double> range) {
            return latFrom <= range._1() && lonFrom <= range._2() && latTo >= range._3() && lonTo >= range._4();
        }

        public boolean isCoverPoint(Tuple2<Double, Double> point) {
            return latFrom < point._1() && latTo > point._1() && lonFrom < point._2() && lonTo > point._2();
        }

        public boolean isCoverPoint(double lat, double lon) {
            return isCoverPoint(new Tuple2<>(lat, lon));
        }

        public int getSize() {
            return objects.size();
        }

        public void addObject(SpatioTextualObject object) {
            objects.add(object);
        }
    }

    public final int CACHE_SIZE_CAPACITY = Constants.OBJECT_CACHE_CAPACITY;
    public final int CACHE_UPDATE_LIMIT = 100;

    private List<Cache> caches = new ArrayList<>();
    private Set<Integer> cacheIdSet = new HashSet<>();
    private Map<Integer, List<Cache>> nodeId2Caches = new HashMap<>();
    private Map<Integer, Integer> LRU = new HashMap<>();
    private int queryTimes = 0;
    private int sumCacheSize = 0;

    public List<SpatioTextualObject> getObjectsUsingCache(RawData rawData,
                                                          int nodeId,
                                                          Tuple4<Double, Double, Double, Double> range) {
        if (++queryTimes >= CACHE_UPDATE_LIMIT)
            callCacheUpdatePolicy(rawData);

        List<Cache> cacheList = nodeId2Caches.get(nodeId);
        if (cacheList == null)
            return new ArrayList<>();
        Cache optCache = null;
        int maxDeltaLoad = -1;
        int numObjectsInLeafNode = rawData.getNumObjectsInLeafNode(nodeId);

        for (Cache cache : cacheList) {
            if (!cache.isCoverRange(range))
                continue;

            int deltaLoad = numObjectsInLeafNode - cache.getSize();
            if (deltaLoad > maxDeltaLoad) {
                maxDeltaLoad = deltaLoad;
                optCache = cache;
            }
        }

        return optCache.objects;
    }

    public List<SpatioTextualObject> getObjectsUsingCache(RawData rawData,
                                                          int nodeId,
                                                          Tuple2<Double, Double> cornerFrom,
                                                          Tuple2<Double, Double> cornerTo) {
        Tuple4<Double, Double, Double, Double> range = new Tuple4<>(cornerFrom._1(), cornerFrom._2(),
                cornerTo._1(), cornerTo._2());
        return getObjectsUsingCache(rawData, nodeId, range);
    }

    public void addObject2Cache(int nodeId, SpatioTextualObject object) {
        List<Cache> cacheList = nodeId2Caches.get(nodeId);
        if (cacheList == null)
            return;

        for (Cache cache : cacheList) {
            if (!cache.isCoverPoint(object.getCoord()))
                continue;
            cache.addObject(object);
        }
    }

    private void callCacheUpdatePolicy(RawData rawData) {
        int idCache2Remove = 0;
        int leastUsedTimes = Integer.MAX_VALUE;

        for (Map.Entry<Integer, Integer> entry : LRU.entrySet()) {
            int cacheId = entry.getKey();
            int usedTimes = entry.getValue();
            if (usedTimes < leastUsedTimes) {
                idCache2Remove = cacheId;
                leastUsedTimes = usedTimes;
            }
        }

        final int id2Remove = idCache2Remove;
        List<Cache> cache2Remove = caches.stream().
                filter(e -> e.cacheId == id2Remove).collect(Collectors.toList());
        caches.remove(cache2Remove.get(0));
        cacheIdSet.remove(id2Remove);
        LRU.remove(id2Remove);
        sumCacheSize -= cache2Remove.get(0).getSize();
        greedySelectCaches(rawData);

        queryTimes = 0;
        for (Integer id : LRU.keySet())
            LRU.put(id, 0);
    }

    private void greedySelectCaches(RawData rawData) {
        List<RawData.RawDataQuadTreeNode> leafNodes = rawData.getLeafNodes();
        List<Cache> candCaches = getCandidateCaches(rawData, leafNodes).stream().filter(n ->
                !cacheIdSet.contains(n.cacheId)).collect(Collectors.toList());
        List<Cache> selectedCaches = new ArrayList<>();

        while (sumCacheSize < CACHE_SIZE_CAPACITY) {
            Cache optCand = null;
            double optBenefit = -1;
            for (Cache cand : candCaches) {
                double benefit = getBenefit(cand, rawData);
                if (benefit > optBenefit)
                    optCand = cand;
            }

            caches.add(optCand);
            cacheIdSet.add(optCand.cacheId);
            LRU.put(optCand.cacheId, 0);
            List<Cache> cacheList = nodeId2Caches.get(optCand.nodeId);
            if (cacheList == null) {
                cacheList = new ArrayList<>();
                nodeId2Caches.put(optCand.cacheId, cacheList);
            }
            cacheList.add(optCand);
            sumCacheSize += optCand.getSize();
            selectedCaches.add(optCand);
        }

        for (Cache cache : selectedCaches) {
            List<SpatioTextualObject> cachedObjects = rawData.getObjectsInLeafNode(cache.nodeId).
                    stream().filter(o -> {
                double lat = o.getLatitude(), lon = o.getLongitude();
                return lat > cache.latFrom && lon > cache.lonFrom &&
                        lat < cache.latTo && lon < cache.lonTo;
            }).collect(Collectors.toList());
            for (SpatioTextualObject object : cachedObjects)
                cache.addObject(object);
        }
    }

    private List<Cache> getCandidateCaches(RawData rawData,
                                           List<RawData.RawDataQuadTreeNode> leafNodes) {
        List<Cache> candViews = new ArrayList<>();

        for (RawData.RawDataQuadTreeNode node : leafNodes) {
            int numObjects = rawData.getNumObjectsInLeafNode(node.id);
            List<Tuple4<Double, Double, Double, Double>> ranges =
                    rawData.getQueriesOverlapLeafNode(node.id);

            Tuple4[] eightTypes = {null, null, null, null, null, null, null, null};
            Tuple2<Double, Double> nodeUpLeft = new Tuple2<>(node.minLat, node.minLon);
            Tuple2<Double, Double> nodeUpRight = new Tuple2<>(node.minLat, node.maxLon);
            Tuple2<Double, Double> nodeDownLeft = new Tuple2<>(node.maxLat, node.minLon);
            Tuple2<Double, Double> nodeDownRight = new Tuple2<>(node.maxLat, node.maxLon);

            eightTypes[0] = new Tuple4<>(nodeUpLeft._1(), nodeUpLeft._2(), -90, nodeDownRight._2());
            eightTypes[1] = new Tuple4<>(nodeUpRight._1(), 180, nodeDownRight._1(), nodeDownRight._2());
            eightTypes[2] = new Tuple4<>(90, nodeDownLeft._2(), nodeDownRight._1(), nodeDownRight._2());
            eightTypes[3] = new Tuple4<>(nodeUpLeft._1(), nodeUpLeft._2(), nodeDownRight._1(), -180);
            eightTypes[4] = new Tuple4<>(-90, -180, -90, -180);
            eightTypes[5] = new Tuple4<>(-90, 180, -90, 180);
            eightTypes[6] = new Tuple4<>(-90, -180, 90, 180);
            eightTypes[7] = new Tuple4<>(90, -180, 90, -180);

            Tuple4<Double, Double, Double, Double> nodeRange = new Tuple4<>(node.minLat,
                    node.minLon, node.maxLat, node.maxLon);
            for (Tuple4<Double, Double, Double, Double> range : ranges) {
                int overlapType = getOverlapType(range, nodeRange);
                switch (overlapType) {
                    case 0: eightTypes[0].set_3(Math.max((Double) eightTypes[0]._3(), range._3()));
                        break;
                    case 1: eightTypes[1].set_2(Math.min((Double) eightTypes[1]._2(), range._2()));
                        break;
                    case 2: eightTypes[2].set_1(Math.min((Double) eightTypes[2]._1(), range._1()));
                        break;
                    case 3: eightTypes[3].set_4(Math.max((Double) eightTypes[3]._4(), range._4()));
                        break;
                    case 4: eightTypes[4].set_3(Math.max((Double) eightTypes[4]._3(), range._3()));
                            eightTypes[4].set_4(Math.max((Double) eightTypes[4]._4(), range._4()));
                        break;
                    case 5: eightTypes[5].set_2(Math.min((Double) eightTypes[5]._2(), range._2()));
                            eightTypes[5].set_3(Math.max((Double) eightTypes[5]._3(), range._3()));
                        break;
                    case 6: eightTypes[6].set_1(Math.max((Double) eightTypes[6]._1(), range._1()));
                            eightTypes[6].set_2(Math.min((Double) eightTypes[6]._2(), range._2()));
                        break;
                    case 7: eightTypes[7].set_1(Math.min((Double) eightTypes[7]._1(), range._1()));
                            eightTypes[7].set_4(Math.max((Double) eightTypes[7]._4(), range._4()));
                        break;
                    default: break;
                }
            }

            candViews.add(new Cache(0, node.id, (double)eightTypes[0]._1(),
                    (double)eightTypes[0]._2(), (double)eightTypes[0]._3(), (double)eightTypes[0]._4()));

            candViews.add(new Cache(1, node.id, (double)eightTypes[1]._1(),
                    (double)eightTypes[1]._2(), (double)eightTypes[1]._3(), (double)eightTypes[1]._4()));

            candViews.add(new Cache(2, node.id, (double)eightTypes[2]._1(),
                    (double)eightTypes[2]._2(), (double)eightTypes[2]._3(), (double)eightTypes[2]._4()));

            candViews.add(new Cache(3, node.id, (double)eightTypes[3]._1(),
                    (double)eightTypes[3]._2(), (double)eightTypes[3]._3(), (double)eightTypes[3]._4()));

            candViews.add(new Cache(4, node.id, (double)eightTypes[4]._1(),
                    (double)eightTypes[4]._2(), (double)eightTypes[4]._3(), (double)eightTypes[4]._4()));

            candViews.add(new Cache(5, node.id, (double)eightTypes[5]._1(),
                    (double)eightTypes[5]._2(), (double)eightTypes[5]._3(), (double)eightTypes[5]._4()));

            candViews.add(new Cache(6, node.id, (double)eightTypes[6]._1(),
                    (double)eightTypes[6]._2(), (double)eightTypes[6]._3(), (double)eightTypes[6]._4()));

            candViews.add(new Cache(7, node.id, (double)eightTypes[7]._1(),
                    (double)eightTypes[7]._2(), (double)eightTypes[7]._3(), (double)eightTypes[7]._4()));
        }

        return candViews;
    }

    private int getOverlapType(Tuple4<Double, Double, Double, Double> queryRange,
                               Tuple4<Double, Double, Double, Double> nodeRange) {
        if (queryRange._1() < nodeRange._1() && queryRange._2() < nodeRange._2() &&
                queryRange._3() < nodeRange._3() && queryRange._4() > nodeRange._4())
            return 0;
        else if (queryRange._1() < nodeRange._1() && queryRange._2() > nodeRange._2() &&
                queryRange._3() > nodeRange._3() && queryRange._4() > nodeRange._4())
            return 1;
        else if (queryRange._1() > nodeRange._1() && queryRange._2() < nodeRange._2() &&
                queryRange._3() > nodeRange._3() && queryRange._4() > nodeRange._4())
            return 2;
        else if (queryRange._1() < nodeRange._1() && queryRange._2() < nodeRange._2() &&
                queryRange._3() > nodeRange._3() && queryRange._4() < nodeRange._4())
            return  3;
        else if (queryRange._1() < nodeRange._1() && queryRange._2() < nodeRange._2() &&
                queryRange._3() < nodeRange._3() && queryRange._4() < nodeRange._4())
            return 4;
        else if (queryRange._1() < nodeRange._1() && queryRange._2() > nodeRange._2() &&
                queryRange._3() < nodeRange._3() && queryRange._4() > nodeRange._4())
            return 5;
        else if (queryRange._1() > nodeRange._1() && queryRange._2() > nodeRange._2() &&
                queryRange._3() > nodeRange._3() && queryRange._4() > nodeRange._4())
            return 6;
        else
            return 7;
    }

    private double getBenefit(Cache cand, RawData rawData) {
        double deltaLoad = 0.0;

        int numObjects = rawData.getNumObjectsInLeafNode(cand.nodeId);
        List<Tuple4<Double, Double, Double, Double>> ranges =
                rawData.getQueriesOverlapLeafNode(cand.nodeId);
        List<SpatioTextualObject> cachedObjects = rawData.getObjectsInLeafNode(cand.nodeId).
                stream().filter(o -> {
                    double lat = o.getLatitude(), lon = o.getLongitude();
                    return lat > cand.latFrom && lon > cand.lonFrom &&
                            lat < cand.latTo && lon < cand.lonTo;
        }).collect(Collectors.toList());
        deltaLoad += (numObjects - cachedObjects.size()) * ranges.size();

        if (deltaLoad < 0.01)
            return -1;
        else if (cachedObjects.isEmpty())
            return Integer.MAX_VALUE;

        for (Cache existedCache : caches) {
            if (existedCache.nodeId != cand.nodeId)
                continue;

            if (existedCache.latFrom <= cand.latFrom && existedCache.lonFrom <= cand.lonFrom &&
                    existedCache.latTo >= cand.latTo && existedCache.lonTo >= cand.lonTo) {
                List<SpatioTextualObject> largerCachedObjects = rawData.getObjectsInLeafNode(cand.nodeId).
                        stream().filter(o -> {
                            double lat = o.getLatitude(), lon = o.getLongitude();
                            return lat > existedCache.latFrom && lon > existedCache.lonFrom &&
                                    lat < existedCache.latTo && lon < existedCache.lonTo;
                }).collect(Collectors.toList());
                deltaLoad -= (largerCachedObjects.size() - cachedObjects.size()) * ranges.size();
            }
        }

        return deltaLoad / cachedObjects.size();
    }
}
