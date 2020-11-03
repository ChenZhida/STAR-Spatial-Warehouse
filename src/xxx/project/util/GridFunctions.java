package xxx.project.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class GridFunctions {
    public static int getKeyCell(double minLat, double minLon, double maxLat, double maxLon,
                                 int numLatCells, int numLonCells, Tuple2<Double, Double> coord) {
        double unitLat = (maxLat - minLat) / numLatCells;
        double unitLon = (maxLon - minLon) / numLonCells;
        double lat = coord._1();
        double lon = coord._2();

        int indexLat = (int)((lat - minLat) / unitLat);
        int indexLon = (int)((lon - minLon) / unitLon);

        return (indexLat << 16) + indexLon;
    }

    public static int getKeyCellByIndexes(int indexLat, int indexLon) {
        return (indexLat << 16) + indexLon;
    }

    public static List<Integer> getKeysCellsByRange(double minLat, double minLon, double maxLat, double maxLon,
                                                    int numLatCells, int numLonCells,
                                                    Tuple4<Double, Double, Double, Double> range) {
        double latFrom = range._1();
        double lonFrom = range._2();
        double latTo = range._3();
        double lonTo = range._4();
        return getKeysCellsByRange(minLat, minLon, maxLat, maxLon, numLatCells, numLonCells,
                new Tuple2<>(latFrom, lonFrom), new Tuple2<>(latTo, lonTo));
    }

    public static List<Integer> getKeysCellsByRange(double minLat, double minLon, double maxLat, double maxLon,
                                                    int numLatCells, int numLonCells,
                                                    Tuple2<Double, Double> cornerFrom,
                                                    Tuple2<Double, Double> cornerTo) {
        double unitLat = (maxLat - minLat) / numLatCells;
        double unitLon = (maxLon - minLon) / numLonCells;

        double cornerFromLat = cornerFrom._1();
        double cornerFromLon = cornerFrom._2();
        double cornerToLat = cornerTo._1();
        double cornerToLon = cornerTo._2();

        List<Integer> keys = new ArrayList<>();
        int indexCornerFromLat = (int)((cornerFromLat - minLat) / unitLat);
        int indexCornerFromLon = (int)((cornerFromLon - minLon) / unitLon);
        int indexCornerToLat = (int)((cornerToLat - minLat) / unitLat);
        int indexCornerToLon = (int)((cornerToLon - minLon) / unitLon);

        for (int indexLat = indexCornerFromLat; indexLat <= indexCornerToLat; ++indexLat)
            for (int indexLon = indexCornerFromLon; indexLon <= indexCornerToLon; ++indexLon)
                keys.add((indexLat << 16) + indexLon);

        return keys;
    }

    public static List<Integer> getBorderKeysCellsByRange(double minLat, double minLon, double maxLat, double maxLon,
                                                          int numLatCells, int numLonCells,
                                                          Tuple2<Double, Double> cornerFrom,
                                                          Tuple2<Double, Double> cornerTo) {
        double unitLat = (maxLat - minLat) / numLatCells;
        double unitLon = (maxLon - minLon) / numLonCells;

        double cornerFromLat = cornerFrom._1();
        double cornerFromLon = cornerFrom._2();
        double cornerToLat = cornerTo._1();
        double cornerToLon = cornerTo._2();

        int indexCornerFromLat = (int)((cornerFromLat - minLat) / unitLat);
        int indexCornerFromLon = (int)((cornerFromLon - minLon) / unitLon);
        int indexCornerToLat = (int)((cornerToLat - minLat) / unitLat);
        int indexCornerToLon = (int)((cornerToLon - minLon) / unitLon);

        HashSet<Integer> keys = new HashSet<>();
        for (int indexLat = indexCornerFromLat; indexLat <= indexCornerToLat; ++indexLat)
            for (int indexLon = indexCornerFromLon; indexLon <= indexCornerToLon; ++indexLon) {
                if (indexLat == indexCornerFromLat || indexLon == indexCornerFromLon ||
                        indexLat == indexCornerToLat || indexLon == indexCornerToLon) {
                    int keyCell = (indexLat << 16) + indexLon;
                    keys.add(keyCell);
                }
            }

        return new ArrayList<>(keys);
    }

    public static List<Integer> getKeysCellsInsideRange(double minLat, double minLon, double maxLat, double maxLon,
                                                        int numLatCells, int numLonCells,
                                                        Tuple2<Double, Double> cornerFrom,
                                                        Tuple2<Double, Double> cornerTo) {
        List<Integer> keysCellsRange = getKeysCellsByRange(minLat, minLon, maxLat, maxLon,
                numLatCells, numLonCells, cornerFrom, cornerTo);
        List<Integer> keysCellsBorder = getBorderKeysCellsByRange(minLat, minLon, maxLat, maxLon,
                numLatCells, numLonCells, cornerFrom, cornerTo);

        return keysCellsRange.stream().filter(k -> !keysCellsBorder.contains(k)).collect(Collectors.toList());
    }

    public static List<Integer> getKeysCellsInsideRange(double minLat, double minLon, double maxLat, double maxLon,
                                                        int numLatCells, int numLonCells,
                                                        Tuple4<Double, Double, Double, Double> range) {
        Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
        Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());

        return getKeysCellsInsideRange(minLat, minLon, maxLat, maxLon, numLatCells, numLonCells, cornerFrom, cornerTo);
    }

    public static Tuple4<Double, Double, Double, Double> getCellRangeByKey(double minLat, double minLon,
                                                                           double maxLat, double maxLon,
                                                                           int numLatCells, int numLonCells,
                                                                           int key) {
        int indexLat = (key & (65535 << 16)) >> 16;
        int indexLon = key & 65535;
        double unitLat = (maxLat - minLat) / numLatCells;
        double unitLon = (maxLon - minLon) / numLonCells;

        double latFrom = minLat + indexLat * unitLat;
        double lonFrom = minLon + indexLon * unitLon;
        double latTo = latFrom + unitLat;
        double lonTo = lonFrom + unitLon;

        return new Tuple4<>(latFrom, lonFrom, latTo, lonTo);
    }

    public static boolean isInside(Tuple2<Double, Double> p,
                                   Tuple2<Double, Double> cornerFrom,
                                   Tuple2<Double, Double> cornerTo) {
        return p._1() >= cornerFrom._1() &&
                p._2() >= cornerFrom._2() &&
                p._1() < cornerTo._1() &&
                p._2() < cornerTo._2();
    }
}
