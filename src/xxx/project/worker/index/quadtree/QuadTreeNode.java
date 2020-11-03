package xxx.project.worker.index.quadtree;

import xxx.project.util.Constants;
import xxx.project.util.Tuple2;
import xxx.project.util.Tuple4;

import java.io.Serializable;

public class QuadTreeNode implements Cloneable, Serializable {
    public static final long serialVersionUID = 101L;

    public int level;
    public double minLat;
    public double minLon;
    public double maxLat;
    public double maxLon;

    private int indexLat;
    private int indexLon;

    public QuadTreeNode(int level, double minLat, double minLon, double maxLat, double maxLon) {
        this.level = level;
        this.minLat = minLat;
        this.minLon = minLon;
        this.maxLat = maxLat;
        this.maxLon = maxLon;

        double numSideCells = Math.pow(2, level);
        double unitLat = (Constants.MAX_LAT - Constants.MIN_LAT) / numSideCells;
        double unitLon = (Constants.MAX_LON - Constants.MIN_LON) / numSideCells;
        indexLat = (int)((minLat - Constants.MIN_LAT) / unitLat);
        indexLon = (int)((minLon - Constants.MIN_LON) / unitLon);
    }

    public QuadTreeNode getCopy() {
        return new QuadTreeNode(level, minLat, minLon, maxLat, maxLon);
    }

    public Object clone() {
        QuadTreeNode o = null;
        try {
            o = (QuadTreeNode)super.clone();
        } catch (CloneNotSupportedException e) {
            System.out.println(e.toString());
        }

        return o;
    }

    public boolean isOverlappedNodeRange(Tuple2<Double, Double> cornerFrom,
                                         Tuple2<Double, Double> cornerTo) {
        double latFrom = cornerFrom._1();
        double lonFrom = cornerFrom._2();
        double latTo = cornerTo._1();
        double lonTo = cornerTo._2();
        boolean nonOverlapped = latTo <= minLat || latFrom >= maxLat ||
                lonTo <= minLon || lonFrom >= maxLon;

        return !nonOverlapped;
    }

    public boolean isOverlappedNodeRange(Tuple4<Double, Double, Double, Double> range) {
        Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
        Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());

        return isOverlappedNodeRange(cornerFrom, cornerTo);
    }

    public boolean isEnclosingNodeRange(Tuple2<Double, Double> cornerFrom,
                                        Tuple2<Double, Double> cornerTo) {
        double latFrom = cornerFrom._1();
        double lonFrom = cornerFrom._2();
        double latTo = cornerTo._1();
        double lonTo = cornerTo._2();

        return latFrom <= minLat && latTo >= maxLat &&
                lonFrom <= minLon && lonTo >= maxLon;
    }

    public boolean isEnclosingNodeRange(Tuple4<Double, Double, Double, Double> range) {
        Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
        Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());

        return isEnclosingNodeRange(cornerFrom, cornerTo);
    }

    public boolean isInsideNodeRange(Tuple2<Double, Double> coord) {
        double lat = coord._1();
        double lon = coord._2();

        return minLat <= lat && maxLat > lat &&
                minLon <= lon && maxLon > lon;
    }

    public boolean isAncestorNode(QuadTreeNode ancestor) {
        return ancestor.level < level &&
                ancestor.minLat <= minLat &&
                ancestor.minLon <= minLon &&
                ancestor.maxLat >= maxLat &&
                ancestor.maxLon >= maxLon;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;

        QuadTreeNode that = (QuadTreeNode) o;

        if (level != that.level) return false;
        if (indexLat != that.indexLat) return false;
        return indexLon == that.indexLon;
    }

    @Override
    public int hashCode() {
        int result = level;
        result = 31 * result + indexLat;
        result = 31 * result + indexLon;
        return result;
    }
}
