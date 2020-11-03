package xxx.project.worker.index.grid;

import xxx.project.util.Constants;
import xxx.project.util.GridFunctions;
import xxx.project.util.SpatioTextualObject;
import xxx.project.util.Tuple2;
import xxx.project.worker.index.InvertedIndex;
import xxx.project.worker.index.RawDataInterface;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RawData implements RawDataInterface, Serializable {
    public static final long serialVersionUID = 104L;

    class GridIndex {
        private double minLat;
        private double minLon;
        private double maxLat;
        private double maxLon;
        private int numLatCells;
        private int numLonCells;
        private HashMap<Integer, InvertedIndex> cell2InvertedIndex = new HashMap<>();

        private double unitLat;
        private double unitLon;
        private int numObjects = 0;

        public GridIndex(double minLat, double minLon, double maxLat, double maxLon, int numLatCells, int numLonCells) {
            this.minLat = minLat;
            this.minLon = minLon;
            this.maxLat = maxLat;
            this.maxLon = maxLon;
            this.numLatCells = numLatCells;
            this.numLonCells = numLonCells;

            this.unitLat = (this.maxLat - this.minLat) / this.numLatCells;
            this.unitLon = (this.maxLon - this.minLon) / this.numLonCells;
        }

        public void addObject(SpatioTextualObject object) {
            ++numObjects;
            int keyCell = GridFunctions.getKeyCell(minLat, minLon, maxLat, maxLon,
                    numLatCells, numLonCells, object.getCoord());
            InvertedIndex invertedIndex = cell2InvertedIndex.get(keyCell);

            if (invertedIndex == null) {
                invertedIndex = new InvertedIndex();
                cell2InvertedIndex.put(keyCell, invertedIndex);
            }
            invertedIndex.addObject(object);
        }

        public Optional<List<SpatioTextualObject>> getObjects() {
            List<SpatioTextualObject> empty = new ArrayList<>();
            List<SpatioTextualObject> objects = new ArrayList<>();
            cell2InvertedIndex.values().stream()
                    .forEach(e -> objects.addAll(e.getObjects().orElse(empty)));

            return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
        }

        public Optional<List<SpatioTextualObject>> getObjectsByRange(Tuple2<Double, Double> cornerFrom,
                                                                     Tuple2<Double, Double> cornerTo) {
            double cornerFromLat = cornerFrom._1();
            double cornerFromLon = cornerFrom._2();
            double cornerToLat = cornerTo._1();
            double cornerToLon = cornerTo._2();
            List<SpatioTextualObject> objects = new ArrayList<>();

            for (double lat = cornerFromLat; lat <= cornerToLat; lat += unitLat) {
                for (double lon = cornerFromLon; lon <= cornerToLon; lon += unitLon) {
                    Tuple2<Double, Double> coord = new Tuple2<>(lat, lon);
                    int keyCell = GridFunctions.getKeyCell(minLat, minLon, maxLat, maxLon, numLatCells, numLonCells, coord);
                    InvertedIndex invertedIndex = cell2InvertedIndex.get(keyCell);
                    if (invertedIndex != null) {
                        if (Math.abs(lat - cornerFromLat) < 0.000000001 ||
                                Math.abs(lon - cornerFromLon) < 0.000000001 ||
                                Math.abs(lat - cornerToLat) < 0.000000001 ||
                                Math.abs(lon - cornerToLon) < 0.000000001) {
                            List<SpatioTextualObject> insideObjects =
                                    invertedIndex.getObjects().orElse(new ArrayList<>())
                                            .stream().filter(o -> {
                                        double oLat = o.getCoord()._1();
                                        double oLon = o.getCoord()._2();
                                        return oLat >= cornerFromLat && oLat < cornerToLat &&
                                                oLon >= cornerFromLon && oLon < cornerToLon;
                                    }).collect(Collectors.toList());
                            objects.addAll(insideObjects);
                        } else
                            objects.addAll(invertedIndex.getObjects().orElse(new ArrayList<>()));
                    }
                }
            }

            return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
        }

        public Optional<List<SpatioTextualObject>> getObjectsByTerms(Set<String> terms) {
            List<SpatioTextualObject> objects = new ArrayList<>();
            cell2InvertedIndex.values().forEach(invertedIndex -> {
                Optional<List<SpatioTextualObject>> objsByTerms = invertedIndex.getObjectsByTerms(terms);
                if (objsByTerms.isPresent())
                    objects.addAll(objsByTerms.get());
            });

            return objects.isEmpty() ? Optional.empty() : Optional.of(objects);

        }

        public Optional<List<SpatioTextualObject>> getObjectsByRangeTerms(Tuple2<Double, Double> cornerFrom,
                                                                          Tuple2<Double, Double> cornerTo,
                                                                          Set<String> terms) {
            double cornerFromLat = cornerFrom._1();
            double cornerFromLon = cornerFrom._2();
            double cornerToLat = cornerTo._1();
            double cornerToLon = cornerTo._2();
            List<SpatioTextualObject> objects = new ArrayList<>();

            for (double lat = cornerFromLat; lat <= cornerToLat; lat += unitLat) {
                for (double lon = cornerFromLon; lon <= cornerToLon; lon += unitLon) {
                    Tuple2<Double, Double> coord = new Tuple2<>(lat, lon);
                    int keyCell = GridFunctions.getKeyCell(minLat, minLon, maxLat, maxLon, numLatCells, numLonCells, coord);
                    InvertedIndex invertedIndex = cell2InvertedIndex.get(keyCell);
                    if (invertedIndex != null) {
                        List<SpatioTextualObject> objectsByTerms =
                                invertedIndex.getObjectsByTerms(terms).orElse(new ArrayList<>());
                        if (Math.abs(lat - cornerFromLat) < 0.000000001 ||
                                Math.abs(lon - cornerFromLon) < 0.000000001 ||
                                Math.abs(lat - cornerToLat) < 0.000000001 ||
                                Math.abs(lon - cornerToLon) < 0.000000001) {
                            List<SpatioTextualObject> insideObjects =
                                    objectsByTerms.stream().filter(o -> {
                                        double oLat = o.getCoord()._1();
                                        double oLon = o.getCoord()._2();
                                        return oLat >= cornerFromLat && oLat < cornerToLat &&
                                                oLon >= cornerFromLon && oLon < cornerToLon;
                                    }).collect(Collectors.toList());
                            objects.addAll(insideObjects);
                        } else
                            objects.addAll(objectsByTerms);
                    }
                }
            }

            return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
        }

        public Optional<List<SpatioTextualObject>> getObjectsByCell(int cell) {
            InvertedIndex invertedIndex = cell2InvertedIndex.get(cell);
            return invertedIndex == null ? Optional.empty() : invertedIndex.getObjects();
        }

        public int getNumObjects() {
            return numObjects;
        }
    }

    private HashMap<Integer, GridIndex> hour2GridIndex = new HashMap<>();

    public static HashSet<String> stopwords;
    static {
        stopwords = new HashSet<>(Arrays.asList(Constants.STOPWORDS.split(",")));
    }

    public void addObjects(List<SpatioTextualObject> objects) {
        for (SpatioTextualObject object : objects)
            addObject(object);
    }

    public void addObject(SpatioTextualObject object) {
        String hour = object.getHour();
        int key = Integer.valueOf(hour);
        GridIndex gridIndex = hour2GridIndex.get(key);

        if (gridIndex == null) {
            gridIndex = new GridIndex(Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON,
                    Constants.GRID_NUM_LAT_CELLS, Constants.GRID_NUM_LON_CELLS);
            hour2GridIndex.put(key, gridIndex);
        }
        gridIndex.addObject(object);
    }

    public Optional<List<SpatioTextualObject>> getObjects() {
        List<SpatioTextualObject> objects = new ArrayList<>();
        for (GridIndex gridIndex : hour2GridIndex.values())
            objects.addAll(gridIndex.getObjects().orElse(new ArrayList<>()));

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByTime(String startTime, String endTime) {
        int start = Integer.valueOf(startTime);
        int end = Integer.valueOf(endTime);
        List<SpatioTextualObject> objects = new ArrayList<>();

        IntStream.rangeClosed(start, end).forEach(t -> {
            GridIndex gridIndex = hour2GridIndex.get(t);
            if (gridIndex != null) {
                Optional<List<SpatioTextualObject>> objsGrid =
                        gridIndex.getObjects();
                if (objsGrid.isPresent())
                    objects.addAll(objsGrid.get());
            }
        });

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByRange(Tuple2<Double, Double> cornerFrom,
                                                                 Tuple2<Double, Double> cornerTo) {
        List<SpatioTextualObject> objects = new ArrayList<>();
        for (GridIndex gridIndex : hour2GridIndex.values())
            objects.addAll(gridIndex.getObjectsByRange(cornerFrom, cornerTo).orElse(new ArrayList<>()));

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByTerms(Set<String> terms) {
        List<SpatioTextualObject> objects = new ArrayList<>();
        for (GridIndex gridIndex : hour2GridIndex.values())
            objects.addAll(gridIndex.getObjectsByTerms(terms).orElse(new ArrayList<>()));

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByRangeTerms(Tuple2<Double, Double> cornerFrom,
                                                                         Tuple2<Double, Double> cornerTo,
                                                                         Set<String> terms) {
        List<SpatioTextualObject> objects = new ArrayList<>();
        for (GridIndex gridIndex : hour2GridIndex.values())
            objects.addAll(gridIndex.getObjectsByRangeTerms(cornerFrom, cornerTo, terms).orElse(new ArrayList<>()));

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByTimeRange(String startTime, String endTime,
                                                                     Tuple2<Double, Double> cornerFrom,
                                                                     Tuple2<Double, Double> cornerTo) {
        int start = Integer.valueOf(startTime);
        int end = Integer.valueOf(endTime);
        List<SpatioTextualObject> objects = new ArrayList<>();

        IntStream.rangeClosed(start, end).forEach(t -> {
            GridIndex gridIndex = hour2GridIndex.get(t);
            if (gridIndex != null) {
                Optional<List<SpatioTextualObject>> objsRange =
                        gridIndex.getObjectsByRange(cornerFrom, cornerTo);
                if (objsRange.isPresent())
                    objects.addAll(objsRange.get());
            }
        });

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByTimeTerms(String startTime, String endTime,
                                                                     Set<String> terms) {
        int start = Integer.valueOf(startTime);
        int end = Integer.valueOf(endTime);
        List<SpatioTextualObject> objects = new ArrayList<>();

        IntStream.rangeClosed(start, end).forEach(t -> {
            GridIndex gridIndex = hour2GridIndex.get(t);
            if (gridIndex != null) {
                Optional<List<SpatioTextualObject>> objsTerms =
                        gridIndex.getObjectsByTerms(terms);
                if (objsTerms.isPresent())
                    objects.addAll(objsTerms.get());
            }
        });

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByTimeRangeTerms(String startTime, String endTime,
                                                                          Tuple2<Double, Double> cornerFrom,
                                                                          Tuple2<Double, Double> cornerTo,
                                                                          Set<String> terms) {
        int start = Integer.valueOf(startTime);
        int end = Integer.valueOf(endTime);
        List<SpatioTextualObject> objects = new ArrayList<>();

        IntStream.rangeClosed(start, end).forEach(t -> {
            GridIndex gridIndex = hour2GridIndex.get(t);
            if (gridIndex != null) {
                Optional<List<SpatioTextualObject>> objsRangeTerms =
                        gridIndex.getObjectsByRangeTerms(cornerFrom, cornerTo, terms);
                if (objsRangeTerms.isPresent())
                    objects.addAll(objsRangeTerms.get());
            }
        });

        return objects.isEmpty() ? Optional.empty() : Optional.of(objects);
    }

    public Optional<List<SpatioTextualObject>> getObjectsByHourCell(String hour, int cell) {
        GridIndex gridIndex = hour2GridIndex.get(Integer.valueOf(hour));
        return gridIndex == null ? Optional.empty() : gridIndex.getObjectsByCell(cell);
    }

    public int getNumObjectsByHourCell(String hour, int cell) {
        Optional<List<SpatioTextualObject>> result = getObjectsByHourCell(hour, cell);
        return result.isPresent() ? result.get().size() : 0;
    }

    public void removeDataAtHour(String hour) {
        hour2GridIndex.remove(Integer.valueOf(hour));
    }

    public int getNumObjects() {
        int numObjects = 0;
        for (GridIndex gridIndex : hour2GridIndex.values())
            numObjects += gridIndex.getNumObjects();

        return numObjects;
    }

    public List<String> getHours() {
        return hour2GridIndex.keySet().stream().map(hour -> String.valueOf(hour)).collect(Collectors.toList());
    }
}
