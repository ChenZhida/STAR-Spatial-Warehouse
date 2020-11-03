package xxx.project.worker.index.grid;

import xxx.project.util.*;
import xxx.project.worker.index.Index;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class GridCubes implements Index, Serializable {
    private class GridCube implements Serializable {
        public String hour;
        public String group;
        public HashSet<Integer> materializedCells;
        public double minLat;
        public double minLon;
        public double maxLat;
        public double maxLon;
        public int numLatCells;
        public int numLonCells;

        public HashMap<Integer, HashMap<String, Integer>> cell2Map =
                new HashMap<>();
        protected HashMap<String, Integer> globalDimValue2Count =
                new HashMap<>();

        public GridCube(String hour, String group, List<Integer> materializedCells,
                        double minLat, double minLon, double maxLat, double maxLon,
                        int numLatCells, int numLonCells) {
            this.hour = hour;
            this.group = group;
            this.materializedCells = new HashSet<>(materializedCells);
            this.minLat = minLat;
            this.minLon = minLon;
            this.maxLat = maxLat;
            this.maxLon = maxLon;
            this.numLatCells = numLatCells;
            this.numLonCells = numLonCells;
        }

        public int getCardinality() {
            return globalDimValue2Count.size();
        }

        public int getCardinalityByCell(int cell) {
            HashMap<String, Integer> map = cell2Map.get(cell);
            return map == null ? 0 : map.size();
        }

        public void updateCube(SpatioTextualObject object) {
            String combinedDimValue = object.getGroupBy(group);
            Integer count = globalDimValue2Count.getOrDefault(combinedDimValue, 0);
            globalDimValue2Count.put(combinedDimValue, count + 1);

            int keyCell = GridFunctions.getKeyCell(minLat, minLon, maxLat, maxLon, numLatCells, numLonCells, object.getCoord());
            if (materializedCells.contains(keyCell)) {
                HashMap<String, Integer> map = cell2Map.get(keyCell);
                if (map == null) {
                    map = new HashMap<>();
                    cell2Map.put(keyCell, map);
                }
                combinedDimValue = object.getGroupBy(group);
                count = map.getOrDefault(combinedDimValue, 0);
                map.put(combinedDimValue, count + 1);
            }
        }

        public void getCountAggregation(Map<String, Integer> result) {
            for (Map.Entry<String, Integer> entry : globalDimValue2Count.entrySet()) {
                String key = entry.getKey();
                int val = entry.getValue();
                int count = result.getOrDefault(key, 0) + val;
                result.put(key, count);
            }
        }

        public void getCountAggregationByRange(RawData rawData,
                                               Tuple2<Double, Double> cornerFrom,
                                               Tuple2<Double, Double> cornerTo,
                                               Map<String, Integer> result) {
            List<Integer> borders = GridFunctions.getBorderKeysCellsByRange(
                    minLat, minLon, maxLat, maxLon, numLatCells, numLonCells, cornerFrom, cornerTo);

            List<Integer> keys = GridFunctions
                    .getKeysCellsByRange(minLat, minLon, maxLat, maxLon, numLatCells, numLonCells, cornerFrom, cornerTo);
            List<Integer> usedKeys = new ArrayList<>();
            List<Integer> unusedKeys = new ArrayList();
            keys.forEach(k -> {
                if (!borders.contains(k)) {
                    if (materializedCells.contains(k))
                        usedKeys.add(k);
                    else
                        unusedKeys.add(k);
                }
            });

            List<HashMap<String, Integer>> mapList = usedKeys.stream()
                    .map(k -> cell2Map.get(k)).filter(map -> map != null).collect(Collectors.toList());
            HashMap<String, Integer> resultMap = new HashMap<>();
            mapList.forEach(map -> {
                map.forEach((k, v) -> {
                    int count = resultMap.getOrDefault(k, 0);
                    resultMap.put(k, count + v);
                });
            });

            unusedKeys.forEach(k -> {
                List<SpatioTextualObject> objects = rawData.getObjectsByHourCell(hour, k).orElse(new ArrayList<>());
                objects.forEach(o -> {
                    String combinedDimValue = o.getGroupBy(group);
                    int count = resultMap.getOrDefault(combinedDimValue, 0);
                    resultMap.put(combinedDimValue, count + 1);
                });
            });

            borders.forEach(k -> {
                List<SpatioTextualObject> objects = rawData.getObjectsByHourCell(hour, k).orElse(new ArrayList<>())
                        .stream()
                        .filter(o -> GridFunctions.isInside(o.getCoord(), cornerFrom, cornerTo))
                        .collect(Collectors.toList());
                objects.forEach(o -> {
                    String combinedDimValue = o.getGroupBy(group);
                    int count = resultMap.getOrDefault(combinedDimValue, 0);
                    resultMap.put(combinedDimValue, count + 1);
                });
            });

            for (Map.Entry<String, Integer> entry : resultMap.entrySet()) {
                String key = entry.getKey();
                int val = entry.getValue();
                int count = result.getOrDefault(key, 0) + val;
                result.put(key, count);
            }
        }

    }
    private class AggregationDependency implements Serializable {
        private HashMap<String, String> city2Country = new HashMap<>();

        private List<String> spaceDimension = Arrays.asList("city", "country");
        private List<String> timeDimension = Arrays.asList("hour", "day", "week", "month");
        private List<String> textDimension = Arrays.asList("topic");

        public void update(SpatioTextualObject object) {
            String city = object.getCity();
            String country = object.getCountry();
            city2Country.put(city, country);
        }

        public void getAggregationFromAncestor(HashMap<String, Integer> resultMap,
                                               HashMap<String, Integer> ancestorMap,
                                               String ancestorGroup,
                                               String group) {
            if (group.equals("none")) {
                for (Map.Entry<String, Integer> entry : ancestorMap.entrySet()) {
                    int count = resultMap.getOrDefault("none", 0);
                    resultMap.put("none", count + entry.getValue());
                }
                return;
            }

            String[] attrsAncestor = ancestorGroup.split(" ");
            String[] attrsChild = group.split(" ");
            String[] attrsAn = {"", "", ""};
            String[] attrsCh = {"", "", ""};
            for (String attr : attrsAncestor) {
                if (spaceDimension.contains(attr))
                    attrsAn[0] = attr;
                else if (timeDimension.contains(attr))
                    attrsAn[1] = attr;
                else if (textDimension.contains(attr))
                    attrsAn[2] = attr;
            }
            for (String attr : attrsChild) {
                if (spaceDimension.contains(attr))
                    attrsCh[0] = attr;
                else if (timeDimension.contains(attr))
                    attrsCh[1] = attr;
                else if (textDimension.contains(attr))
                    attrsCh[2] = attr;
            }

            int[] idxesAn = {-1, -1, -1};
            if (!attrsAn[0].isEmpty()) {
                idxesAn[0] = 0;
                if (!attrsAn[1].isEmpty()) {
                    idxesAn[1] = 1;
                    if (!attrsAn[2].isEmpty())
                        idxesAn[2] = 2;
                } else if (!attrsAn[2].isEmpty())
                    idxesAn[2] = 1;
            }
            if (attrsAn[0].isEmpty() && !attrsAn[1].isEmpty()) {
                idxesAn[1] = 0;
                if (!attrsAn[2].isEmpty())
                    idxesAn[2] = 1;
            }
            if (attrsAn[0].isEmpty() && attrsAn[1].isEmpty() && !attrsAn[2].isEmpty())
                idxesAn[2] = 0;

            // 0: "", 1: same as the ancestor, 2: obtained from the ancestor
            int flagChildSpace = 0, flagChildTime = 0, flagChildText = 0;
            int endPosTime = 0;

            if (attrsAn[0].equals(attrsCh[0]))
                flagChildSpace = 1;
            else if (attrsAn[0].equals("city") && attrsCh[0].equals("country"))
                flagChildSpace = 2;

            if (attrsAn[1].equals(attrsCh[1]))
                flagChildTime = 1;
            else if (attrsAn[1].equals("hour") && attrsCh[1].equals("day")) {
                flagChildTime = 2;
                endPosTime = 8;
            } else if (attrsAn[1].equals("hour") && attrsCh[1].equals("week")) {
                flagChildTime = 2;
                endPosTime = -1;
            } else if (attrsAn[1].equals("hour") && attrsCh[1].equals("month")) {
                flagChildTime = 2;
                endPosTime = 6;
            } else if (attrsAn[1].equals("day") && attrsCh[1].equals("week")) {
                flagChildTime = 2;
                endPosTime = -1;
            } else if (attrsAn[1].equals("day") && attrsCh[1].equals("month")) {
                flagChildTime = 2;
                endPosTime = 6;
            }

            if (attrsAn[2].equals(attrsCh[2]))
                flagChildText = 1;

            for (Map.Entry<String, Integer> entry : ancestorMap.entrySet()) {
                String[] splitList = entry.getKey().split(SpatioTextualObject.DIMENSION_SEPARATOR);
                String ancestorSpace = idxesAn[0] >= 0 ? splitList[idxesAn[0]] : "";
                String ancestorTime = idxesAn[1] >= 0 ? splitList[idxesAn[1]] : "";
                String ancestorText = idxesAn[2] >= 0 ? splitList[idxesAn[2]] : "";

                String childSpace = "", childTime = "", childText = "";

                if (flagChildSpace == 1)
                    childSpace = ancestorSpace;
                else if (flagChildSpace == 2)
                    childSpace = getCountryFromCity(ancestorSpace);

                if (flagChildTime == 1)
                    childTime = ancestorTime;
                else if (flagChildTime == 2) {
                    if (endPosTime > 0)
                        childTime = ancestorTime.substring(0, endPosTime);
                    else {
                        String year = ancestorTime.substring(0, 4);
                        String month = ancestorTime.substring(4, 6);
                        String formattedDate = year + "-" + month + "-" + ancestorTime.substring(6, 8) + " 00:00:00";

                        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        LocalDateTime dateTime = LocalDateTime.parse(formattedDate, format);
                        TemporalField weekOfYear = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear();
                        String week = year + String.valueOf(dateTime.get(weekOfYear));
                        childTime = week;
                    }
                }

                if (flagChildText == 1)
                    childText = ancestorText;

                final String separator = SpatioTextualObject.DIMENSION_SEPARATOR;
                StringBuilder childKey = new StringBuilder();
                if (!childSpace.isEmpty())
                    childKey.append(childSpace);
                if (!childTime.isEmpty()) {
                    if (childKey.length() != 0)
                        childKey.append(separator + childTime);
                    else
                        childKey.append(childTime);
                }
                if (!childText.isEmpty()) {
                    if (childKey.length() != 0)
                        childKey.append(separator + childText);
                    else
                        childKey.append(childText);
                }

                int count = resultMap.getOrDefault(childKey.toString(), 0);
                resultMap.put(childKey.toString(), count + entry.getValue());
            }
        }

        private String getCountryFromCity(String city) {
            return city2Country.get(city);
        }

        private String getDayFromHour(String hour) {
            return hour.substring(0, 8);
        }

        private String getWeekFromHour(String hour) {
            return getWeekFromDay(hour.substring(0, 8));
        }

        private String getMonthFromHour(String hour) {
            return hour.substring(0, 6);
        }

        private String getMonthFromDay(String day) {
            return day.substring(0, 6);
        }

        private String getWeekFromDay(String day) {
            String year = day.substring(0, 4);
            String month = day.substring(4, 6);
            String formattedDate = year + "-" + month + "-" + day.substring(6, 8) + " 00:00:00";

            DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime dateTime = LocalDateTime.parse(formattedDate, format);
            TemporalField weekOfYear = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear();
            String week = year + String.valueOf(dateTime.get(weekOfYear));

            return week;
        }
    }
    private class CellStat {
        public int numObjects = 0;
        public int numQueries = 0;
        public int cardinality = 0;

        public CellStat(int numObjects, int numQueries, int cardinality) {
            this.numObjects = numObjects;
            this.numQueries = numQueries;
            this.cardinality = cardinality;
        }
    }
    private class Stats {
        private class Stat {
            private int numObjects = 0;
            private int numQueries = 0;
            private HashMap<String, HashSet<String>> group2DimValues = new HashMap<>();

            public void incrementObject() {
                ++numObjects;
            }

            public void incrementQuery() {
                ++numQueries;
            }

            public void addDimValue(String group, String dimValue) {
                HashSet<String> dimValues = group2DimValues.get(group);
                if (dimValues == null) {
                    dimValues = new HashSet<>();
                    group2DimValues.put(group, dimValues);
                }
                dimValues.add(dimValue);
            }

            public int getCardinality(String group) {
                HashSet<String> dimValues = group2DimValues.get(group);
                return dimValues == null ? 0 : dimValues.size();
            }
        }

        private double minLat;
        private double minLon;
        private double maxLat;
        private double maxLon;
        private int numLatCells;
        private int numLonCells;
        private HashMap<Integer, Stat> cell2Stat = new HashMap<>();
        private HashMap<String, HashSet<String>> globalGroup2DimValues = new HashMap<>();
        private List<String> groups;

        public Stats(double minLat, double minLon, double maxLat, double maxLon, int numLatCells, int numLonCells,
                     List<SpatioTextualObject> objects, Map<String, List<WarehouseQuery>> log) {
            this.minLat = minLat;
            this.minLon = minLon;
            this.maxLat = maxLat;
            this.maxLon = maxLon;
            this.numLatCells = numLatCells;
            this.numLonCells = numLonCells;
            setStats(objects, log);
        }

        public List<String> getGroups() {
            return groups;
        }

        public CellStat getStat(int cell, String group) {
            Stat stat = cell2Stat.get(cell);
            return new CellStat(stat.numObjects, stat.numQueries, stat.getCardinality(group));
        }

        public int getGlobalCardinality(String group) {
            return globalGroup2DimValues.get(group).size();
        }

        public int getNumObjectsByCell(int cell) {
            return cell2Stat.get(cell).numObjects;
        }

        private void setStats(List<SpatioTextualObject> objects, Map<String, List<WarehouseQuery>> log) {
            groups = new ArrayList<>(log.keySet());
            for (SpatioTextualObject object : objects) {
                int cell = GridFunctions.getKeyCell(
                        minLat, minLon, maxLat, maxLon, numLatCells, numLonCells, object.getCoord());
                Stat stat = cell2Stat.get(cell);
                if (stat == null) {
                    stat = new Stat();
                    cell2Stat.put(cell, stat);
                }

                stat.incrementObject();
                for (String group : groups) {
                    stat.addDimValue(group, object.getGroupBy(group));

                    HashSet<String> globalDimValues = globalGroup2DimValues.get(group);
                    if (globalDimValues == null) {
                        globalDimValues = new HashSet<>();
                        globalGroup2DimValues.put(group, globalDimValues);
                    }
                    globalDimValues.add(object.getGroupBy(group));
                }
            }

            for (Map.Entry<String, List<WarehouseQuery>> entry : log.entrySet()) {
                for (WarehouseQuery query : entry.getValue()) {
                    List<Integer> cells = GridFunctions.getKeysCellsByRange(minLat, minLon, maxLat, maxLon,
                            numLatCells, numLonCells, query.getRange().get());
                    for (Integer cell : cells) {
                        Stat stat = cell2Stat.get(cell);
                        if (stat == null) {
                            stat = new Stat();
                            cell2Stat.put(cell, stat);
                        }
                        stat.incrementQuery();
                    }
                }
            }
        }
    }

    private RawData rawData = new RawData();
    private List<Tuple2<String, Integer>> materializedPairsOfGroupAndCell = null;
    private HashMap<Integer, HashMap<String, GridCube>> hour2Cubes = new HashMap<>();
    private AggregationDependency dependency = new AggregationDependency();

    private static List<Tuple2<String, String>> spaceDependencies;
    private static List<Tuple2<String, String>> timeDependencies;
    private static List<Tuple2<String, String>> textDependencies;
    public static List<Tuple2<String, String>> groupDependencies;
    public static HashMap<String, Integer> group2Level;
    static {
        setSpaceDependencies();
        setTimeDependencies();
        setTextDependencies();
        setGroupDependenciesAndGroup2Level();
    }

    // For performance evaluation
    public static long timeReadCubes = 0L;
    public static long timeReadRawData = 0L;
    public static long numReadedObjectsInRawData = 0L;
    public static long numObjectsForQueries = 0L;

    private static void setSpaceDependencies() {
        Tuple2<String, String> d1 = new Tuple2<>("city", "country");
        Tuple2<String, String> d2 = new Tuple2<>("country", "none");

        spaceDependencies = Arrays.asList(d1, d2);
    }

    private static void setTimeDependencies() {
        Tuple2<String, String> d1 = new Tuple2<>("hour", "day");
        Tuple2<String, String> d2 = new Tuple2<>("day", "week");
        Tuple2<String, String> d3 = new Tuple2<>("day", "month");
        Tuple2<String, String> d4 = new Tuple2<>("week", "none");
        Tuple2<String, String> d5 = new Tuple2<>("month", "none");

        timeDependencies = Arrays.asList(d1, d2, d3, d4, d5);
    }

    private static void setTextDependencies() {
        Tuple2<String, String> d1 = new Tuple2<>("topic", "none");

        textDependencies = Arrays.asList(d1);
    }

    private static void setGroupDependenciesAndGroup2Level() {
        Queue<String> groups2Visit = new LinkedList<>();
        List<Tuple2<String, String>> dependencies = new ArrayList<>();
        HashMap<String, Integer> group2LevelMap = new HashMap<>();
        HashSet<String> visitedGroups = new HashSet<>();

        String root = Constants.ROOT.replaceAll(" ", ",");
        groups2Visit.add(root);
        group2LevelMap.put(root, 0);
        setGroupDependenciesAndGroup2Level(groups2Visit, dependencies, group2LevelMap, visitedGroups);

        groupDependencies =
                dependencies.stream().map(d -> {
                    String from = d._1();
                    String to = d._2();
                    List<String> array1 = Arrays.asList(from, to);
                    List<String> array2 = new ArrayList<>();
                    array1.forEach(s -> {
                        String tmp =
                                s.replaceAll(",", " ")
                                        .replaceAll("none", " ")
                                        .trim()
                                        .replaceAll(" +", " ");
                        array2.add(tmp.isEmpty() ? "none" : tmp);
                    });

                    return new Tuple2<>(array2.get(0), array2.get(1));
                }).collect(Collectors.toList());

        group2Level = new HashMap<>();
        for (Map.Entry<String, Integer> entry : group2LevelMap.entrySet()) {
            String key = entry.getKey();
            int val = entry.getValue();
            key = key.replaceAll(",", " ")
                    .replaceAll("none", " ")
                    .trim()
                    .replaceAll(" +", " ");
            if (key.isEmpty())
                key = "none";
            group2Level.put(key, val);
        }
    }

    private static void setGroupDependenciesAndGroup2Level(Queue<String> groups2Visit,
                                                    List<Tuple2<String, String>> dependencies,
                                                    HashMap<String, Integer> group2LevelMap,
                                                    HashSet<String> visitedGroups) {
        String group = groups2Visit.poll();
        visitedGroups.add(group);
        String[] attrs = group.split(",");
        String spaceAttr = attrs[0];
        String timeAttr = attrs[1];
        String textAttr = attrs[2];

        List<String> spaceChildren = spaceDependencies.stream()
                .filter(e -> {
                    String from = e._1();
                    String to = e._2();
                    return from.equals(spaceAttr);
                })
                .map(e -> e._2())
                .collect(Collectors.toList());
        List<String> timeChildren = timeDependencies.stream()
                .filter(e -> {
                    String from = e._1();
                    String to = e._2();
                    return from.equals(timeAttr);
                })
                .map(e -> e._2())
                .collect(Collectors.toList());
        List<String> textChildren = textDependencies.stream()
                .filter(e -> {
                    String from = e._1();
                    String to = e._2();
                    return from.equals(textAttr);
                })
                .map(e -> e._2())
                .collect(Collectors.toList());

        List<String> children = new ArrayList<>();
        spaceChildren.forEach(space -> {
            String child = space + "," + timeAttr + "," + textAttr;
            children.add(child);
        });
        timeChildren.forEach(time -> {
            String label = spaceAttr + "," + time + "," + textAttr;
            children.add(label);
        });
        textChildren.forEach(text -> {
            String label = spaceAttr + "," + timeAttr + "," + text;
            children.add(label);
        });

        if (children.isEmpty())
            return;

        int level = group2LevelMap.get(group) + 1;
        children.forEach(c -> {
            if (!visitedGroups.contains(c))
                groups2Visit.add(c);

            Integer childLevel = group2LevelMap.get(c);
            if (childLevel == null || level < childLevel)
                group2LevelMap.put(c, level);

            Tuple2<String, String> dependency = new Tuple2<>(group, c);
            if (!dependencies.contains(dependency))
                dependencies.add(dependency);
        });

        setGroupDependenciesAndGroup2Level(groups2Visit, dependencies, group2LevelMap, visitedGroups);
    }

    public static boolean isAncestorOf(String ancestorGroup, String childGroup) {
        if (ancestorGroup.equals(childGroup))
            return false;
        else if (childGroup.equals("none"))
            return true;

        List<String> spaceDimension = Arrays.asList("city", "country");
        List<String> timeDimension = Arrays.asList("hour", "day", "week", "month");
        List<String> textDimension = Arrays.asList("topic");

        String[] attrsAncestor = ancestorGroup.split(" ");
        String[] attrsChild = childGroup.split(" ");

        List<String> parsedAttrsAncestor = Arrays.asList("", "", "");
        for (String attr : attrsAncestor) {
            if (spaceDimension.contains(attr))
                parsedAttrsAncestor.set(0, attr);
            else if (timeDimension.contains(attr))
                parsedAttrsAncestor.set(1, attr);
            else if (textDimension.contains(attr))
                parsedAttrsAncestor.set(2, attr);
        }

        List<String> parsedAttrsChild = Arrays.asList("", "", "");
        for (String attr : attrsChild) {
            if (spaceDimension.contains(attr))
                parsedAttrsChild.set(0, attr);
            else if (timeDimension.contains(attr))
                parsedAttrsChild.set(1, attr);
            else if (textDimension.contains(attr))
                parsedAttrsChild.set(2, attr);
        }

        String[] attrsAn = parsedAttrsAncestor.toArray(new String[0]);
        String[] attrsCh = parsedAttrsChild.toArray(new String[0]);

        if ((attrsAn[0].equals("") && attrsCh[0].equals("country")) ||
                (attrsAn[0].equals("") && attrsCh[0].equals("city")) ||
                (attrsAn[0].equals("country") && attrsCh[0].equals("city")))
            return false;
        if ((attrsAn[1].equals("") && attrsCh[1].equals("month")) ||
                (attrsAn[1].equals("") && attrsCh[1].equals("week")) ||
                (attrsAn[1].equals("") && attrsCh[1].equals("day")) ||
                (attrsAn[1].equals("") && attrsCh[1].equals("hour")) ||
                (attrsAn[1].equals("month") && attrsCh[1].equals("day")) ||
                (attrsAn[1].equals("month") && attrsCh[1].equals("hour")) ||
                (attrsAn[1].equals("week") && attrsCh[1].equals("day")) ||
                (attrsAn[1].equals("week") && attrsCh[1].equals("hour")) ||
                (attrsAn[1].equals("week") && attrsCh[1].equals("month")) ||
                (attrsAn[1].equals("month") && attrsCh[1].equals("week")) ||
                (attrsAn[1].equals("day") && attrsCh[1].equals("hour")))
            return false;
        if (attrsAn[2].equals("") && attrsCh[2].equals("topic"))
            return false;

        return true;
    }

    public void addObjects(List<SpatioTextualObject> objects) {
        for (SpatioTextualObject object : objects)
            addObject(object);
    }

    public void addObject(SpatioTextualObject object) {
        rawData.addObject(object);
        updateCubes(object);

        List<String> hours = rawData.getHours();
        if (hours.size() > Constants.NUM_HOURS_DATA_KEPT) {
            int oldestHour = Integer.MAX_VALUE;
            for (int i = 0; i < hours.size(); ++i) {
                int currentHour = Integer.valueOf(hours.get(i));
                if (currentHour < oldestHour)
                    oldestHour = currentHour;
            }
            rawData.removeDataAtHour(String.valueOf(oldestHour));
            removeCubesAtHour(String.valueOf(oldestHour));
        }
    }

    private void addCubesAtHour(String hour) {
        int hourInt = Integer.valueOf(hour);
        HashMap<String, List<Integer>> group2Cells = new HashMap<>();

        for (Tuple2<String, Integer> groupCell : materializedPairsOfGroupAndCell) {
            String group = groupCell._1();
            Integer cell = groupCell._2();

            List<Integer> cells = group2Cells.get(group);
            if (cells == null) {
                cells = new ArrayList<>();
                group2Cells.put(group, cells);
            }
            cells.add(cell);
        }

        HashMap<String, GridCube> cubes = new HashMap<>();
        for (Map.Entry<String, List<Integer>> group2CellsEntry : group2Cells.entrySet()) {
            String group = group2CellsEntry.getKey();
            List<Integer> cells = group2CellsEntry.getValue();
            GridCube gridCube = new GridCube(String.valueOf(hour), group, cells,
                    Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON,
                    Constants.GRID_NUM_LAT_CELLS, Constants.GRID_NUM_LON_CELLS);
            cubes.put(group, gridCube);
        }

        hour2Cubes.put(hourInt, cubes);
    }

    private void removeCubesAtHour(String hour) {
        hour2Cubes.remove(Integer.valueOf(hour));
    }

    public void materializeCubesAndWrite2File(
            List<SpatioTextualObject> objects, Map<String, List<WarehouseQuery>> log, String fileName) throws IOException {
        Stats stats = new Stats(Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON,
                Constants.GRID_NUM_LAT_CELLS, Constants.GRID_NUM_LON_CELLS, objects, log);
        List<Tuple2<String, Integer>> materializedCubes =
                materializeCubes(stats);

        FileOutputStream fileOut =
                new FileOutputStream(fileName);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(materializedCubes);
        out.close();
        fileOut.close();
        System.out.println("Materialized cubes are saved in " + fileName);
    }

    public List<Tuple2<String, Integer>> materializeCubes(Stats stats) {
        List<Tuple2<String, Integer>> materializedCubes = new ArrayList<>();

        Map<String, List<Integer>> groupAndCells = computeMaterializedCubes(stats);
        for (Map.Entry<String, List<Integer>> entry : groupAndCells.entrySet()) {
            String group = entry.getKey();
            List<Integer> cells = entry.getValue();

            for (Integer cell : cells)
                materializedCubes.add(new Tuple2<>(group, cell));
        }

        return materializedCubes;
    }

    public void readMaterializedCubesFromFile(String cubeFilePath)
            throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(cubeFilePath);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        materializedPairsOfGroupAndCell = (List<Tuple2<String, Integer>>)in.readObject();
        in.close();
        fileIn.close();
    }

    public Map<String, Integer> processCountQuery(WarehouseQuery query) {
        Map<String, Integer> result = new HashMap<>();
        String group = query.getGroupBy();

        if (query.getRange().isPresent()) {
            Tuple4<Double, Double, Double, Double> range =
                    query.getRange().get();
            Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
            Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());

            for (Map.Entry<Integer, HashMap<String, GridCube>> entry : hour2Cubes.entrySet()) {
                String hour = String.valueOf(entry.getKey());
                GridCube cube = entry.getValue().get(group);
                getCountAggregationByRange(hour, group, cube, cornerFrom, cornerTo, result);
            }
        } else {
            for (HashMap<String, GridCube> group2Cube : hour2Cubes.values()) {
                GridCube cube = group2Cube.get(group);
                cube.getCountAggregation(result);
            }
        }

        return result;
    }

    private void getCountAggregationByRange(String hour, String group, GridCube cube,
                                            Tuple2<Double, Double> cornerFrom,
                                            Tuple2<Double, Double> cornerTo,
                                            Map<String, Integer> result) {
        long t1 = System.currentTimeMillis();
        List<Integer> borders = GridFunctions.getBorderKeysCellsByRange(
                Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON,
                Constants.GRID_NUM_LAT_CELLS, Constants.GRID_NUM_LON_CELLS,
                cornerFrom, cornerTo);
        List<Integer> keys = GridFunctions.getKeysCellsByRange(
                Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON,
                Constants.GRID_NUM_LAT_CELLS, Constants.GRID_NUM_LON_CELLS,
                cornerFrom, cornerTo);
        List<Integer> usedKeys = new ArrayList<>();
        List<Integer> unusedKeys = new ArrayList();
        keys.forEach(k -> {
            if (!borders.contains(k)) {
                if (cube != null && cube.materializedCells.contains(k))
                    usedKeys.add(k);
                else
                    unusedKeys.add(k);
            }
        });

        List<HashMap<String, Integer>> mapList = usedKeys.stream()
                .map(k -> cube.cell2Map.get(k)).filter(map -> map != null).collect(Collectors.toList());
        HashMap<String, Integer> resultMap = new HashMap<>();
        mapList.forEach(map -> {
            map.forEach((k, v) -> {
                int count = resultMap.getOrDefault(k, 0);
                resultMap.put(k, count + v);
            });
        });
        long t2 = System.currentTimeMillis();
        timeReadCubes += t2 - t1;

        t1 = System.currentTimeMillis();
        for (int k : borders) {
            List<SpatioTextualObject> objects = rawData.getObjectsByHourCell(hour, k).orElse(new ArrayList<>())
                    .stream()
                    .filter(o -> GridFunctions.isInside(o.getCoord(), cornerFrom, cornerTo))
                    .collect(Collectors.toList());
            objects.forEach(o -> {
                String combinedDimValue = o.getGroupBy(group);
                int count = resultMap.getOrDefault(combinedDimValue, 0);
                resultMap.put(combinedDimValue, count + 1);
            });
            numReadedObjectsInRawData += objects.size();
        }
        t2 = System.currentTimeMillis();
        timeReadRawData += t2 - t1;

        int hourInt = Integer.valueOf(hour);
        List<GridCube> ancestorCubes = Arrays.stream(Constants.GROUPS)
                .filter(ancestor -> isAncestorOf(ancestor, group) && hour2Cubes.get(hourInt).containsKey(ancestor))
                .map(ancestor -> hour2Cubes.get(hourInt).get(ancestor))
                .collect(Collectors.toList());
        for (Integer cell : unusedKeys) {
            long ttt1 = System.currentTimeMillis();
            List<GridCube> ancestors = new ArrayList<>();
            for (GridCube cand : ancestorCubes) {
                if (cand.materializedCells.contains(cell))
                    ancestors.add(cand);
            }

            Comparator<GridCube> comp = (a, b) -> Integer.compare(a.getCardinalityByCell(cell),
                            b.getCardinalityByCell(cell));
            GridCube optAncestor = ancestors.stream().min(comp).orElse(null);
            long ttt2 = System.currentTimeMillis();
            timeReadCubes += ttt2 - ttt1;

            if (optAncestor != null) {
                t1 = System.currentTimeMillis();
                HashMap<String, Integer> ancestorMap = optAncestor.cell2Map.get(cell);
                if (ancestorMap != null)
                    dependency.getAggregationFromAncestor(resultMap, ancestorMap, optAncestor.group, group);
                t2 = System.currentTimeMillis();
                timeReadCubes += t2 - t1;
            } else {
                t1 = System.currentTimeMillis();
                List<SpatioTextualObject> objects = rawData.getObjectsByHourCell(hour, cell).orElse(new ArrayList<>())
                        .stream()
                        .collect(Collectors.toList());
                objects.forEach(o -> {
                    String combinedDimValue = o.getGroupBy(group);
                    int count = resultMap.getOrDefault(combinedDimValue, 0);
                    resultMap.put(combinedDimValue, count + 1);
                });
                t2 = System.currentTimeMillis();
                timeReadRawData += t2 - t1;
                numReadedObjectsInRawData += objects.size();
            }
        }

        for (Map.Entry<String, Integer> entry : resultMap.entrySet()) {
            String key = entry.getKey();
            int val = entry.getValue();
            int count = result.getOrDefault(key, 0) + val;
            result.put(key, count);
        }
    }

    public Map<String, Map<String, Integer>> processWordCountQuery(WarehouseQuery query) {
        return null;
    }

    private void updateCubes(SpatioTextualObject object) {
        String hour = object.getHour();
        int key = Integer.valueOf(hour);
        HashMap<String, GridCube> cubes = hour2Cubes.get(key);
        if (cubes == null) {
            addCubesAtHour(hour);
            cubes = hour2Cubes.get(key);
        }

        for (Map.Entry<String, GridCube> entry : cubes.entrySet()) {
            String group = entry.getKey();
            GridCube cube = entry.getValue();
            cube.updateCube(object);
        }

        dependency.update(object);
    }

    public Map<String, List<Integer>> computeMaterializedCubes(Stats stats) {
        HashMap<String, HashSet<Integer>> group2SelectedCells =
                selectCellsForCubes(stats);
        List<Tuple2<String, Integer>> materializedGroupCells = new ArrayList<>();

        long sumMemoryCost = 0L;
        List<Tuple2<String, Integer>> groupCellPairs = new ArrayList<>();
        for (Map.Entry<String, HashSet<Integer>> entry : group2SelectedCells.entrySet()) {
            String group = entry.getKey();
            HashSet<Integer> selectedCells = entry.getValue();
            for (Integer cell : selectedCells) {
                groupCellPairs.add(new Tuple2<>(group, cell));
                int size = stats.getStat(cell, group).cardinality;
                sumMemoryCost += size;
            }
        }

        if (sumMemoryCost <= Constants.MEM_CAPACITY) {
            Map<String, List<Integer>> group2Cells = new HashMap<>();
            for (Map.Entry<String, HashSet<Integer>> entry : group2SelectedCells.entrySet()) {
                String group = entry.getKey();
                List<Integer> cells = new ArrayList<>(entry.getValue());
                group2Cells.put(group, cells);
            }

            System.out.println("Number of selected cells: " + groupCellPairs.size());
            System.out.println("Memory cost: " + sumMemoryCost);
            return group2Cells;
        }

        int numPhases = groupCellPairs.size();
        long memoryCapacity = Constants.MEM_CAPACITY;
        sumMemoryCost = 0L;

        System.out.println("Num phases: " + numPhases);

        for (int i = 0; i < numPhases; ++i) {
            Tuple2<String, Integer> optimalPair = null;
            double optimalMetric = -Double.MAX_VALUE;
            int memoryCost = 0;

            for (Tuple2<String, Integer> groupAndCell : groupCellPairs) {
                String group = groupAndCell._1();
                int cell = groupAndCell._2();

                if (materializedGroupCells.contains(groupAndCell))
                    continue;

                double benefit = getBenefit(stats, groupAndCell, materializedGroupCells);
                int size = stats.getStat(cell, group).cardinality;

                double metric = benefit / size;
                if (metric > 0 && metric > optimalMetric) {
                    optimalMetric = metric;
                    optimalPair = groupAndCell;
                    memoryCost = size;
                }
            }

            if (optimalPair != null && sumMemoryCost + memoryCost <= memoryCapacity) {
                materializedGroupCells.add(optimalPair);
                sumMemoryCost += memoryCost;
            }

            if (i % 100 == 0)
                System.out.printf("Finish %d phases\n", i + 1);
        }

        Map<String, List<Tuple2<String, Integer>>> group2Pairs =
                materializedGroupCells.stream().collect(Collectors.groupingBy(Tuple2<String, Integer>::_1));
        Map<String, List<Integer>> group2Cells = new HashMap<>();
        group2Pairs.forEach((group, pairs) -> {
            List<Integer> cells = group2Cells.get(group);
            if (cells == null) {
                cells = new ArrayList<>();
                group2Cells.put(group, cells);
            }
            for (Tuple2<String, Integer> pair : pairs)
                cells.add(pair._2());
        });

        System.out.println("Number of selected cells: " + materializedGroupCells.size());
        System.out.println("Memory cost: " + sumMemoryCost);

        return group2Cells;
    }

    private HashMap<String, HashSet<Integer>> selectCellsForCubes(Stats stats) {
        List<String> groups = stats.getGroups();
        Comparator<String> comparator =
                Comparator.comparingInt(group2Level::get);
        Collections.sort(groups, comparator);
        HashMap<String, HashSet<Integer>> group2SelectedCells =
                new HashMap<>();

        for (String group : groups) {
            for (Integer cell : stats.cell2Stat.keySet()) {
                CellStat cellStat = stats.getStat(cell, group);
                List<Double> costs = getCosts(cellStat);

                if (costs.get(0) < costs.get(1)) {
                    Predicate<Tuple2<String, String>> isAncestor = t -> t._2().equals(group);
                    List<String> ancestors = groupDependencies
                            .stream()
                            .filter(isAncestor)
                            .map(t -> t._1())
                            .collect(Collectors.toList());
                    if (ancestors.isEmpty()) {
                        HashSet<Integer> selectedCells = group2SelectedCells.get(group);
                        if (selectedCells == null) {
                            selectedCells = new HashSet<>();
                            group2SelectedCells.put(group, selectedCells);
                        }
                        selectedCells.add(cell);
                    } else {
                        String ancestorGroupMinCard =
                                group2SelectedCells.entrySet().stream()
                                .filter(e -> {
                                    if (!isAncestorOf(e.getKey(), group))
                                        return false;
                                    HashSet<Integer> selectedCells = e.getValue();
                                    return selectedCells == null ? false : selectedCells.contains(cell);
                                })
                                .min((a, b) -> {
                                    int cardOfA = stats.getGlobalCardinality(a.getKey());
                                    int cardOfB = stats.getGlobalCardinality(b.getKey());
                                    return Integer.compare(cardOfA, cardOfB);
                                }).map(e -> e.getKey()).orElse(null);

                        if (ancestorGroupMinCard != null) {
                            List<Double> costsUsingAndNotUsingAncestor =
                                    getCosts(stats, ancestorGroupMinCard, group, cell);
                            if (costsUsingAndNotUsingAncestor.get(1) < costsUsingAndNotUsingAncestor.get(0)) {
                                HashSet<Integer> selectedCells = group2SelectedCells.get(group);
                                if (selectedCells == null) {
                                    selectedCells = new HashSet<>();
                                    group2SelectedCells.put(group, selectedCells);
                                }
                                selectedCells.add(cell);
                            }
                        } else {
                            HashSet<Integer> selectedCells = group2SelectedCells.get(group);
                            if (selectedCells == null) {
                                selectedCells = new HashSet<>();
                                group2SelectedCells.put(group, selectedCells);
                            }
                            selectedCells.add(cell);
                        }
                    }
                }
            }
        }

        return group2SelectedCells;
    }

    // Return cost with cube and cost without cube using the raw data
    private List<Double> getCosts(CellStat cellStat) {
        double weightMaintain = Constants.WEIGHT_MAINTAIN, weightQuery = Constants.WEIGHT_QUERY;
        int numObjects = cellStat.numObjects;
        int numQueries = cellStat.numQueries;
        int cardinality = cellStat.cardinality;

        double maintainCost = numObjects;
        double queryCostWithCube = cardinality * cardinality;
        double costWithCube = maintainCost * weightMaintain + queryCostWithCube * weightQuery;

        double queryCostWithoutCube = numQueries * numObjects;
        double costWithoutCube = queryCostWithoutCube;

        return Arrays.asList(costWithCube, costWithoutCube);
    }

    // Return cost using ancestor cube and cost not using ancestor cube (using the new cube)
    private List<Double> getCosts(Stats stats, String ancestorGroupMinCard, String group, int cell) {
        double weightMaintain = Constants.WEIGHT_MAINTAIN, weightQuery = Constants.WEIGHT_QUERY;
        CellStat stat = stats.getStat(cell, group);
        CellStat ancestorStat = stats.getStat(cell, ancestorGroupMinCard);
        int numObjects = stat.numObjects;
        int numQueries = stat.numQueries;

        double costUsingAncestor = ancestorStat.cardinality * numQueries;

        double maintainCost = numObjects;
        double queryCostUsingNewCube = stat.cardinality * numQueries;
        double costUsingNewCube = maintainCost * weightMaintain + queryCostUsingNewCube * weightQuery;

        return Arrays.asList(costUsingAncestor, costUsingNewCube);
    }

    private double getBenefit(Stats stats, Tuple2<String, Integer> groupCell,
                              List<Tuple2<String, Integer>> materializedGroupCells) {
        int addedCell = groupCell._2();
        List<Tuple2<String, Integer>> relatedGroupCells = materializedGroupCells.stream().
                filter(pair -> pair._2() == addedCell).collect(Collectors.toList());

        double sumQueryCostBefore = getQueryCost(stats, addedCell, relatedGroupCells);
        List<Tuple2<String, Integer>> newRelatedGroupCells = new ArrayList<>(relatedGroupCells);
        newRelatedGroupCells.add(groupCell);
        double sumQueryCostAfter = getQueryCost(stats, addedCell, newRelatedGroupCells);

        return sumQueryCostBefore - sumQueryCostAfter - stats.getNumObjectsByCell(addedCell);
    }

    private double getQueryCost(Stats stats, int checkedCell, List<Tuple2<String, Integer>> relatedGroupCells) {
        double sumQueryCost = 0.0;
        for (String group : stats.getGroups()) {
            CellStat cellStat = stats.getStat(checkedCell, group);
            int numQueries = cellStat.numQueries;
            if (numQueries == 0)
                continue;

            List<String> ancestors = relatedGroupCells
                    .stream()
                    .filter(pair -> pair._1().equals(group) || isAncestorOf(pair._1(), group))
                    .map(pair -> pair._1())
                    .collect(Collectors.toList());
            if (ancestors.isEmpty())
                sumQueryCost += numQueries * cellStat.numObjects;
            else {
                Comparator<String> comparator = (a, b) -> {
                    CellStat stat1 = stats.getStat(checkedCell, a);
                    CellStat stat2 = stats.getStat(checkedCell, b);
                    return Integer.compare(stat1.cardinality,
                            stat2.cardinality);
                };
                int minCardinality = ancestors.stream().min(comparator).map(ancestor -> {
                    CellStat stat = stats.getStat(checkedCell, ancestor);
                    return stat.cardinality;
                }).get();
                sumQueryCost += numQueries * minCardinality;
            }
        }

        return sumQueryCost;
    }

    private static void compareQueryResult(RawData rawData, GridCubes cubes, WarehouseQuery query) {
        Tuple4<Double, Double, Double, Double> queryRange = query.getRange().get();
        System.out.printf("Comparing the result for query: {%s, [(%f, %f), (%f, %f)]}\n",
                query.getGroupBy(), queryRange._1(), queryRange._2(), queryRange._3(), queryRange._4());

        System.out.println("Running the queries without using the cubes...");
        String group = query.getGroupBy();
        Tuple4<Double, Double, Double, Double> range = query.getRange().get();
        Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
        Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());

        List<SpatioTextualObject> os =
                rawData.getObjectsByRange(cornerFrom, cornerTo).orElse(new ArrayList<>());
        Map<String, Integer> aggregation = new HashMap<>();
        for (SpatioTextualObject o : os) {
            String key = o.getGroupBy(group);
            int count = aggregation.getOrDefault(key, 0) + 1;
            aggregation.put(key, count);
        }
        aggregation.forEach((k, v) -> System.out.printf("%s:%d\n", k, v));

        System.out.println("Running the queries using the cubes...");
        aggregation = cubes.processCountQuery(query);
        aggregation.forEach((k, v) -> System.out.printf("%s:%d\n", k, v));
    }

    private static void evaluateRawDataReadingTime(String filePath, int numObjects, int numQueries, double rangeRatio)
            throws IOException {
        System.out.println("------------------------------------");
        System.out.println("Evaluate the running time for data: " + filePath);
        numObjectsForQueries = 0L;
        List<SpatioTextualObject> objects = UtilFunctions.createObjects(filePath, numObjects);
        List<WarehouseQuery> queries = UtilFunctions.createQueries(objects, numQueries,
                Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON, rangeRatio);

        RawData rawData = new RawData();
        rawData.addObjects(objects);
        long t1 = System.currentTimeMillis();
        long sumTimeGetObjectsByRange = 0L;
        long sumTimeGetAggregation = 0L;
        for (WarehouseQuery query : queries) {
            String group = query.getGroupBy();
            Tuple4<Double, Double, Double, Double> range = query.getRange().get();
            Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
            Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());

            long tt1 = System.currentTimeMillis();
            List<SpatioTextualObject> os =
                    rawData.getObjectsByRange(cornerFrom, cornerTo).orElse(new ArrayList<>());
            long tt2 = System.currentTimeMillis();
            sumTimeGetObjectsByRange += tt2 - tt1;

            tt1 = System.currentTimeMillis();
            Map<String, Integer> aggregation = new HashMap<>();
            for (SpatioTextualObject o : os) {
                String key = o.getGroupBy(group);
                int count = aggregation.getOrDefault(key, 0) + 1;
                aggregation.put(key, count);
            }
            tt2 = System.currentTimeMillis();
            sumTimeGetAggregation += tt2 - tt1;

            numObjectsForQueries += os.size();
        }
        long t2 = System.currentTimeMillis();
        System.out.printf("Done! It takes %f seconds\n", (t2-t1)/1000.0);
        System.out.printf("sumTimeGetObjectsByRange: %f seconds\n", sumTimeGetObjectsByRange/1000.0);
        System.out.printf("sumTimeGetAggregation: %f seconds\n", sumTimeGetAggregation/1000.0);
        System.out.printf("Number of checked objects for the queries: %d\n", numObjectsForQueries);

        System.exit(0);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String dataPath = args[0];
        int numObjects = Integer.valueOf(args[1]);
        int numQueries = Integer.valueOf(args[2]);
        double rangeRatio = Double.valueOf(args[3]);
        String cubeFile = args[4];

//        String dataPath = "resources/sql_tweets.txt";
//        int numObjects = 100000;
//        int numQueries = 1000;
//        double rangeRatio = 0.1;
//        String cubeFile = "resources/cubes.txt";
//        evaluateRawDataReadingTime(dataPath, numObjects, numQueries, rangeRatio);

        System.out.println("Configuration:");
        System.out.println("\tdataPath = " + dataPath);
        System.out.println("\tnumObjects = " + numObjects);
        System.out.println("\tnumQueries = " + numQueries);
        System.out.println("\trangeRatio = " + rangeRatio);
        System.out.println("\tcubeFile = " + cubeFile);

        long t1 = System.currentTimeMillis();
        System.out.println("Reading the raw data...");
        List<SpatioTextualObject> objects = UtilFunctions.createObjects(dataPath, numObjects);
        long t2 = System.currentTimeMillis();
        System.out.printf("Done! It takes %f seconds\n", (t2-t1)/1000.0);
        System.out.println("Number of objects: " + objects.size());

        List<WarehouseQuery> queries = UtilFunctions.createQueries(objects, numQueries,
                Constants.MIN_LAT, Constants.MIN_LON, Constants.MAX_LAT, Constants.MAX_LON, rangeRatio);
        Map<String, List<WarehouseQuery>> log = queries
                .stream()
                .filter(q -> q.getRange().isPresent())
                .collect(Collectors.groupingBy(q -> q.getGroupBy()));

        t1 = System.currentTimeMillis();
        System.out.println("Running the cube materialization algorithm...");
        GridCubes cubes = new GridCubes();
        cubes.materializeCubesAndWrite2File(objects, log, cubeFile);
        cubes.readMaterializedCubesFromFile(cubeFile);
        cubes.addObjects(objects);
        t2 = System.currentTimeMillis();
        System.out.printf("Done! It takes %f seconds\n", (t2-t1)/1000.0);

        RawData rawData = new RawData();
        rawData.addObjects(objects);
        System.out.println("Running the queries without using the cubes...");
        t1 = System.currentTimeMillis();
        for (WarehouseQuery query : queries) {
            String group = query.getGroupBy();
            Tuple4<Double, Double, Double, Double> range = query.getRange().get();
            Tuple2<Double, Double> cornerFrom = new Tuple2<>(range._1(), range._2());
            Tuple2<Double, Double> cornerTo = new Tuple2<>(range._3(), range._4());

            List<SpatioTextualObject> os =
                    rawData.getObjectsByRange(cornerFrom, cornerTo).orElse(new ArrayList<>());
            Map<String, Integer> aggregation = new HashMap<>();
            for (SpatioTextualObject o : os) {
                String key = o.getGroupBy(group);
                int count = aggregation.getOrDefault(key, 0) + 1;
                aggregation.put(key, count);
            }
            numObjectsForQueries += os.size();
        }
        t2 = System.currentTimeMillis();
        System.out.printf("Done! It takes %f seconds\n", (t2-t1)/1000.0);

        t1 = System.currentTimeMillis();
        System.out.println("Running the queries using the cubes...");
        for (WarehouseQuery query : queries)
            cubes.processCountQuery(query);
        t2 = System.currentTimeMillis();
        System.out.printf("Done! It takes %f seconds\n", (t2-t1)/1000.0);
        System.out.printf("Time used in reading cubes: %f seconds\n", timeReadCubes/1000.0);
        System.out.printf("Time used in reading raw data: %f seconds\n", timeReadRawData/1000.0);
        System.out.printf("Number of checked objects for the queries: %d\n", numObjectsForQueries);
        System.out.printf("Number of objects in reading raw data: %d\n", numReadedObjectsInRawData);
        System.out.printf("Percentage of objects in reading raw data: %f\n",
                (double)numReadedObjectsInRawData/numObjectsForQueries);

//        compareQueryResult(rawData, cubes, queries.get(20));
    }
}
