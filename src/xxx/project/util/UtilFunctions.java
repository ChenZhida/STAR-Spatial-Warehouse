package xxx.project.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

public class UtilFunctions {
    public static List<SpatioTextualObject> createObjects(String filePath, int numObjects)
            throws IOException {
        List<SpatioTextualObject> objects = new ArrayList<>();

        Reader reader = new InputStreamReader(new FileInputStream(filePath));
        char[] tempChars = new char[1000];
        int index = 0, charRead = 0;
        int numLines = 0;

        while ((charRead = reader.read()) != -1) {
            if ((char)charRead != '\n')
                tempChars[index++] = (char)charRead;
            else {
                String line = new String(tempChars, 0, index);
                String[] values = line.split(String.valueOf(Constants.SEPARATOR));
                long id;
                double lat, lon;
                String text, timeStamp, country, city;
                ArrayList<String> detailedTime;
                String year, month, week, day, hour;

                try {
                    id = Long.valueOf(values[0]);
                    lat = Double.valueOf(values[5]);
                    lon = Double.valueOf(values[6]);
                    text = values[4];
                    timeStamp = values[13];
                    country = values[9];
                    city = values[8];

                    detailedTime = SpatioTextualObject.getDetailedTimeInfo(timeStamp);
                    year = detailedTime.get(0);
                    month = detailedTime.get(1);
                    week = detailedTime.get(2);
                    day = detailedTime.get(3);
                    hour = detailedTime.get(4);
                } catch (Exception ex) {
                    index = 0;
                    continue;
                }

                int randIndex = new Random().nextInt(Constants.TEXT_TOPICS.length);
                String topic = Constants.TEXT_TOPICS[randIndex];

                SpatioTextualObject object = new SpatioTextualObject(new Tuple2<>(lat, lon),
                        text, timeStamp, country, city, year, month, week, day, hour, topic);
                objects.add(object);

                index = 0;
                if (++numLines % 1000000 == 0)
                    System.out.println("Finish creating " + numLines + " objects.");
                if (numLines >= numObjects)
                    break;
            }
        }
        reader.close();

        return objects;
    }

    public static List<WarehouseQuery> createQueries(List<SpatioTextualObject> objects, int numQueries,
                                                     double MIN_LAT, double MIN_LON,
                                                     double MAX_LAT, double MAX_LON, double rangeRatio) {
        Random rand = new Random(0);
        int ratio = objects.size() / numQueries;
        double lonIncrement = (MAX_LON - MIN_LON) * rangeRatio;
        double latIncrement = (MAX_LAT - MIN_LAT) * rangeRatio;
        List<WarehouseQuery> queries = new ArrayList<>();

        objects.forEach(o -> {
            if (rand.nextInt(ratio) == 0) {
                double lon = o.getCoord()._2();
                double lat = o.getCoord()._1();

                double lonFrom = Double.max(MIN_LON, lon - lonIncrement / 2.0);
                double latFrom = Double.max(MIN_LAT, lat - latIncrement / 2.0);
                double lonTo = Double.min(MAX_LON - 0.000001, lon + lonIncrement / 2.0);
                double latTo = Double.min(MAX_LAT - 0.000001, lat + latIncrement / 2.0);
                Tuple4<Double, Double, Double, Double> range = new
                        Tuple4<>(latFrom, lonFrom, latTo, lonTo);
                String measure = "count";
                String groupBy = Constants.GROUPS[rand.nextInt(Constants.GROUPS.length)];
                String topic = Constants.TEXT_TOPICS[rand.nextInt(Constants.TEXT_TOPICS.length)];
                HashSet<String> keywords = null;

                queries.add(new WarehouseQuery(measure, groupBy, range, keywords, topic));
            }
        });

        return queries;
    }
}
