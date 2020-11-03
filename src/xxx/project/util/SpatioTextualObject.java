package xxx.project.util;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.*;

import static xxx.project.util.Constants.GROUPS;


public class SpatioTextualObject implements Serializable {

    public static final String DIMENSION_SEPARATOR = "\1";
    private static final long serialVersionUID = 1L;

    private Tuple2<Double, Double> coord;
    private String text;
    private String timestamp;
    private Set<String> terms;

    // spatial dimension
    private String country;
    private String city;

    // temporal dimension
    private LocalDate date;
    private String year;
    private String month;
    private String week;
    private String day;
    private String hour;

    // textual dimension
    private String topic;

    public SpatioTextualObject(Tuple2<Double, Double> coord, String text, String timestamp, String country, String city, String year, String month, String week, String day, String hour, String topic) {
        this.coord = coord;
        this.text = text;
        this.timestamp = timestamp;
        this.country = country;
        this.city = city;
        this.year = year;
        this.month = month;
        this.week = week;
        this.day = day;
        this.hour = hour;
        this.topic = topic;
        this.date = LocalDate.of(Integer.valueOf(year),
                Integer.valueOf(month.substring(4)), // month is yyyymm
                Integer.valueOf(day.substring(6)));

        String[] termArray = text.split(" ");
        this.terms = new HashSet<>();
        for (String term : termArray)
            this.terms.add(term);
    }

    public Tuple2<Double, Double> getCoord() {
        return coord;
    }

    public double getLatitude() {
        return coord._1();
    }

    public double getLongitude() {
        return coord._2();
    }

    public String getText() {
        return text;
    }

    public String getCountry() {
        return country;
    }

    public String getCity() {
        return city;
    }

    public String getYear() {
        return year;
    }

    public String getMonth() {
        return month;
    }

    public String getWeek() {
        return week;
    }

    public String getDay() {
        return day;
    }

    public String getHour() {
        return hour;
    }

    public LocalDate getDate() {
        return date;
    }

    public String getTopic() {
        return topic;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getGroupBy(String group) {
        final String separator = DIMENSION_SEPARATOR;

        if (group.equals(GROUPS[0]))
            return country;
        else if (group.equals(GROUPS[1]))
            return city;
        else if (group.equals(GROUPS[2]))
            return month;
        else if (group.equals(GROUPS[3]))
            return week;
        else if (group.equals(GROUPS[4]))
            return day;
        else if (group.equals(GROUPS[5]))
            return hour;
        else if (group.equals(GROUPS[6]))
            return topic;
        else if (group.equals(GROUPS[7]))
            return country + separator + month;
        else if (group.equals(GROUPS[8]))
            return country + separator + week;
        else if (group.equals(GROUPS[9]))
            return country + separator + day;
        else if (group.equals(GROUPS[10]))
            return country + separator + hour;
        else if (group.equals(GROUPS[11]))
            return country + separator + topic;
        else if (group.equals(GROUPS[12]))
            return city + separator + month;
        else if (group.equals(GROUPS[13]))
            return city + separator + week;
        else if (group.equals(GROUPS[14]))
            return city + separator + day;
        else if (group.equals(GROUPS[15]))
            return city + separator + hour;
        else if (group.equals(GROUPS[16]))
            return city + separator + topic;
        else if (group.equals(GROUPS[17]))
            return month + separator + topic;
        else if (group.equals(GROUPS[18]))
            return week + separator + topic;
        else if (group.equals(GROUPS[19]))
            return day + separator + topic;
        else if (group.equals(GROUPS[20]))
            return hour + separator + topic;
        else if (group.equals(GROUPS[21]))
            return country + separator + month + separator + topic;
        else if (group.equals(GROUPS[22]))
            return country + separator + week + separator + topic;
        else if (group.equals(GROUPS[23]))
            return country + separator + day + separator + topic;
        else if (group.equals(GROUPS[24]))
            return country + separator + hour + separator + topic;
        else if (group.equals(GROUPS[25]))
            return city + separator + month + separator + topic;
        else if (group.equals(GROUPS[26]))
            return city + separator + week + separator + topic;
        else if (group.equals(GROUPS[27]))
            return city + separator + day + separator + topic;
        else if (group.equals(GROUPS[28]))
            return city + separator + hour + separator + topic;
        else
            return "none";
    }

    public Set<String> getTerms() {
        return terms;
    }

    public static ArrayList<String> getDetailedTimeInfo(String timeStamp) {
        ArrayList<String> detailedTimeInfo = new ArrayList<>();
        // time stamp format: yyyy-mm-dd HH:MM:SS
        String year = timeStamp.substring(0, 4);
        String month = year + timeStamp.substring(5, 7);
        String day = month + timeStamp.substring(8, 10);
        String hour = day + timeStamp.substring(11, 13);

        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(timeStamp, format);
        TemporalField weekOfYear = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear();
        String week = year + String.valueOf(dateTime.get(weekOfYear));

        detailedTimeInfo.add(year);
        detailedTimeInfo.add(month);
        detailedTimeInfo.add(week);
        detailedTimeInfo.add(day);
        detailedTimeInfo.add(hour);

        return detailedTimeInfo;
    }
}
