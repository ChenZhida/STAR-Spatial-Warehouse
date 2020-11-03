package xxx.project.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xxx.project.util.Constants;

import java.util.ArrayList;

public class ParserBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ParserBolt.class);
    public static final String[] RAW_FIELDS = {"tid", "createtime", "text", "hashtags", "tags", "lat", "lon",
            "placeid", "placefullname",	"country", "screenname", "ulang", "ulocation", "timestamp"};
    public static final String[] PARSED_FIEDLS = {"id", "lat", "lon", "text", "timestamp", "country",
            "city", "year", "month", "day", "hour"};

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) throws NumberFormatException {
        // 0: topic, 1: part_id, 2: offset, 3: msg_key, 4: msg_content
        String message = tuple.getString(4);
        String[] values = message.split(String.valueOf(Constants.SEPARATOR));

        try {
            long id = Long.valueOf(values[0]);
            double lat = Double.valueOf(values[5]);
            double lon = Double.valueOf(values[6]);
            String text = values[4];
            String timeStamp = values[13];
            String country = values[9];
            String city = values[8];

            ArrayList<String> detailedTime = getDetailedTimeInfo(timeStamp);
            String year = detailedTime.get(0);
            String month = detailedTime.get(1);
            String day = detailedTime.get(2);
            String hour = detailedTime.get(3);

            collector.emit(new Values(id, lat, lon, text, timeStamp, country, city,
                    year, month, day, hour));
        } catch (NumberFormatException e) {
            throw(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(PARSED_FIEDLS));
    }

    private ArrayList<String> getDetailedTimeInfo(String timeStamp) {
        ArrayList<String> detailedTimeInfo = new ArrayList<>();
        // time stamp format: yyyy-mm-dd HH:MM:SS
        String year = timeStamp.substring(0, 4);
        String month = year + timeStamp.substring(5, 7);
        String day = month + timeStamp.substring(8, 10);
        String hour = day + timeStamp.substring(11, 13);

        detailedTimeInfo.add(year);
        detailedTimeInfo.add(month);
        detailedTimeInfo.add(day);
        detailedTimeInfo.add(hour);

        return detailedTimeInfo;
    }
}
