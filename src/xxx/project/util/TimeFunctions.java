package xxx.project.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeFunctions {
    public static String FORMAT_FULL = "yyyy-MM-dd HH:mm:ss.S";

    public static String getTimeString() {
        SimpleDateFormat dateFormat = new SimpleDateFormat(FORMAT_FULL);
        Calendar calendar = Calendar.getInstance();
        return dateFormat.format(calendar.getTime());
    }

    public static void main(String[] args) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());

        int month = calendar.get(Calendar.MONTH) + 1;
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);

        System.out.printf("%d %d : %d %d", month, day, hour, minute);

        long t1 = System.currentTimeMillis();
        try {
            Thread.sleep(1000 * 60 * 2);
        } catch (InterruptedException e) {

        }
        long t2 = System.currentTimeMillis();
        double elapsed = (t2 - t1) / (1000 * 60.0);
        System.out.println("Elapsed time: " + elapsed + " min");

        return;
    }
}
