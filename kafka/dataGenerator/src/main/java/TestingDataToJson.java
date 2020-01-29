import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.Date;

public class TestingDataToJson {

    public static void main(String[] args) {

        boolean outage = false;
        boolean energyTheft = false;
        Double movingAvg = 0.003;

        String geohash = "xyz";

        Date date = new Date();
        long time = date.getTime();
        Timestamp ts = new Timestamp(time);

        TestRecord tr = new TestRecord(ts, geohash, movingAvg, energyTheft, outage);

        JSONObject jo = new JSONObject(tr);
        String jsonStr = jo.toString();
        System.out.println(jsonStr);

    }
}
