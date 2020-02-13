package redisConnection;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class TestingTimeStamp {

    public static void main(String[] args) {
        String timestampstr = "2020-02-10T12:08:11Z";
        String test2 = "2020-02-10T12:32:42Z";
      Instant instant = Instant.parse(  timestampstr );
      long millis = instant.getEpochSecond();

      System.out.println(millis);

      // the above works locally but not when I run the pipeline...



        // ok since this format seems to be the problem, let's
        // change it

        Instant now = Instant.now();
        Instant nowNoMilli = now.truncatedTo(ChronoUnit.SECONDS);
        String tsString = nowNoMilli.toString();
        System.out.println(tsString);

        String pattern = "yyyy-MM-dd'T'HH:mm:ssX";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        LocalDateTime localDateTime = LocalDateTime.from(formatter.parse(tsString));

        Timestamp timestamp = Timestamp.valueOf(localDateTime);
        System.out.println(timestamp.toString());






    }
}
