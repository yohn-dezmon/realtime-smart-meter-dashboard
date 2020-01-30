import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class TestingRegex {
    public static void main(String[] args) {

        String keyStr = "gcpuwyskfgux@1579871665000/1579871670000";

//        Pattern p = Pattern.compile("[a-zA-z]+");
        Pattern p = Pattern.compile("([a-zA-Z]+)(.*)");
        Matcher m = p.matcher(keyStr);
        boolean b = m.matches();
        String geoHashKey = m.group(1);

        System.out.println(b);
        System.out.println(geoHashKey);


    }
}
