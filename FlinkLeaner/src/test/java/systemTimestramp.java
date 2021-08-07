import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

public class systemTimestramp {
    public static void main(String[] args) {
        long timestramp = System.currentTimeMillis()/1000;
        System.out.println(timestramp);
    }
}
