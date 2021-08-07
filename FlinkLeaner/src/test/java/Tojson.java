import com.alibaba.fastjson.JSONObject;

public class Tojson {
    public static void main(String[] args) {
        String str="{\"timestamp\":\"2021-08-06\",\"serverId\":\"5\",\"cpuCore\":\"6\",\"memlast\":\"82\",\"disklast\":\"2450\",\"location\":\"印度\",\"alarmCount\":\"4\"}";
        JSONObject jsonObject = JSONObject.parseObject(str);

        String timeStrap = jsonObject.getString("timestamp");
        System.out.println(timeStrap);
    }
}
