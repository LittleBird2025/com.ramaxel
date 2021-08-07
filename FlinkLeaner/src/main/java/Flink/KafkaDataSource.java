package Flink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class KafkaDataSource {
    public static void main(String[] args) throws InterruptedException {
        int i = 0;
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.80.101:9092,192.168.80.102:9092,192.168.80.103:9092");
        props.put("acks", "1");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        while (i < 10000) {
            i++;
//            String time = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            String time = String.valueOf(System.currentTimeMillis()/1000);

            //服务器ID
            int serverId = new Random().nextInt(200);
            //CPU core
            int cpuCore = new Random().nextInt(64);
            //内存余量
            int memlast = new Random().nextInt(128);
            //磁盘余量
            int disklast = new Random().nextInt(10000);
            //服务器位置
            ArrayList<String> Area = new ArrayList<String>();
            Area.add("深圳");
            Area.add("大朗");
            Area.add("长安");
            Area.add("北京");
            Area.add("台北");
            Area.add("印度");
            Area.add("孟加拉");
            Area.add("新加坡");
            Area.add("上海");
            Area.add("广州");
            Area.add("杭州");
            Area.add("重庆");
//            String nub = String.valueOf(new Random().nextInt(9999));
            String location = Area.get(new Random().nextInt(Area.size()));
            //报警次数
            int alarmCount = new Random().nextInt(9);

            //将以上数据通过\t拼接成长字符串
//            String data2 = time+"\t"+serverId + "\t" + cpuCore + "\t" + memlast + "\t" + disklast + "\t" + location + "\t" + alarmCount;
            String data2 = "{"+"\"timestamp\""+":"+"\""+time+"\""+","+
                    "\"serverId\""+":"+"\""+serverId+"\""+","+
                    "\"cpuCore\""+":"+"\""+cpuCore+"\""+","+
                    "\"memlast\""+":"+"\""+memlast+"\""+","+
                    "\"disklast\""+":"+"\""+disklast+"\""+","+
                    "\"location\""+":"+"\""+location+"\""+","+
                    "\"alarmCount\""+":"+"\""+alarmCount+"\""+"}";
            System.out.println(data2);
            ProducerRecord<Integer, String> record = new ProducerRecord("etl",data2);
            Future<RecordMetadata> send = producer.send(record);
            Thread.sleep(2000);
        }
    }
}