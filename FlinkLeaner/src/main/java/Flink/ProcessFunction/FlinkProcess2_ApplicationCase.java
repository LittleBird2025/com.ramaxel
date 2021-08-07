package Flink.ProcessFunction;

import Bean.AlertInfo;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FlinkProcess2_ApplicationCase {
    public static void main(String[] args) throws Exception {
        //kafka的参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.80.101:9092,192.168.80.102:9092,192.168.80.103:9092");
        properties.setProperty("auto.offset.reset","latest");

        //获取flink的环境,并拿到kafka中的数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.addSource(new FlinkKafkaConsumer<>("etl", new SimpleStringSchema(), properties));
        //对数据进行处理，返回Java bean类型
        SingleOutputStreamOperator<AlertInfo> maped = data.map(line -> {
            //解析json字段
            JSONObject jsonObject = JSONObject.parseObject(line);
            String timeStrap = jsonObject.getString("timestamp");
            String serverId = jsonObject.getString("serverId");
            String location = jsonObject.getString("location");
            String alarmCount = jsonObject.getString("alarmCount");

            return new AlertInfo(timeStrap,
                    serverId,
                    location,
                    Long.valueOf(alarmCount)
            );
        });
//        maped.print();
        //process方法
        maped.keyBy("serverId").process( new AlarmConsIncr() ).print();

        env.execute();
    }
    //实现自定义函数AlarmConsIncr,alarm数量连续三次上升的信息输出
    public static class AlarmConsIncr extends KeyedProcessFunction<Tuple,AlertInfo,String>{
        @Override
        public void processElement(AlertInfo alertInfo, Context context, Collector<String> collector) throws Exception {
            String alarmCount = alertInfo.getAlarmCount().toString();
            collector.collect(alarmCount);

            context.getCurrentKey();
//            Long currentTime = context.timerService().currentProcessingTime();
            long processTime = context.timerService().currentProcessingTime();
            context.timerService().registerEventTimeTimer(Long.valueOf(alertInfo.getTimeStrap())+3000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println(timestamp+"定时器出发");
        }
    }
}
