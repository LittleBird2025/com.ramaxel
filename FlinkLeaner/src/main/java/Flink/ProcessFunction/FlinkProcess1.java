package Flink.ProcessFunction;

import Bean.AlertInfo;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FlinkProcess1 {
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
        maped.keyBy("serverId").process( new myprocess() ).print();

        env.execute();
    }
    //实现自定义函数myprocess
    public static class myprocess extends KeyedProcessFunction<Tuple,AlertInfo,Long>{
        //保存processtime供关闭定时器使用
        ValueState<Long> process_State;
        @Override
        public void open(Configuration parameters) throws Exception {
            process_State = getRuntimeContext().getState(new ValueStateDescriptor<Long>("process-timer", Long.class));
        }

        @Override
        public void processElement(AlertInfo alertInfo, Context context, Collector<Long> collector) throws Exception {
            Long alarmCount = alertInfo.getAlarmCount();
            collector.collect(alarmCount);

            context.getCurrentKey();
//            Long currentTime = context.timerService().currentProcessingTime();
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime()+1000L);
            //更新状态
            process_State.update(context.timerService().currentProcessingTime()+1000L);
            //清空定时器
            context.timerService().deleteProcessingTimeTimer(process_State.value());
        }

        //触发定时器
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Long> out) throws Exception {
            System.out.println(timestamp+"定时器出发");
        }

        //关闭状态
        @Override
        public void close() throws Exception {
            process_State.clear();
        }
    }
}
