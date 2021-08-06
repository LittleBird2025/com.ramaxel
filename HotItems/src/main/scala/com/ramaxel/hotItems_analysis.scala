package com.ramaxel


import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
//先定义输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
//定义窗口聚合结果样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object hotItems_analysis {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //  为了打印到控制台的结果不乱序，我们配置全局的并发为 1 ，这里改变并发对结果正确性没有影响
    env.setParallelism(1)
    //  设定 Time 类型为 EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //读取数据
    val data: DataStream[String] = env.readTextFile("C:\\Users\\Dell\\IdeaProjects\\com.ramaxel\\HotItems\\src\\main\\resources\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = data.map(data => {
      val dataArray: Array[String] = data.split(",")
      UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(3).trim.toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //处理数据 transform
    val processedStream: DataStream[String] = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new windowResult()) //按照窗口聚合
      .keyBy(_.windowEnd) //按照窗口分组
      .process(new TopNHotItems(3))

    //控制台输出
    processedStream.print()

    env.execute()
  }
}
//定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long =0

  override def add(in: UserBehavior, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(a: Long, b: Long): Long = a+b
}

//自定义窗口函数，输出ItemViewCount样例类
class windowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}

//自定义的处理函数
class TopNHotItems(topSize:Int) extends KeyedProcessFunction[Long,ItemViewCount,String] {
  private var itemState:ListState[ItemViewCount]=_
  override def open(parameters: Configuration): Unit = {
    itemState=getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount]))
  }
  override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //把每一条数据存入状态列表
    itemState.add(value)
    //注册一个定时器
    context.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  //定时器触发时，多所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
//    将所有state中的数据取出，放到一个list buffer中
    val allItems:ListBuffer[ItemViewCount]=new ListBuffer()
    import scala.collection.JavaConversions._
    for(item<-itemState.get()){
      allItems+=item
    }

    //按照count大小排序,并取前N个
    val sortedItems=allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //清空状态
    itemState.clear()

    //将排名结果格式化输出
    val result:StringBuilder=new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    //输出每一个商品的信息
    for(i <- 0 to sortedItems.length-1 ){
      val currentItem=sortedItems(i)
      result.append("No").append(i+1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量").append(currentItem.count)
        .append("\n")
    }
    result.append("============================")
    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())


  }




}

