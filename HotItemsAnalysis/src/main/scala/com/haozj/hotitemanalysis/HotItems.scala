package com.haozj.hotitemanalysis


import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


//定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义窗口聚合的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


		val properties = new Properties()
		val input = this.getClass.getClassLoader.getResourceAsStream("kafka.properties")
		properties.load(input)
		val inputStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))

		//val inputStream = env.readTextFile("F:\\cala\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
		//将数据转换时间戳，并提取watermark
		val dataStream = inputStream.map(
			data => {
				val dataArray = data.split(",")
				UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
			}
		).assignAscendingTimestamps(_.timestamp * 1000)

		//对数据进行转换，过滤pv行为，开窗聚合统计个数
		val aggStream: DataStream[ItemViewCount] = dataStream
				.filter(_.behavior == "pv") //过滤pv行为
				.keyBy(_.itemId) //按照ItemId分组
				.timeWindow(Time.hours(1), Time.minutes(5)) //定义滑动窗口
				.aggregate(new CountAgg(), new ItemCountWindowResult())


		//对窗口聚合结果按照窗口进行分组，并做排序TopN输出
		val resultStream = aggStream
				.keyBy("windowEnd")
				.process(new TopNHotItems(5))
		resultStream.print()
		env.execute("HotItems Analysis job")
	}
}


//自定义预聚合函数，来一条数据就加 1
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
	//初始化状态
	override def createAccumulator(): Long = 0L

	override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

	override def getResult(accumulator: Long): Long = accumulator

	override def merge(a: Long, b: Long): Long = a + b
}


//自定义平均值聚合函数
class AvgAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
	override def createAccumulator(): (Long, Int) = (0L, 0)

	override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) =
		(accumulator._1 + value.timestamp, accumulator._2 + 1)

	override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2.toDouble

	override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
}

//自定义窗口函数，结合window函数包装成样例类
//class ItemCountWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
//	override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
//		val itemId = key.asInstanceOf[Tuple1[Long]].f0
//		val windowEnd = window.getEnd
//		val count = input.iterator.next()
//		out.collect(ItemViewCount(itemId, windowEnd, count))
//	}
//}

//自定义窗口函数，结合window函数包装成样例类
class ItemCountWindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
	override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
		val itemId = key
		val windowEnd = window.getEnd
		val count = input.iterator.next()
		out.collect(ItemViewCount(itemId, windowEnd, count))
	}
}


class TopNHotItems(n: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
	//定义一个ListState,用来保存当前窗口所有count结果
	lazy val itemCountListState: ListState[ItemViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemcount-list", classOf[ItemViewCount]))


	override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
		//每来一条数据就保存到状态中
		itemCountListState.add(value)
		//注册定时器，在windowEnd+100ms
		ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
	}

	//ctrl+o可以重载类方法，定时器触发时从状态中取数据，然后排序输出
	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
		//将状态中的数据提取到ListBuffer中
		val allItemCountList: ListBuffer[ItemViewCount] = ListBuffer()
		import scala.collection.JavaConversions._
		for (itemCount <- itemCountListState.get()) {
			allItemCountList += itemCount
		}

		//按照count值大小排序
		val sortedItemCountList = allItemCountList.sortBy(_.count)(Ordering.Long.reverse).take(n)

		//清空状态
		itemCountListState.clear()

		//将TopN格式化为字符串
		val result = new StringBuilder
		result.append("时间：").append(new Timestamp(timestamp - 100)).append("\n")
		//遍历sorted列表，输出TopN
		for (i <- sortedItemCountList.indices) {
			val currentItemCount = sortedItemCountList(i)
			result.append("Top").append(i + 1).append(":")
					.append(" 商品id=").append(currentItemCount.itemId)
					.append(" 访问量=").append(currentItemCount.count)
					.append("\n")
		}
		result.append("========================================\n\n")
		//控制输出频率
		Thread.sleep(1000)
		out.collect(result.toString())
	}
}
