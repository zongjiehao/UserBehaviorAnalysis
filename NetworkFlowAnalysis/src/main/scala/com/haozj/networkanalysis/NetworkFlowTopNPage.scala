package com.haozj.networkanalysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


//定义输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//聚合结果样例类
case class PageViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlowTopNPage {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		val inputStream = env.readTextFile("F:\\cala\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
		val dataStream = inputStream.map(
			data => {
				val dataArray = data.split(" ")
				val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
				val timestamp = simpleDateFormat.parse(dataArray(3)).getTime
				ApacheLogEvent(dataArray(0), dataArray(1), timestamp, dataArray(5), dataArray(6))
			}
		).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(60)) {
			override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
		}) //大概查看数据的最大乱序程序为60s

		//开窗聚合
		val aggStream = dataStream
				.keyBy(_.url)
				.timeWindow(Time.minutes(10), Time.seconds(5))
				.aggregate(new PageCountAgg(), new PageCountWindowResult())

		val resultStream = aggStream
				.keyBy(_.windowEnd)
				.process(new TopNPageView(5))
		resultStream.print()
		env.execute("HotItems Analysis job")
	}
}

//自定义聚合函数
class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
	override def createAccumulator(): Long = 0L

	override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

	override def getResult(accumulator: Long): Long = accumulator

	override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数[预聚合的结果是这里的输入]
class PageCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
	override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
		out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
	}
}

class TopNPageView(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
	//定义一个listState保存所有聚合结果
	lazy val listState = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("topPage", classOf[PageViewCount]))

	override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
		listState.add(value)
		//注册一个定时器
		ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
	}

	//定时器触发时从状态中取数据，然后排序输出
	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
		//将状态中的数据取出来，存到listBuffer里
		val pageCountList: ListBuffer[PageViewCount] = ListBuffer()
				val iter = listState.get().iterator()
				while (iter.hasNext){
					pageCountList += iter.next()
				}

		listState.clear()
		val sortedPageViewList = pageCountList.sortWith(_.count>_.count).take(n)

		//将TopN格式化为字符串
		val result = new StringBuilder
		result.append("时间：").append(new Timestamp(timestamp - 100)).append("\n")
		//遍历sorted列表，输出TopN
		for (i <- sortedPageViewList.indices) {
			val currentItemCount = sortedPageViewList(i)
			result.append("Top").append(i + 1).append(":")
					.append(" url=").append(currentItemCount.url)
					.append(" 访问量=").append(currentItemCount.count)
					.append("\n")
		}
		result.append("========================================\n\n")
		//控制输出频率
		Thread.sleep(1000)
		out.collect(result.toString())
	}
}
