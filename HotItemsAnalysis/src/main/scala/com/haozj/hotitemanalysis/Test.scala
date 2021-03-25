package com.haozj.hotitemanalysis

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.Set

object Test {
	def main(args: Array[String]): Unit = {
		val ss = new Date()
		println("一般输出",ss)
		println("时间戳",ss.getTime)
		val simpleDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
		println("格式化结果",simpleDate.format(ss))
		val simpleDate1 = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒")
		println("格式化结果2",simpleDate1.format(ss.getTime))
		val simpleDate3 = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
		println("格式化结果3",simpleDate3.parse("20/05/2015:17:05:07"))
		println("格式化结果33",simpleDate3.parse("20/05/2015:17:05:07").getTime)

	}
}