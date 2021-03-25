package com.haozj.hotitemanalysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerUtil {
	def main(args: Array[String]): Unit = {
		writeToKafkaWithTopic("test")
	}

	def writeToKafkaWithTopic(topic: String): Unit ={
		val properties = new Properties()
		properties.setProperty("bootstrap.servers", "121.36.41.71:9092")
		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

		//创建一个kakfaProducer来发送数据
		val producer = new KafkaProducer[String,String](properties)
		//从文件中读取数据逐条发送
		val bufferedSource = io.Source.fromFile("F:\\cala\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
		for (line <-bufferedSource.getLines()){
			val record = new ProducerRecord[String,String](topic,line)
			producer.send(record)
		}
	}
}
