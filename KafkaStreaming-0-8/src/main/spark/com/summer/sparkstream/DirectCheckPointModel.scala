package com.summer.sparkstream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
 * Kafka基于Direct模式 二   -- 使用`checkpoint`
 */
object DirectCheckPointModel {
  def main(args: Array[String]): Unit = {
    val args: Array[String] = Array("hadoop.summer.com:9092", "kafka-stream-direct-checkpoint", "sparkstream")
    if (args == null || args.length < 3) {
      println(
        """
          |Usage: java -jar MainClass <brokers.list> <group.id> <topic>
          |""".stripMargin)
      System.exit(-1)
    }

    val Array(brokers, groups, topics) = args
    val topic: Set[String] = topics.split(",").toSet
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groups,
      "auto.offset.reset" -> "smallest"
    )

    val checkpoint: String = "checkpoint"

    def createDS(): StreamingContext = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka direct checkpoint")
      val ssc = new StreamingContext(sparkConf, Seconds(2))
      ssc.checkpoint(checkpoint)

      val inputDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic)

      inputDS.foreachRDD((rdd: RDD[(String, String)], time: Time) => {
        if (!rdd.isEmpty()) {
          println(s"==================== $time ====================")
          println(s"rdd.count() = ${rdd.count()}")
        }
      })
      ssc
    }

    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint, createDS)

    ssc.start()
    ssc.awaitTermination()
  }

}
