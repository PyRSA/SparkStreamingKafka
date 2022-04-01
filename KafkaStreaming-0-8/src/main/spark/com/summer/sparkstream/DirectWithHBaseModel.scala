package com.summer.sparkstream

import com.summer.util.HBaseConnectionPool
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import scala.collection.mutable

/**
 * kafka基于direct方式三——使用HBase管理offset
 */
object DirectWithHBaseModel {

  def getOffsets(topics: Set[String], group: String): Map[TopicAndPartition, Long] = {
    val offsets: mutable.Map[TopicAndPartition, Long] = mutable.Map[TopicAndPartition, Long]()
    // 获取连接
    val connection: Connection = HBaseConnectionPool.getConnection
    // 设置表名，列族，rowkey
    val tableName = "spark_stream_kafka_hbase"
    val columnFamily = "kafka_offset"
    for (topic <- topics) {
      val rowkey = s"${topic}-${group}"
      val partitionToOffset: mutable.Map[Int, Long] = HBaseConnectionPool.getCellValue(connection, tableName, rowkey, columnFamily)
      partitionToOffset.foreach{
        case (partition, offset) =>
          offsets.put(TopicAndPartition(topic, partition), offset)
      }
    }
    HBaseConnectionPool.release(connection)
    offsets.toMap
  }

  def createMessage(ssc: StreamingContext, kafkaParams: Map[String, String], topic: Set[String]): InputDStream[(String, String)] = {
    // 从zookeeper中获取偏移量
    val offsets: Map[TopicAndPartition, Long] = getOffsets(topic, kafkaParams("group.id"))
    var messages: InputDStream[(String, String)] = null
    if (offsets.isEmpty) {
      // 获取的偏移量为空，则从0开始读
      messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic)
    } else {
      // 从指定的偏移量开始读取数据
      val messageHandler: MessageAndMetadata[String, String] => (String, String) = (msg: MessageAndMetadata[String, String]) => (msg.key(), msg.message())
      messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, offsets, messageHandler)
    }
    messages
  }

  def setOffsets(offsetRanges: Array[OffsetRange], group: String): Unit = {
    val connection: Connection = HBaseConnectionPool.getConnection
    val tableName = "spark_stream_kafka_hbase"
    val columnFamily = "kafka_offset"
    for (offsetRange <- offsetRanges) {
      val rowkey = s"${offsetRange.topic}-${group}"
      val partition: Int = offsetRange.partition
      // 获取偏移量
      val offset: Long = offsetRange.untilOffset
      // 存储偏移量至HBase
      HBaseConnectionPool.setCellValue(connection, tableName, rowkey, columnFamily, partition.toString, offset.toString)
    }
    HBaseConnectionPool.release(connection)
  }

  def main(args: Array[String]): Unit = {
    val args: Array[String] = Array("hadoop.summer.com:9092", "kafka-stream-direct-hbase", "sparkstream")
    if (args == null || args.length < 3) {
      println(
        """
          |Usage: java -jar MainClass <brokers.list> <group.id> <topic>
          |""".stripMargin)
      System.exit(-1)
    }

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka direct hbase")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val Array(brokers, groups, topics) = args
    val topic: Set[String] = topics.split(",").toSet
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groups,
      "auto.offset.reset" -> "smallest"
    )

    val result: InputDStream[(String, String)] = createMessage(ssc, kafkaParams, topic)
    result.foreachRDD((rdd: RDD[(String, String)], time: Time) => {
      if (rdd.isEmpty()) {
        println(s"==================== $time ====================")
        println(s"rdd.count() = ${rdd.count()}")
        // 更新偏移量
        setOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, kafkaParams("group.id"))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
