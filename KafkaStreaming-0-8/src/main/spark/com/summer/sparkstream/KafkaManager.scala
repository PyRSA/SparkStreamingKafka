package com.summer.sparkstream

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

import scala.collection.mutable
import scala.collection.JavaConversions._

object KafkaManager {
  // 获取偏移量
  def getOffsets(topics: Set[String], group: String, curator: CuratorFramework): Map[TopicAndPartition, Long] = {
    val offsets: mutable.Map[TopicAndPartition, Long] = mutable.Map[TopicAndPartition, Long]()
    for (topic <- topics) {
      val path = s"${topic}/${group}"
      checkExists(path, curator)
      for (partition <- curator.getChildren.forPath(path)) {
        val path = s"${path}/${partition}"
        val offset: Long = new String(curator.getData.forPath(path)).toLong
        offsets.put(TopicAndPartition(topic, partition.toInt), offset)
      }
    }
    offsets.toMap
  }

  // 创建消息
  def createMessage(ssc: StreamingContext, kafkaParams: Map[String, String], topic: Set[String],
                    curator: CuratorFramework): InputDStream[(String, String)] = {
    // 从zookeeper中获取偏移量
    val offsets: Map[TopicAndPartition, Long] = getOffsets(topic, kafkaParams("group.id"), curator)
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

  // 设置偏移量
  def setOffsets(offsetRanges: Array[OffsetRange], group: String, curator: CuratorFramework): Unit = {
    for (offsetRange <- offsetRanges) {
      val path = s"${offsetRange.topic}/${group}/${offsetRange.partition}"
      checkExists(path, curator)
      curator.setData().forPath(path, offsetRange.untilOffset.toString.getBytes())
    }
  }

  // 检测是否存在
  def checkExists(path: String, curator: CuratorFramework): Unit = {
    if (curator.checkExists().forPath(path) == null) {
      // 不存在则创建
      curator.create().creatingParentsIfNeeded().forPath(path)
    }
  }
}
