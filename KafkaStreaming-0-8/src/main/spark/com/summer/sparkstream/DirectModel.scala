package com.summer.sparkstream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
 * Direct方式<p>
 *  Direct方式的特点：<p>
 *
 *  简化的并行性：不需要创建多个输入Kafka流并将其合并。 使用directStream，Spark Streaming将创建与使用Kafka分区一样多的RDD分区，
 *    这些分区将全部从Kafka并行读取数据。所以在Kafka和RDD分区之间有一对一的映射关系。<p>
 *
 *  效率：在第一种方法中实现零数据丢失需要将数据存储在预写日志中，这会进一步复制数据。这实际
 *  上是效率低下的，因为数据被有效地复制了两次:一次是Kafka，另一次是由预先写入日志（Write
 *  Ahead Log）复制。这个第二种方法消除了这个问题，因为没有接收器，因此不需要预先写入日志。
 *  只要Kafka数据保留时间足够长。<p>
 *
 *  正好一次（Exactly-once）的语义：第一种方法使用Kafka的高级API来在Zookeeper中存储消耗的偏移量。
 *  传统上这是从Kafka消费数据的方式。虽然这种方法（结合提前写入日志）可以确保零数据丢失（即至少一次语义），但是在某些失败情况下，
 *  有一些记录可能会消费两次。发生这种情况是因为Spark Streaming可靠接收到的数据与Zookeeper跟踪的偏移之间的不一致。因此，
 *  在第二种方法中，我们使用不使用Zookeeper的简单Kafka API。在其检查点内，Spark Streaming跟踪偏移量。这消除了Spark Streaming
 *  和Zookeeper/Kafka之间的不一致，因此Spark Streaming每次记录都会在发生故障的情况下有效地收到一次。为了实现输出结果的一次语义，
 *  将数据保存到外部数据存储区的输出操作必须是幂等的，或者是保存结果和偏移量的原子事务。<p>
 *
 * Kafka基于Direct模式 一
 */
object DirectModel {
  def main(args: Array[String]): Unit = {

    val args: Array[String] = Array("hadoop.summer.com:9092", "kafka-stream-direct", "sparkstream")
    if (args == null || args.length < 3) {
      println(
        """
          |Usage: java -jar MainClass <brokers.list> <group.id> <topic>
          |""".stripMargin)
      System.exit(-1)
    }

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka direct")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val Array(brokers, groups, topics) = args
    val topic: Set[String] = topics.split(",").toSet
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groups,
      "auto.offset.reset" -> "smallest"
    )

    val inputDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder,
      StringDecoder](ssc, kafkaParams, topic)

    val result: DStream[(String, Int)] = inputDS.flatMap((_: (String, String))._2.split("\\s+")).map((_: String, 1))
      .reduceByKey((_: Int) + (_: Int))

    result.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
      if (!rdd.isEmpty()) {
        println(s"==================== $time ====================")
        rdd.foreach(println)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
