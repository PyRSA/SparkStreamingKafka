package com.summer.sparkstream

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import scalikejdbc.{ConnectionPool, DB, DBSession, WrappedResultSet, scalikejdbcSQLInterpolationImplicitDef}


/**
 * 原子性处理偏移量：将偏移量存储在事物里面
 */
object KafkaOffsetTransaction {
  def main(args: Array[String]): Unit = {
    val args: Array[String] = Array("hadoop.summer.com:9092", "kafka-stream-transaction", "sparkstream")
    if (args == null || args.length < 3) {
      println(
        """
          |Usage: java -jar MainClass <brokers.list> <group.id> <topic>
          |""".stripMargin)
      System.exit(-1)
    }

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("transaction")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // kafka topic 相关配置
    val Array(brokers, groups, topics) = args
    val topic: Set[String] = topics.split(",").toSet
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groups,
      "auto.offset.reset" -> "smallest"
    )

    // 数据库相关配置
    val driver = "com.mysql.jdbc.Driver"
    val jdbcUrl = "jdbc:mysql://hadoop101:3306/kafka_offset_transaction"
    val username = "root"
    val password = "toor"

    // 初始化连接
    Class.forName(driver)
    ConnectionPool.singleton(jdbcUrl, username, password)

    // 隐式参数转换
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session: DBSession =>
      sql"select topic, partid, offset from mytopic"
        .map { r: WrappedResultSet =>
          // topic partition offset
          TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
        }
        .list.apply().toMap // 转换结果类型为Map
    }

    val messageHandler: MessageAndMetadata[String, String] => (String, String) = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder,
      StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    messages.foreachRDD((rdd: RDD[(String, String)], time: Time) => {
      if (!rdd.isEmpty()) {
        println(s"=================== Time $time =================")
        rdd.foreachPartition((partition: Iterator[(String, String)]) => {
          // 偏移量
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          // 分区对应的偏移量
          val parOffsetRange: OffsetRange = offsetRanges(TaskContext.get.partitionId)

          // 使用Scala中的 localTx 开启事物
          DB.localTx {
            implicit session: DBSession =>
              // 数据获取
              partition.foreach((msg: (String, String)) => {
                // 使用scalike的batch插入数据
                val name: String = msg._2.split(",")(0)
                val id: String = msg._2.split(",")(1)
                sql"""insert into mydata(name, id) values (${name}, ${id})""".execute().apply()
              })
              // 1 / 0 // 测试异常，事物是否回滚
              // 偏移量
              sql"""update mytopic set offset = ${parOffsetRange.untilOffset} where topic = {parOffsetRange.topic}
                   and partid = ${parOffsetRange.partition}""".update.apply()
          }
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
