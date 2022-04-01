package com.summer.sparkstream

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 幂等操作
 * 创建测试的mysql数据库: create database kafka_offset_idempotent;<p>
 * 建表: create table offset_idempotent(name varchar(20), order_id varchar(100) primary key);<p>
 * 1新建topic：idempotent<p>
 * kafka-topics.sh --bootstrap-server hadoop.summer.com:9092/kafka --create --topic idempotent --partitions 3 --replication-factor 1<p>
 * 运行程序之后，向idempotent发送数据，数据格式为 “字符,数字” 比如 abc,3<p>
 * kafka-console-producer.sh --topic idempotent --broker-list hadoop.summer.com:9092<p>
 */
object KafkaOffsetIdempotent {
  def main(args: Array[String]): Unit = {
    val args: Array[String] = Array("hadoop.summer.com:9092", "kafka-stream-idempotent", "sparkstream")
    if (args == null || args.length < 3) {
      println(
        """
          |Usage: java -jar MainClass <brokers.list> <group.id> <topic>
          |""".stripMargin)
      System.exit(-1)
    }

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka offset idempotent")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val Array(brokers, groups, topics) = args
    val topic: Set[String] = topics.split(",").toSet
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groups,
      "auto.offset.reset" -> "smallest"
    )

    val jdbcURL = "jdbc:mysql://hadoop101:3306/kafka_offset_idempotent"
    val jdbcUser = "root"
    val jdbcPassword = "toor"

    val messages: InputDStream[(String, String)] = KafkaManager.createMessage(ssc, kafkaParams, topic, client)

    messages.foreachRDD((rdd: RDD[(String, String)]) => {
      if (!rdd.isEmpty()) {
        rdd.map((x: (String, String)) => x._2).foreachPartition((partition: Iterator[String]) => {
          // 获取connection
          val dbConn: Connection = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPassword)
          partition.foreach((msg: String) => {
            // 安装指定格式切分
            val Array(name, order_id) = msg.split(",")
            // 幂等操作：如果主键相同，则覆盖这个结果
            val sql: String =
              s"""
                 |INSERT INTO `offset_idempotent`(`name`, `order_id`)
                 |VALUES ('${name}', '${order_id}')
                 |ON DUPLICATE KEY UPDATE `name`='${name}'
                 |""".stripMargin
            val statement: PreparedStatement = dbConn.prepareStatement(sql)
            statement.execute()
          })
          dbConn.close()
        })
        // 提交偏移量
        KafkaManager.setOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, kafkaParams("group.id"), client)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  private val client: CuratorFramework = {
    val client: CuratorFramework = CuratorFrameworkFactory.builder()
      .connectString("hadoop101:2181,hadoop102:2181,hadoop103:2181")
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .namespace("kafka/consumers/offset")
      .build()
    client.start()
    client
  }

}
