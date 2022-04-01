package com.summeer.sparkstream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang

object DirectWordCount {
  def main(args: Array[String]): Unit = {
    val args: Array[String] = Array("hadoop101:9092,hadoop102:9092,hadoop103:9092", "kafka-stream", "kafkaStream")
    if (args == null || args.length < 3) {
      println(
        """
          |Usage: java -jar MainClass <brokers.list> <group.id> <topic>
          |""".stripMargin)
      System.exit(-1)
    }

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka stream")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // 配置检查点
    ssc.checkpoint("checkpoint")

    /**
     * kafka: auto.offset.reset取值含义解释：<p>
     *    earliest：当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费<p>
     *    latest：当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据<p>
     *    none：topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常<p>
     *
     * kafka: enable.auto.commit参数解释：<p>
     *    bool: true or false <p>
     *    spark streaming整合kafka时，一般需要将`enable.auto.commit`设置为false，即禁用自动提交偏移量。<p>
     */
    val Array(brokers, groups, topics) = args
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groups,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: lang.Boolean)
    )

    /**
     * def createDirectStream[K, V]( <p>
     *    ssc: StreamingContext, <p>
     *    locationStrategy: LocationStrategy, <p>
     *    consumerStrategy: ConsumerStrategy[K, V] <p>
     *  ): InputDStream[ConsumerRecord[K, V]] <p>
     * <p>
     *  相关参数解释： <p>
     *    LocationStrategies <p>
     *      LocationStrategies.PreferConsistent：在可用的executor上均匀分布分区； <p>
     *      LocationStrategies.PreferBroker：如果你的executor与Kafka的Broker节点在同一台物理机上，使用PreferBrokers
     *      ，这更倾向于在该节点上安排KafkaLeader对应的分区； <p>
     *      LocationStrategies.PreferFixed：如果发生分区之间数据负载倾斜，使用PreferFixed。这允许你指定分区和主机之间的显示映射（任何未指定的分区将使用一致的位置)； <p>
     * <p>
     *    ConsumerStrategies <p>
     *      ConsumerStrategies.Subscribe：可以指定订阅的固定主题集合，可以指定多个主题，但是主题中的数据格式应保持一致； <p>
     *      ConsumerStrategies.SubscribePattern：使用正则表达式匹配订阅的主题； <p>
     *      ConsumerStrategies.Assign：可指定固定的分区集合 <p>
     *  如果上述的策略无法满足需求，那么可以使用ConsumerStrategy这个公共类自行拓展，自定义消费策略。 <p>
     */
    val topic: Array[String] = Array(topics)
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.
      createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topic, kafkaParams))

    val result: DStream[(String, Int)] = kafkaStream
      .flatMap((_: ConsumerRecord[String, String]).value().split("\\s+"))
      .map((_: String, 1))
      .updateStateByKey(
        (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
          iter.flatMap {
            case (x, y, z) => Some(y.sum + z.getOrElse(0)).map((count: Int) => (x, count))
          }
        },
        new HashPartitioner(ssc.sparkContext.defaultParallelism),
        true
    )

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 偏移量管理有如下几种方式
   *  利用chechpoint存储偏移量：如果启用spark的checkpoint，则偏移量将存储在检查点中。
   *                          此中方法有缺陷，当应用程序代码更改后，可能会丢失偏移量数据。
   *  保存在kafka中：利用kafka的commitAsync API来手动提交偏移量，与checkpoint相比，它的好处是，
   *                无论您对应用程序代码进行如何更改和升级，kaka对偏移量都是持久存储的（存储在单独的topic中），
   *                Kafka不是事务性的，输出必须仍然保证是幂等的。
   *
   *  eg:
   *   stream.foreachRDD { rdd =>
   *      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
   *      // some time later, after outputs have completed
   *      // commitAsync最好在计算完成后调用
   *      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
   *  }
   *
   *  自己存储管理偏移量：将偏移量和计算结果进行事务性存储，如存储在数据库中，当失败时可以进行回滚。
   *  eg:
   *     // The details depend on your data store, but the general idea looks like this
   *
   *     // begin from the the offsets committed to the database
   *     val fromOffsets = selectOffsetsFromYourDatabase.map { resultSet =>
   *     new TopicPartition(resultSet.string("topic"), resultSet.int("partition")) -> resultSet.long("offset")
   *     }.toMap
   *
   *     val stream = KafkaUtils.createDirectStream[String, String](
   *     streamingContext,
   *     PreferConsistent,
   *     Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
   *     )
   *
   *     stream.foreachRDD { rdd =>
   *     val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
   *
   *     val results = yourCalculation(rdd)
   *
   *     // begin your transaction
   *
   *     // update results
   *     // update offsets where the end of existing offsets matches the beginning of this batch of offsets
   *     // assert that offsets were updated correctly
   *
   *     // end your transaction
   *     }
   *
   */

}
