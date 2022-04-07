package com.summer.sparkstream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{BinaryComparator, RowFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, CompareOperator, HBaseConfiguration, TableName}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.{JavaDStream, JavaInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

import java.io.IOException
import scala.collection.mutable

/**
 * Kafka delivery semantics in the case of failure depend on how and when offsets are stored.
 * Spark output operations are at-least-once. So if you want the equivalent of exactly-once semantics,
 * you must either store offsets after an idempotent output, or store offsets in an atomic transaction alongside output.
 * There is Spark Streaming how to store Kafka topic offset with HBase.
 */
object KafkaTopicOffsetSaveToHBase {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage: JavaDirectKafkaOffsetSaveToHBase <kafka-brokers> <topics> <topic-groupid> <zklist> <datatable>\n\n")
      System.exit(1)
    }

    var stream: JavaInputDStream[ConsumerRecord[String, String]] = null
    org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.toLevel("WARN"))

    val brokers: String = args(0) //Kafka brokers list， 用逗号分割
    val topics: String = args(1) //要消费的话题，目前仅支持一个，想要扩展很简单，不过需要设计一下保存offset的表，请自行研究
    val groupid: String = args(2) //指定消费者group
    val zklist: String = args(3) //hbase连接要用到zookeeper server list，用逗号分割
    val datatable: String = args(4) //想要保存消息offset的hbase数据表

    val sparkConf: SparkConf = new SparkConf().setAppName("JavaDirectKafkaOffsetSaveToHBase")
    val jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest", // 默认第一次执行应用程序时从Topic的首位置开始消费
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean) //不使用kafka自动提交模式，由应用程序自己接管offset
    )

    //此处把1个或多个topic解析为集合对象，因为后面接口需要传入Collection类型
    val topicsSet: Predef.Set[String] = topics.split(",").toSet

    //建立hbase连接
    val conf: Configuration = HBaseConfiguration.create() // 获得配置文件对象
    conf.set("hbase.zookeeper.quorum", zklist)
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    //获得连接对象
    val connection: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = connection.getAdmin

    //System.out.println(  " @@@@@@@@ " + admin ); //调试命令，判断连接是否成功
    val tableName: TableName = TableName.valueOf(datatable) //创建表名对象


    /*存放offset的表模型如下，请自行优化和扩展，请把每个rowkey对应的record的version设置为1（默认值），因为要覆盖原来保存的offset，而不是产生多个版本
     *----------------------------------------------------------------------------------------------
     *  rowkey           |  column family                                                          |
     *                   --------------------------------------------------------------------------
     *                   |  column:topic(string)  |  column:partition(int)  |   column:offset(long)|
     *----------------------------------------------------------------------------------------------
     * topic_partition   |   topic                |   partition             |    offset            |
     *----------------------------------------------------------------------------------------------
     */

    //判断数据表是否存在，如果不存在则从topic首位置消费，并新建该表；如果表存在，则从表中恢复话题对应分区的消息的offset
    val isExists: Boolean = admin.tableExists(tableName)
    System.out.println(s"表${tableName}是否存在：${isExists}")
    if (isExists) try { //HTable table = new HTable(conf, datatable);
      val table: Table = connection.getTable(tableName)
      val filter = new RowFilter(CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(topics + "_")))
      val s = new Scan
      s.setFilter(filter)
      val rs: ResultScanner = table.getScanner(s)
      // begin from the the offsets committed to the database
      val fromOffsets: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
      var s1: String = null
      var s2: Int = 0
      var s3: Long = 0L
      import scala.collection.JavaConversions._  // 导入Java集合与Scala集合互转的隐式转换
      for (r <- rs) {
        System.out.println("rowkey:" + new String(r.getRow))
        for (cell <- r.rawCells) {
          if (new String(CellUtil.cloneValue(cell)) == "topic") {
            s1 = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
            System.out.println("列族:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
              " 列:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + s1)
          }
          if (new String(CellUtil.cloneValue(cell)) == "partition") {
            s2 = Bytes.toInt(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
            System.out.println("列族:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
              " 列:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + s2)
          }
          if (new String(CellUtil.cloneValue(cell)) == "offset") {
            s3 = Bytes.toLong(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
            System.out.println("列族:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
              " 列:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + s3)
          }
        }
        fromOffsets.put(new TopicPartition(s1, s2), s3)
      }
      stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Assign[String, String](fromOffsets.keySet, kafkaParams, fromOffsets))
    } catch {
      case e: IOException =>
        // TODO Auto-generated catch block
        e.printStackTrace()
    }
    else { //如果不存在TopicOffset表，则从topic首位置开始消费
      stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
      //并创建TopicOffset表
      val tableNameBuilder: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)
      val familyDescriptorBuilder: ColumnFamilyDescriptorBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("topic_partition_offset"))
      tableNameBuilder.setColumnFamily(familyDescriptorBuilder.build())
      admin.createTable(tableNameBuilder.build())
      System.out.println(datatable + "表已经成功创建!----------------")
    }

    val jpds: JavaDStream[String] = stream.map((recode: ConsumerRecord[String, String]) => recode.value())


    stream.foreachRDD(new VoidFunction[JavaRDD[ConsumerRecord[String, String]]]() {
      @throws[Exception]
      override def call(rdd: JavaRDD[ConsumerRecord[String, String]]): Unit = {
        val offsetRanges: Array[OffsetRange] = rdd.rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offsetRange <- offsetRanges) {
          System.out.println("the topic is " + offsetRange.topic)
          System.out.println("the partition is " + offsetRange.partition)
          System.out.println("the fromOffset is " + offsetRange.fromOffset)
          System.out.println("the untilOffset is " + offsetRange.untilOffset)
          System.out.println("the object is " + offsetRange.toString)
          // begin your transaction
          // 为了保证业务的事务性，最好把业务计算结果和offset同时进行hbase的存储，这样可以保证要么都成功，要么都失败，最终从端到端体现消费精确一次消费的意境
          // update results
          // update offsets where the end of existing offsets matches the beginning of this batch of offsets
          // assert that offsets were updated correctly
          // HTable table = new HTable(conf, datatable);
          val table: Table = connection.getTable(tableName)
          val put = new Put(Bytes.toBytes(offsetRange.topic + "_" + offsetRange.partition))
          put.addColumn(Bytes.toBytes("topic_partition_offset"), Bytes.toBytes("topic"), Bytes.toBytes(offsetRange.topic))
          put.addColumn(Bytes.toBytes("topic_partition_offset"), Bytes.toBytes("partition"), Bytes.toBytes(offsetRange.partition))
          put.addColumn(Bytes.toBytes("topic_partition_offset"), Bytes.toBytes("offset"), Bytes.toBytes(offsetRange.untilOffset))
          table.put(put)
          System.out.println("add data Success!")
          // end your transaction
        }
        System.out.println("the RDD records counts is " + rdd.count)
      }
    })

    jssc.start()
    jssc.awaitTermination()
  }
}
