package com.summer.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.filter.{BinaryComparator, CompareFilter, RowFilter}
import org.apache.hadoop.hbase.util.Bytes

import java.io.IOException
import java.util
import scala.collection.mutable


/**
 * 手动从hbase中读取上一次消费的offset信息<p>

    有：从指定的offset位置开始消费<p>
    无：从offset为0或者最小的位置开始消费<p>
    使用指定offset从kafka中拉取数据<p>
    拉取到数据之后进行业务处理<p>
    指定HBase进行offset的更新<p>
    Table hadoop-topic-offset<p>
      字段	        注释
      topic-group	  行键（Rowkey）
      cf	          列族（Columns）
      partition	    分区
      offset	      偏移量
      connection  	连接
 */
object HBaseConnectionPool {
  private val pool = new util.LinkedList[Connection]()

  // 创建HBase配置
  private val config: Configuration = HBaseConfiguration.create()
  config.set("hbase.zookeeper.quorum", "hadoop101:2181,hadoop102:2181,hadoop103:2181")
  // 初始化5个连接对象

  try {
    for (_ <- 1 to 5) {
      pool.push(ConnectionFactory.createConnection(config))
    }
  } catch {
    case e: IOException => e.printStackTrace()
  }

  // 获取连接对象
  def getConnection: Connection = {
    while (pool.isEmpty) {
      try {
        println("Connection pool is empty!")
        Thread.sleep(1000)
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }
    }
    pool.poll()
  }

  // 回收连接对象
  def release(connection: Connection): Unit = {
    pool.push(connection)
  }

  // 获取字段值
  def getCellValue(connection: Connection, tableName: String, rowkey: String, columnFamily: String): mutable.Map[Int, Long] = {
    // 定义一个map用于存储分区和偏移量
    val partitionToOffset = new mutable.HashMap[Int, Long]()
    try {
      // 获取表对象
      val table: Table = connection.getTable(TableName.valueOf(tableName))
      // 获取一个扫描器
      val scan = new Scan()
      // 创建一个过滤器
      val filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(rowkey)))
      // 设置过滤规则
      scan.setFilter(filter)
      scan.addFamily(Bytes.toBytes(columnFamily))
      // 获取结果集
      val resultScanner: ResultScanner = table.getScanner(scan)
      // 遍历结果集
      import scala.collection.JavaConversions._
      for (result <- resultScanner) {
        val cells: util.List[Cell] = result.listCells()
        for (cell <- cells) {
          val partition: Int = Bytes.toInt(CellUtil.cloneQualifier(cell))
          val offset: Long = Bytes.toLong(CellUtil.cloneValue(cell))
          partitionToOffset.put(partition, offset)
        }
      }
      table.close()
    } catch {
      case ex: IOException => ex.printStackTrace()
    }
    partitionToOffset
  }

  // 保存偏移量到HBase
  def setCellValue(connection: Connection, tableName: String,  rowkey: String, columnFamily: String,
                   column: String, value: String): Unit = {
    try {
      val table: Table = connection.getTable(TableName.valueOf(tableName))
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))
      table.put(put)
      table.close()
    } catch {
      case ex: IOException => ex.printStackTrace()
    }
  }
}
