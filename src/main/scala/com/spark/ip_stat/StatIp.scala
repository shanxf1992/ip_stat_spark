package com.spark.ip_stat

import java.sql.{Connection, PreparedStatement, Statement}

import com.spark.utils.JDBCUtils
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.{ArrayHandler, ArrayListHandler}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * 根据用户当前的Ip, 统计在某个区域内用户的数量
  */
object StatIp {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("rdd");
        // 构建spark上下文
        val sc = new SparkContext(sparkConf)

        //获取用户访问数据
        val httpRdd: RDD[String] = sc.textFile("C:\\git_projects\\ip_http.format").map(line => line.split("\\|")(1))
        //获取ip信息数据
        val ipRDD: RDD[(String, String, String, String)] = sc.textFile("C:\\git_projects\\ip.txt").map(line => {
            val fields: Array[String] = line.split("\\|")
            (fields(2), fields(3), fields(fields.length-2), fields(fields.length-1))
        })

        //1 对ip信息rdd进行广播
        val ipBroadcast: Broadcast[Array[(String, String, String, String)]] = sc.broadcast(ipRDD.collect())

        //2 获取每个访问ip对应的经纬度信息
        val mapPartitions: RDD[((String, String), Int)] = httpRdd.mapPartitions(iter => {
            val ipArray: Array[(String, String, String, String)] = ipBroadcast.value

            val tuples: Iterator[((String, String), Int)] = for {
                ip <- iter
                ipLong = ip2Long(ip)
                index = findIpArea(ip, ipArray)
            } yield {
                ((ipArray(index.toInt)._3, ipArray(index.toInt)._4), 1)
            }

            tuples
        })

        val result: RDD[((String, String), Int)] = mapPartitions.reduceByKey(_ + _)

        //3. 将结果写入到数据库
        // result.foreachPartition(iter => {
        //
        //     //3.1 获取数据连接池
        //     val connection: Connection = JDBCUtils.getConnection
        //
        //     val sql: String = "insert into iplocation(longitude, latitude, total_count) values(?, ?, ?)"
        //     val ps: PreparedStatement = connection.prepareStatement(sql)
        //
        //     for(line <- iter){
        //         ps.setString(1, line._1._1)
        //         ps.setString(2, line._1._2)
        //         ps.setInt(3, line._2)
        //         ps.addBatch()
        //     }
        //
        //     try{
        //         ps.executeBatch()
        //     } catch {
        //         case e: Exception => println(e)
        //     } finally {
        //         ps.close()
        //         connection.close()
        //     }
        // })

       //4. 使用 DBUtils 将结果写入数据库
        result.foreachPartition(iter => {
            // 获取 queryRunner 对象
            val queryRunner = new QueryRunner(JDBCUtils.getDataSource)
            val sql: String = "insert into iplocation(longitude, latitude, total_count) values(?, ?, ?)"
            val params = new ListBuffer[Array[AnyRef]]()

            // 构建参数
            for(line <- iter) {
                val array: Array[AnyRef] = Array(line._1._1, line._1._2, line._2.toString)
                params.append(array)
            }
            //批量插入
            val insertBatch: Any = queryRunner.insertBatch(sql, new ArrayListHandler(), params.toArray)
        })
    }


    /**
      * 将IP转化成对应的Long类型数字
      * @param ip
      */
    def ip2Long(ip: String): Long = {
        val fields: Array[String] = ip.split("\\.")
        var ipNum: Long = 0L
        for(field <- fields) {
            ipNum = field.toLong | ipNum << 8
        }
        ipNum
    }

    /**
      * 从ipArray中查找到ip属于哪个区间, 返回对应的索引
      * @param ip String
      * @param ipArray Array[(String, String, String, String)]
      * @return
      */
    def findIpArea(ip: String, ipArray: Array[(String, String, String, String)]) : Long = {

        val ipLong = ip2Long(ip)
        var left = 0
        var right = ipArray.length - 1

        while (left <= right) {
            var mid = (left + right) / 2
            if (ipLong < ipArray(mid)._1.toLong){
                right = mid - 1
            }else if (ipLong > ipArray(mid)._2.toLong) {
                left = mid + 1
            }else {
                return mid
            }
        }
        -1 // 没有就返回 -1
    }
}
