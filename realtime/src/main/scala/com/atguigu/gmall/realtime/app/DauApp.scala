package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstant
import com.atguigu.gmall.common.util.MyEsUtil
import com.atguigu.gmall.realtime.bean.StartUpLog
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

    /*inputDstream.foreachRDD{
      rdd => println(rdd.map(_.value()).collect().mkString("\n"))
    }*/

    val startUpLogStream: DStream[StartUpLog] = inputDstream.map {
      record =>
        val jsonStr: String = record.value()
        val startUpLog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])
        val date: Date = new Date(startUpLog.ts)
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
        val dateArr: Array[String] = dateStr.split(" ")
        startUpLog.logDate = dateArr(0)
        startUpLog.logHour = dateArr(1).split(":")(0)
        startUpLog.logHourMinute = dateArr(1)

        startUpLog
    }
    
    // 利用redis进行去重过滤
    // 方法一：每条数据执行都要获取一个jedis连接
    /*startUpLogStream.filter{
      startUpLog =>
        val jedis: Jedis = RedisUtil.getJedisClient
        val key = "dau:" + startUpLog.logDate
        !jedis.sismember(key, startUpLog.mid)
    }*/

    // 方法二：在外面获取的集合只执行一次，而我们需要周期性执行，该方法错误
    /*val curdate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val jedis: Jedis = RedisUtil.getJedisClient
    val key = "dau:" + curdate
    val dauSet: util.Set[String] = jedis.smembers(key)
    val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
    startUpLogStream.filter{
      startUpLog =>
        val dauSet: util.Set[String] = dauBC.value
        !dauSet.contains(startUpLog.mid)
    }*/

    // 方法三：
    val filteredDStream: DStream[StartUpLog] = startUpLogStream.transform {
      rdd => // 在driver中执行
        println("过滤前：" + rdd.count())
        val curdate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        val jedis: Jedis = RedisUtil.getJedisClient
        val key = "dau:" + curdate
        val dauSet: util.Set[String] = jedis.smembers(key)
        val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
        val filteredRDD: RDD[StartUpLog] = rdd.filter {
          startUpLog => // 在executor中执行
            val dauSet: util.Set[String] = dauBC.value
            !dauSet.contains(startUpLog.mid)
        }
        println("过滤后：" + filteredRDD.count())
        filteredRDD
    }

    // 可能会存在每个5秒只能的数据重复，5秒之内数据清单还没更新，而同一用户在此期间登录了多次
    // 去重思路：把相同的mid的数据分成一组，每组取第一个
    val groupByMidDStream: DStream[(String, Iterable[StartUpLog])] = filteredDStream.map(startUpLog=>(startUpLog.mid,startUpLog)).groupByKey()

    val distinctDStream: DStream[StartUpLog] = groupByMidDStream.flatMap {
      case (mid, startUpLogItr) =>
        startUpLogItr.take(1)
    }

    // 保存到redis中
    distinctDStream.foreachRDD{
      rdd =>  // 在driver中执行

        // 该方法获取不到jedis连接
        /*rdd.foreach{  // 在executor中执行
          startUpLog =>
            // redis: type=>set key=>dau:2019-06-03 value:mids
            val jedis: Jedis = RedisUtil.getJedisClient
            val key = "dau:" + startUpLog.logDate
            val value = startUpLog.mid
            jedis.sadd(key, value)
            jedis.close()
        }*/

        rdd.foreachPartition{
          startUpLogItr =>  // 在executor中执行
            // redis: type=>set key=>dau:2019-06-03 value:mids
            val jedis: Jedis = RedisUtil.getJedisClient
            val list: List[StartUpLog] = startUpLogItr.toList
            for(startUpLog <- list){
              val key = "dau:" + startUpLog.logDate
              val value = startUpLog.mid
              jedis.sadd(key, value)
              println(startUpLog) //往es中保存
            }

            MyEsUtil.indexBulk(GmallConstant.ES_INDEX_DAU, list)
            jedis.close()
        }

    }

    
    
    ssc.start()

    ssc.awaitTermination()

  }


}
