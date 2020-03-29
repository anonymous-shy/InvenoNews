package com.donews.streaming

import java.time.{LocalDate, LocalDateTime}
import java.util
import java.util.Properties

import Constant._
import com.donews.utils._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.config.ConfigFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.JedisCluster

object NewsStreaming {
  val Log: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val resConf = ConfigFactory.load()
    val confTopic: String = resConf.getString("aliyun.KafkaTopics")
    val topics = Seq[String](confTopic)
    val group = resConf.getString("aliyun.KafkaGroup")
    val zkQuorums = resConf.getString("aliyun.ZKNodes")
    val brokerList = resConf.getString("aliyun.KafkaBrokers")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokerList,
      "auto.offset.reset" -> "smallest",
      "group.id" -> group,
      "zookeeper.connect" -> zkQuorums,
      "fetch.message.max.bytes" -> s"${15 * 1024 * 1024}"
    )
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // Kryo
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 优雅的关闭
      .set("spark.streaming.backpressure.enabled", "true") // 激活削峰功能
      .set("spark.streaming.backpressure.initialRate", "5000") // 第一次读取的最大数据值
      .set("spark.streaming.kafka.maxRatePerPartition", "2000") // 每个进程每秒最多从kafka读取的数据条数
      .set("spark.streaming.concurrentJobs", "4")
      .set("es.mapping.date.rich", "false")
      .set("es.index.read.missing.as.empty", "true")
    val esOptions = Map("es.index.auto.create" -> "true",
      "pushdown" -> "true",
      "es.nodes" -> "172.25.102.20,172.25.102.21,172.25.102.22",
      "es.port" -> "9200",
      "es.http.timeout" -> "2m",
      "es.mapping.id" -> ES_ID_KEY)
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(60))

    // 获取当前分钟数
    var minuteBrdcst = ssc.sparkContext.broadcast[Int](NewsProcess.getNowMinuteNo)
    // 定期更新黑名单
    var blacklistBrdcst: Broadcast[Array[String]] =
      ssc.sparkContext.broadcast[Array[String]](MysqlUtils.getBlackList(null))
    // 定期更新白名单
    var whitelistBrdcst: Broadcast[Array[String]] =
      ssc.sparkContext.broadcast[Array[String]](MysqlUtils.getSourceWhiteList(null))
    // 定期更新敏感词
    var senseWordsBrdcst: Broadcast[Array[String]] =
      ssc.sparkContext.broadcast[Array[String]](MysqlUtils.getSensetiveWords(null))

    val kafkaStream: InputDStream[(String, String)] = StreamingUtils.CreateKafkaDirectStream(ssc, kafkaParams, zkQuorums, topics)
    var offsetRanges = Array[OffsetRange]()
    kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(_._2)
      .foreachRDD(rdd => {
        val minute = NewsProcess.getNowMinuteNo
        val distance = Math.abs(minute - minuteBrdcst.value)
        if (distance >= 10 && distance <= 50) { //至少间隔10分钟才更新
          println(s"######## last_minute=${minuteBrdcst.value} current_minute=$minute minute distance: $distance")
          minuteBrdcst.unpersist()
          minuteBrdcst = ssc.sparkContext.broadcast[Int](minute)

          println("####### update blacklist #######")
          blacklistBrdcst =
            ssc.sparkContext.broadcast[Array[String]](MysqlUtils.getBlackList(blacklistBrdcst))

          println("####### update whitelist #######")
          whitelistBrdcst
            = ssc.sparkContext.broadcast[Array[String]](MysqlUtils.getSourceWhiteList(whitelistBrdcst))

          println("####### update sense words #######")
          senseWordsBrdcst =
            ssc.sparkContext.broadcast[Array[String]](MysqlUtils.getSensetiveWords(senseWordsBrdcst))

        }
        if (!rdd.isEmpty()) {
          rdd.foreachPartition(iter => {
            try {

            } catch {
              case ex: Exception => Log.error(ex.getMessage)
            }
          })
        }
      })

    ssc.start()
    ssc.awaitTermination()
  }

  // 主要处理逻辑
  private def mapPartitionsFunc(redis_task_prefix: String, news_mode_map: util.HashMap[String, Int],
                                blacklistBrdcst: Broadcast[Array[String]], whitelistBrdcst: Broadcast[Array[String]],
                                tags_rds_flg_Broadcast: Broadcast[Int], senseWordsBrdcst: Broadcast[Array[String]],
                                ES_INDEX: String, ES_ERROR_INDEX: String,
                                kvmessages: Iterator[String], article_min_words: Int,
                                topic: String, id_field_name: String): Iterator[util.HashMap[String, Object]] = {
    val redis = RedisClusterHelper.getConnection
    val messages = kvmessages.map(msg => {
      //将数据转换成jsonNode格式，便于数据验证
      val messageNode = JsonNodeUtils.getJsonNodeFromStringContent(msg).asInstanceOf[ObjectNode]
      // 2. 验证article_genre
      if (messageNode.hasNonNull("article_genre")) {
        common_validate(messageNode, ES_INDEX, ES_ERROR_INDEX, redis, tags_rds_flg_Broadcast, article_min_words, topic)
      } else {
        messageNode.put(ES_TYPE_KEY, "error_data_type")
        NewsProcess.addErrorInfo(messageNode, ES_ERROR_INDEX, "the field article_genre is Null!")
      }

      //验证黑名单白名单逻辑
      validateBWlist(ES_ERROR_INDEX, blacklistBrdcst, whitelistBrdcst, messageNode)
      //为数据生成es的id
      messageNode.put(Constant.ES_ID_KEY, produceRecordId(messageNode, ES_ERROR_INDEX, id_field_name))

//      NewsProcess.processInvenoOptionFields(messageNode)
      //替换旧字段
      NewsProcess.renameFields(messageNode)

      //因为有替换字段的过程，最后要验证字段长度，应该在替换字段后进行
      //验证一些字段的长度限制url,tags,title,author
      NewsProcess.validate_fields_length(messageNode, ES_ERROR_INDEX)
      if (!messageNode.has(Constant.FIELD_ERROR_KEY)) {
//        if (news_mode != -2) { //微博的不验证敏感词
//          NewsProcess.addSensetiveInfo(messageNode, senseWordsBrdcst.value)
//        }
        NewsProcess.addGrantLevelInfo(redis, messageNode)
      } else {
        messageNode.put(ES_INDEX_KEY, ES_ERROR_INDEX) //防止漏加错误索引
      }
      jsonNode2Map(messageNode) //最终将数据转换成map形式，便于发送至es
    })
    messages
  }

  protected def jsonNode2Map(messageNode: ObjectNode): util.HashMap[String, Object] = {
    val objectMapper = new ObjectMapper
    objectMapper.readValue(messageNode.toString, classOf[util.HashMap[String, Object]])
  }

  private def validateBWlist(ES_ERROR_INDEX: String,
                             blacklistBrdcst: Broadcast[Array[String]],
                             whitelistBrdcst: Broadcast[Array[String]], messageNode: ObjectNode): Unit = {
    if (!messageNode.has(Constant.FIELD_ERROR_KEY)) {
      //加上验证黑名单逻辑
      NewsProcess.validateBlacklist(messageNode, blacklistBrdcst, ES_ERROR_INDEX)
      //加上验证白名单逻辑
      NewsProcess.validateWhitelist(messageNode, whitelistBrdcst, ES_ERROR_INDEX)
    }
  }

  def produceRecordId(messageNode: ObjectNode, error_index: String, data_id_field_name: String): String = {
    val url = messageNode.get(data_id_field_name).textValue()
    var record_id: String = ""
    if (url == null || url.trim.length == 0) { //url会出现为空的情况
      NewsProcess.addErrorInfo(messageNode, error_index, "url为空！")
      record_id = MD5Utils.md5(LocalDateTime.now().toString)
    } else {
      record_id = MD5Utils.md5(url)
      if (messageNode.has(FIELD_ERROR_KEY)) {
        record_id = s"${LocalDate.now()}_$record_id"
      }
    }
    record_id
  }

  // 常规验证逻辑
  def common_validate(messageNode: ObjectNode, index: String, index_error: String, jedis: JedisCluster,
                      tags_rds_flg_Broadcast: Broadcast[Int], article_min_words: Int, topic: String): Unit = {
    messageNode.remove("body")
    //将tags字段由数组类型转变为字符串类型 ！inveno不需要这个字段
    NewsProcess.processTags(messageNode)
    val content_node = NewsProcess.getFieldNode(messageNode, "parsed_content")
    val contenttext_node = NewsProcess.getFieldNode(messageNode, "parsed_content_main_body")
    val s_img_lc_node = NewsProcess.getFieldNode(messageNode, "small_img_location")
    val img_lc_node = NewsProcess.getFieldNode(messageNode, "img_location")
    var publish_time_node = NewsProcess.getFieldNode(messageNode, "publish_time")
    val timestamp_node = NewsProcess.getFieldNode(messageNode, "timestamp")
    val article_genre = messageNode.get("article_genre").textValue()

    if (timestamp_node == null) {
      val now_time = NewsProcess.getNowTime()
      messageNode.put("timestamp", now_time)
      NewsProcess.addErrorInfo(messageNode, index_error, "缺少timestamp字段！")
    }

    if (publish_time_node == null) {
      val publish_time_nd = messageNode.get("publish_time")
      println(s"### publish_time 为空 publish_time=$publish_time_node nd=${publish_time_nd.textValue()} ####")
      messageNode.set("publish_time", messageNode.get("timestamp"))
    }

    //设置小说标题
    NewsProcess.setNovelTitle(messageNode, article_genre)

    val timestamp = messageNode.get("timestamp").textValue()
    //按照当前月生成动态索引
    NewsProcess.buildEsIndexOnMonth(messageNode, index, timestamp)

    val publish_time = messageNode.get("publish_time").textValue()
    if (!NewsProcess.chargeTimeField(messageNode, timestamp, publish_time, index_error, article_genre)) return
    //验证必须含有的字段
    val error_msg = JsonNodeUtils.validateData(messageNode, Schemas.base_schema)
    if (error_msg.trim.length > 0) {
      NewsProcess.addErrorInfo(messageNode, index_error, error_msg)
    }
    //验证复合类型字段
    Schemas.based_arraytype_fields.foreach(field => {
      if (!messageNode.has(field)) {
        NewsProcess.addErrorInfo(messageNode, index_error, "field $field is needed ")
      }
    })

    NewsProcess.imgCommonTrans(messageNode, s_img_lc_node, "small_img_location", article_genre)
    NewsProcess.imgCommonTrans(messageNode, img_lc_node, "img_location", article_genre)
    NewsProcess.videoTrans(messageNode, "video_location", article_genre, index_error)

    if (article_genre.startsWith("article")) { //验证article和article_video
      //验证文章和文章视频
      NewsProcess.validate_article(messageNode, index_error, content_node, contenttext_node)
    }
  }
}
