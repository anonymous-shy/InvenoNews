package com.donews.streaming

import java.time.LocalDateTime
import java.util

import com.donews.streaming.Constant._
import com.donews.utils._
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.JedisCluster


object NewsProcess {
  val replaceOldFields = new java.util.ArrayList[String](util.Arrays.asList(
    "data_source_id", "parsed_content_main_body", "media",
    "id", "publish_time", "newsmode", "url", "article_genre",
    "like_count", "data_source_sub_id", "sub_channel", "_id",
    "repost_count", "parser_info", "proxy_para", "parsed_content_char_count",
    "dbkey", "frequency", "crawl_type_id", "toutiao_category_class",
    "task_priority", "para_info", "js_para", "authorized",
    "reviewmaf", "reviewscore", "task_run_state_time", "crawlid",
    "toutiao_out_url", "inveno_keywords", "reviewsummary", "saved_data_location",
    "post_request", "proxy", "status_code", "batch_id", "cookiejar", "update_time",
    "audio_location", "parse_function", "dbkey_all", "toutiao_category_class_id",
    "timestamp", "dont_filter", "comment_count", "click_count",
    "toutiao_refer_url", "audio_location_count", "data_source_type",
    "kafka_offset", "save_location", "original_url", "appid", "url_domain", "is_banned"))

  def getNowTime: String = {
    var now_time = LocalDateTime.now().toString
    if (now_time.length >= 20) {
      now_time = now_time.substring(0, 19)
    }
    now_time
  }

  def getNowMinuteNo: Int = {
    getNowTime.substring(14, 16).toInt
  }

  def buildEsIndexOnMonth(messageNode: ObjectNode, index: String, timestamp: String): Unit = {
    val y_month = timestamp.substring(0, 7)
    val es_index = s"${index}_$y_month"
    messageNode.put(ES_INDEX_KEY, es_index)
  }

  def getFieldNode(messageNode: ObjectNode, obj_type_field: String): JsonNode = {
    val field_node = messageNode.get(obj_type_field)
    if (null == field_node || field_node.getNodeType.equals(JsonNodeType.NULL)) null else field_node
  }

  def getStrField(messageNode: ObjectNode, fieldName: String): String = {
    val field_node = getFieldNode(messageNode, fieldName)
    var field_value: String = ""
    if (field_node != null) {
      field_value = field_node.textValue()
    }
    field_value
  }

  def addErrorInfo(messageNode: ObjectNode, error_index: String, error_msg: String): Unit = {
    var error_message: String = error_msg
    messageNode.put(Constant.ES_INDEX_KEY, error_index)
    if (messageNode.hasNonNull(Constant.FIELD_ERROR_KEY)) {
      val last_error_msg = messageNode.get(Constant.FIELD_ERROR_KEY).textValue()
      error_message += " || " + last_error_msg
    }
    messageNode.put(Constant.FIELD_ERROR_KEY, error_message)
  }

  /**
   * 替换字段
   * TODO inveno重新定义字段名称
   */
  def renameFields(messageNode: ObjectNode) {
    // inveno field 转换
    messageNode.set("link", messageNode.get("response_url"))
    messageNode.set("content", messageNode.get("parsed_content"))
    messageNode.set("contenttext", messageNode.get("parsed_content_main_body"))
    //    messageNode.set("pubDate", messageNode.get("publish_time").textValue().replace("T",""))
    messageNode.put("pubDate", messageNode.get("publish_time").textValue().replace("T", " "))
    //    messageNode.set("ctime", messageNode.get("timestamp"))
    messageNode.put("ctime", messageNode.get("timestamp").textValue().replace("T", " "))
    messageNode.set("mode", messageNode.get("article_genre"))
    messageNode.set("source", messageNode.get("channel"))
    messageNode.set("desc", messageNode.get("description"))
    messageNode.set("sourcesubname", messageNode.get("sub_channel"))
    messageNode.set("datasourcetype", messageNode.get("media"))
    messageNode.set("datasourcesubid", messageNode.get("data_source_sub_id"))
    messageNode.set("datasourceid", messageNode.get("data_source_id"))
    //**********************************************************************/
    //    messageNode.set("contenttext", messageNode.get("parsed_content_main_body"))
    //    messageNode.set("source", messageNode.get("media"))
    //    messageNode.set("sourceurl", messageNode.get("id"))
    //    messageNode.set("imglists", messageNode.get("img_location"))
    //    messageNode.set("videolists", messageNode.get("video_location"))
    //替换换url的工作应适配于所有数据，包括正确，错误,已经单独提出去了
    //    messageNode.set("shareurl", messageNode.get("url"))
    //    val videotime_node = getFieldNode(messageNode, "videotime")
    //    if (videotime_node == null) {
    //      messageNode.put("videotime", 0) //videotime
    //    }
    //删掉老字段
    messageNode.remove(replaceOldFields)
  }

  /**
   * 确保有publish_time字段,如果没有，就用timestamp补足
   */
  def supplyPublishTime(messageNode: ObjectNode, publish_time_node: JsonNode): Unit = {
    if (null == publish_time_node) {
      messageNode.set("publish_time", messageNode.get("timestamp"))
    }
  }

  /**
   * 确保有publish_time字段
   */
  def validateTimeStamp(messageNode: ObjectNode, index_error: String): Boolean = {
    var is_valid = true
    if (!messageNode.hasNonNull("timestamp")) {
      addErrorInfo(messageNode, index_error, "timestamp mustn't be null")
      is_valid = false
    }
    is_valid
  }

  /**
   * 验证timestamp,publish_time两个字段,以及比较他们的值
   */
  def chargeTimeField(messageNode: ObjectNode, timestamp: String, publish_time: String, index_error: String, article_genre: String): Boolean = {
    var is_valid: Boolean = true
    val regex = "[1-2]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d"
    var n_publish_time = publish_time
    if (timestamp == null || publish_time == null) {
      n_publish_time = timestamp
    }
    val time_fields = Array[String](timestamp, n_publish_time)

    for (i <- time_fields.indices) {
      var time_field_name = "timestamp"
      if (i == 1) {
        time_field_name = "publish_time"
      }
      val time_field = time_fields(i)
      if (!time_field.matches(regex)) {
        addErrorInfo(messageNode, index_error, s"the field $time_field_name is Invalid! " +
          s"$time_field_name:$time_field line 166")
        is_valid = false
      } else {
        messageNode.put(time_field_name, time_field.trim.replace(" ", "T"))
      }
    }

    if (n_publish_time > timestamp) { //publish_time不能大于timestamp
      val datasourceid_node = getFieldNode(messageNode, "data_source_id")
      if (!article_genre.equals("stock") //article_genre为stock，并且datasourceid为YLZX-DFCF-8的放过此验证
        || datasourceid_node == null
        || !datasourceid_node.textValue().equals("YLZX-DFCF-8")) {
        addErrorInfo(messageNode, index_error, "Error_Data: publish_time is Later than timstamp!!!")
        is_valid = false
      }

    }
    is_valid
  }

  /**
   * 验证文章正文
   */
  def validate_article(messageNode: ObjectNode, error_index: String, contenttext_node: JsonNode): Unit = {
    val ctnt_count_node = getFieldNode(messageNode, "parsed_content_char_count")
    var ct_count: Int = 0
    if (null != ctnt_count_node) {
      ct_count = ctnt_count_node.toString.toInt
    }
    val title = messageNode.get("title").toString
    // 剔除乱码数据
    if (title != null && contenttext_node != null && ct_count >= 150) {
      //乱码校验
      val dirtyCode = !GNewsUtil.checkArticleCode(title.toString, contenttext_node.textValue())
      if (dirtyCode) {
        addErrorInfo(messageNode, error_index, "文章乱码了，不可用！")
      }
    }
  }

  def processTitle(messageNode: ObjectNode): Unit = {
    val title_node = getFieldNode(messageNode, "title")
    if (title_node != null) {
      var title = title_node.textValue().trim
      messageNode.put("title", title)
      // add by liudinghua at 2019-01-18
      //由于weibo的标题和正文内容是一样的，所以标题很可能超出100，因此需要对其做特殊处理
      //weibo 标题如果长度超过100，截取前一百长度字符串作为其内容
      val newsmd_node = getFieldNode(messageNode, "newsmode")
      if (newsmd_node != null) {
        val newsmode = newsmd_node.intValue()
        if ((newsmode == -2 || newsmode == -3) && title.length >= 100) {
          title = title.substring(0, 98)
          messageNode.put("title", title)
        }
      }
    }
  }

  def validate_fields_length(messageNode: ObjectNode, error_index: String): Unit = {
    validate_field_len(messageNode, "shareurl", error_index, 330)
    processTitle(messageNode) //去除title两端的空格
    validate_field_len(messageNode, "title", error_index, 100)
    validate_field_len(messageNode, "keywords", error_index, 500)
    validate_field_len(messageNode, "author", error_index, 100)
    validate_field_len(messageNode, "contenttext", error_index, Constant.LONGTEXT_LENGTH)
    validate_field_len(messageNode, "content", error_index, Constant.MEDIUMTEXT_LENGTH)
  }

  def validate_field_len(messageNode: ObjectNode, field_name: String, error_index: String, max_len: Double): Unit = {
    val field_length = getFieldLength(messageNode, field_name)
    if (field_length > max_len) {
      addErrorInfo(messageNode, error_index, s"$field_name 字段长度超出 $max_len 字节最大限制！")
    }
  }

  def getFieldLength(messageNode: ObjectNode, field_name: String): Int = {
    val node = getFieldNode(messageNode, field_name)
    var length: Int = 0
    if (null != node) {
      val field_value = node.textValue()
      length = field_value.length
    }
    length
  }

  def imgCommonTrans(messageNode: ObjectNode, img_location: JsonNode, img_field: String, genre: String): Unit = {
    if (img_location != null) {
      val new_list_nodes = new ArrayNode(JsonNodeFactory.instance)
      val img_list_nodes = img_location.asInstanceOf[ArrayNode].elements()
      var index: Int = 0 //只截取100张
      while (img_list_nodes.hasNext && index < 100) {
        val img_node = img_list_nodes.next().asInstanceOf[ObjectNode]
        // TODO 不需要转换字段！！！
        // processImgNode(img_node)
        img_node.remove("bucket_name")
        if (0 == index) {
          val filepath = getFieldNode(img_node, "filepath")
          if (filepath != null && filepath.textValue().toLowerCase.endsWith("webp")) {
            //图片后缀是webp将data value设置成7
            messageNode.put("datavalid", 7)
          }
        }
        new_list_nodes.add(img_node)
        index = index + 1
      }
      //处理imgcount字段
      if (genre.equals("gallery") && img_field.equals("img_location")) {
        messageNode.put("img_location_count", new_list_nodes.size())
      } else if (!genre.equals("video") && !genre.equals("gallery") && img_field.equals("small_img_location")) { //video类型数据不做处理
        messageNode.put("small_img_location_count", new_list_nodes.size())
      }
      if (index > 0)
        messageNode.set(img_field, new_list_nodes)
      else
        messageNode.set(img_field, null)
    } else messageNode.set(img_field, null)
  }

  def videoTrans(messageNode: ObjectNode, video_field: String, genre: String, error_index: String): Unit = {
    val video_location = NewsProcess.getFieldNode(messageNode, video_field)
    if (video_location == null) {
      if ("video".equals(genre)) {
        addErrorInfo(messageNode, error_index, "video类型的数据，视频列表为空")
      }
      messageNode.set(video_field, null)
    } else {
      val vd_list_nodes = video_location.asInstanceOf[ArrayNode].elements()
      var index = 0
      while (vd_list_nodes.hasNext) {
        val vd_node = vd_list_nodes.next().asInstanceOf[ObjectNode]
        if (0 == index && !genre.equals("gallery")) { //gallery类型数据不做处理
          //如果有多个视频，把第一个视频的属性设置为此数据的视频属性
          if (!vd_node.hasNonNull("video_duration")) {
            println(s"视频缺乏视频时长字段 str=> ${messageNode.toString}")
            addErrorInfo(messageNode, error_index, "视频时长字段video_location为null ！")
          } else {
            val duration_str = vd_node.findValue("video_duration").textValue()
            val duration = GNewsUtil.videoTime2Sec(duration_str)
            messageNode.put("videotime", duration)
            if (duration < 5) {
              addErrorInfo(messageNode, error_index, "视频长度小于5秒！")
            }
          }
        }
        // processVideoNode(vd_node)
        vd_node.remove("bucket_name")
        index = index + 1
      }
      if (index == 0) {
        messageNode.set(video_field, null)
      }
    }
  }

  /**
   *
   */
  def processImgNode(img_node: ObjectNode): Unit = {
    img_node.remove("bucket_name")
    //    processImgVideoSubNode(img_node, "img_path", "filepath", Constant.FIELD_TYPE_STR)
    //    processImgVideoSubNode(img_node, "img_src", "shareurl", Constant.FIELD_TYPE_STR)
    //    processImgVideoSubNode(img_node, "img_desc", "title", Constant.FIELD_TYPE_STR)
    //    processImgVideoSubNode(img_node, "img_width", "width", Constant.FIELD_TYPE_INT)
    //    processImgVideoSubNode(img_node, "img_height", "height", Constant.FIELD_TYPE_INT)
  }

  /**
   *
   */
  def processVideoNode(video_node: ObjectNode): Unit = {
    video_node.remove("bucket_name")
    //    processImgVideoSubNode(video_node, "video_desc", "content", Constant.FIELD_TYPE_STR)
    //    processImgVideoSubNode(video_node, "video_src", "shareurl", Constant.FIELD_TYPE_STR)
    //    processImgVideoSubNode(video_node, "video_path", "filepath", Constant.FIELD_TYPE_STR)
    //    processImgVideoSubNode(video_node, "video_duration", "duration", Constant.FIELD_TYPE_STR)
    //    processImgVideoSubNode(video_node, "video_width", "videowidth", Constant.FIELD_TYPE_STR)
    //    processImgVideoSubNode(video_node, "video_height", "videoheight", Constant.FIELD_TYPE_STR)
  }

  def validateBlacklist(messageNode: ObjectNode,
                        blacklistBrdcst: Broadcast[Array[String]]
                        , index_error: String): Unit = {
    val blacklist: Array[String] = blacklistBrdcst.value
    for (black_info <- blacklist) {
      val infos = black_info.split("___")
      val field_name = infos(0)
      val relation = infos(1)
      val keyword = infos(2)
      val field_type = infos(3)
      if (messageNode.hasNonNull(field_name)) {
        if ("contains".equals(relation) && "str".equals(field_type)) { //包含内容
          if (messageNode.get(field_name).textValue().contains(keyword)) {
            addErrorInfo(messageNode, index_error, s"$field_name 字段包含 $keyword 内容")
          }
        }
      }
    }
  }

  def validateWhitelist(messageNode: ObjectNode,
                        whitelistBrdcst: Broadcast[Array[String]]
                        , index_error: String): Unit = {
    val whitelist: Array[String] = whitelistBrdcst.value
    if (messageNode.hasNonNull("saved_data_location")) {
      val saved_data_location = messageNode.get("saved_data_location").textValue()
      if (!whitelist.contains(saved_data_location)) {
        addErrorInfo(messageNode, index_error, "save_data_location 值不在白名单内")
      }
    } else {
      addErrorInfo(messageNode, index_error, "缺乏 saved_data_location 字段或者其值为空")
    }
  }

  /**
   * add by shenhuayu at 2019-03-06 加上授权状态和评分字段
   */
  def addGrantLevelInfo(jedisCluster: JedisCluster, messageNode: ObjectNode): Unit = {
    val info_source_id_prefix = getStrField(messageNode, "info_source_id_prefix")
    val info_source_prefix = getStrField(messageNode, "info_source_prefix")
    val data_source_unique_id = getStrField(messageNode, "data_source_unique_id")
    val idGroup = s"$info_source_id_prefix@$info_source_prefix@$data_source_unique_id"
    val v = jedisCluster.hget("H_news", idGroup)
    val score = if (v != null) v else "-1@-1"
    val Array(grant, level) = score.split("@")
    messageNode.put("grant", grant)
    //    messageNode.put("level", level)
  }

  private def checkContainsWordsGrp(text: String, words_grp: String): Boolean = {
    val words_arr = words_grp.split("#")
    var sensitive: Boolean = true //设置初始状态，使得可以进入while循环
    val iter = words_arr.iterator
    //判定text敏感条件：必须包含words_arr中所有关键词，一旦找到text其中一个关键词，则判定text为不敏感
    while (iter.hasNext && sensitive) {
      sensitive = text.contains(iter.next())
    }
    sensitive
  }

  def addSensetiveInfo(messageNode: ObjectNode, senseWords: Array[String]): Unit = {
    val iter = senseWords.iterator
    var sense_word_get: Boolean = false
    val title = messageNode.get("title").asText("")
    val content = messageNode.get("content").asText("")
    val sense_node = new ObjectNode(JsonNodeFactory.instance)

    while (iter.hasNext && !sense_word_get) {
      val words = iter.next()
      val title_sense = checkContainsWordsGrp(title, words)
      var content_sense: Boolean = false //默认内容不包含敏感词，并且按需查询，提升效率
      if (title_sense) {
        sense_node.put("title", words)
        messageNode.put("datavalid", 8)
      } else {
        //        content_sense = content.contains(word)
        content_sense = checkContainsWordsGrp(content, words)
        if (content_sense) {
          sense_node.put("body", words)
          messageNode.put("datavalid", 9)
        }
      }
      sense_word_get = title_sense || content_sense
    }
    messageNode.put("contenttext", sense_node.toString)
  }

  def processImgVideoSubNode(img_video_node: ObjectNode, old_field: String, new_field: String, field_value_type: String): Unit = {
    val sub_field_node = ensureExistSubNode(img_video_node, old_field)
    if (sub_field_node == null) {
      if (field_value_type.equals(Constant.FIELD_TYPE_STR))
        img_video_node.put(new_field, "")
      else img_video_node.put(new_field, 0)
    } else
      img_video_node.set(new_field, sub_field_node)
    img_video_node.remove(old_field) //删除原有的老的字字段
  }

  def ensureExistSubNode(node: ObjectNode, sub_field: String): JsonNode = {
    val sub_node = node.get(sub_field)
    if (null == sub_node || JsonNodeType.NULL.equals(sub_node.getNodeType)) null else sub_node
  }

  // 注释内容**********************************************************************************************************//

  /**
   * inveno开头的字符串都加入option字段中
   */
  /*def processInvenoOptionFields(messageNode: ObjectNode): Unit = {
    val option_node = new ObjectNode(JsonNodeFactory.instance)
    val fieldNames = messageNode.fieldNames()
    val remove_fields = new ArrayBuffer[String]
    while (fieldNames.hasNext) {
      val fieldName = fieldNames.next()
      //inveno开头的字符串都加入option字段中
      if (fieldName.startsWith("inveno_")) {
        option_node.set(fieldName, messageNode.get(fieldName))
        remove_fields.append(fieldName)
      }
    }
    //季家震需求
    //add by liudh at 2020-03-10 将video_location等字段，拷贝到inveno_info中
    //后续可能还会加字段，为了尽量不改动代码，将其做成配置
    val copy_fields_str = MysqlUtils.getProperties().getProperty("inveno_copy_fields")
    val copy_fields = copy_fields_str.split(",")
    copy_fields.foreach { field =>
      if (messageNode.has(field)) {
        option_node.put(field, messageNode.get(field).toString)
      }
    }

    messageNode.put("invenoinfo", option_node.toString)
    messageNode.remove(remove_fields)
  }*/

  /*private def getArrayNode(messageNode: ObjectNode, fieldName: String): ArrayNode = {
    val field_node = messageNode.get(fieldName)
    if (null == field_node || field_node.getNodeType.equals(JsonNodeType.NULL)) null else field_node.asInstanceOf[ArrayNode]
  }*/

  /*def getCountTypeField(messageNode: ObjectNode, count_field_name: String): Int = {
    val count_node = messageNode.get(count_field_name)
    var count = 0
    if (null != count_node && !count_node.getNodeType.equals(JsonNodeType.NULL)) {
      count = count_node.toString.toInt
    }
    count
  }*/

  /*def validate_video(messageNode: ObjectNode, error_index: String): Unit = {
    val vd_count_node = messageNode.get("video_location_count")
    var vd_count = 0
    if (!vd_count_node.getNodeType.equals(JsonNodeType.NULL)) {
      vd_count = vd_count_node.toString.toInt
    }
  }*/

  /*def getTagsIndex(tags: String): Int = {
    val first_tag = tags.split(":")(0)
    Math.abs(first_tag.hashCode) % TAGS_KEY_ARR_NUM
  }*/

  /*  def produceKeywords(messageNode: ObjectNode): Unit = {
    if (!messageNode.has("keywords")) {
      val tags_nd = getFieldNode(messageNode, "tags")
      if (tags_nd != null && !tags_nd.textValue().trim.equals("")) {
        val news_tags = tags_nd.textValue().trim.replaceAll(":0\\.\\d+| +", "")
        messageNode.put("keywords", news_tags)
      } else {
        messageNode.put("keywords", "")
      }
    }
  }*/

  /*def getQuchongValue: String = {
    val quchong = MysqlUtils.getProperties().getProperty("quchong")
    quchong
  }*/

  /*def querryOldTags(sc: SparkContext, options: Map[String, String], tags_rds_flg_Broadcast: Broadcast[Int]): Broadcast[Map[String, AList[String]]] = {
    val nowDateTime: LocalDateTime = LocalDateTime.now
    //获取两天内 article tags
    val now = nowDateTime.toString.substring(0, 19)
    val twoDaysAgo = nowDateTime.minusDays(2).toString.substring(0, 19)
    val yearMonth = nowDateTime.toString.substring(0, 7)
    val read_index: String = "gnews_raw_data_full"

    val query2days =
      s"""
         |{
         | "query":{
         |    "constant_score":{
         |      "filter":{
         |        "range":{
         |          "timestamp":{
         |            "gte":"$twoDaysAgo",
         |            "lt":"$now"
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
        """.stripMargin


    val tagsRdd = EsSpark.esRDD(sc, s"$read_index/article", query2days, options).repartition(8).map { news =>
      var tags = news._2.get("tags") match {
        case Some(v) => if (v == None) null else v.asInstanceOf[String]
        case None => null
      }
      var channel = news._2.get("channel") match {
        case Some(v) => if (v == None) "kong" else v.asInstanceOf[String]
        case None => "kong"
      }


      val data_index = news._1
      if (null != tags) tags = s"$tags--$data_index"
      (channel, tags)
    }.filter(_._2 != null)


    val tags_map = tagsRdd.aggregateByKey(new AList[String]())(seqOp = (tagsList, tagsStr) => {

      tagsList.add(tagsStr)
      tagsList
    },
      combOp = (tagsList1, tagsList2) => {
        tagsList1.addAll(tagsList2)
        tagsList1
      }).collect().toMap


    val keys_iter = tags_map.keySet.iterator
    val tags_map_broadCast = sc.broadcast[Map[String, AList[String]]](tags_map)
    tags_map_broadCast
  }*/

  /*private def get_similar_data_days(): Int = {
    val days_str = MysqlUtils.getProperties().getProperty("similar_data_days")
    days_str.toInt
  }*/

  /*def nquerryOldTags(sc: SparkContext, options: Map[String, String], tags_rds_flg_Broadcast: Broadcast[Int]): Unit = {
    val nowDateTime: LocalDateTime = LocalDateTime.now
    val days = get_similar_data_days()
    //获取两天内 article tags
    val now = nowDateTime.toString.substring(0, 19)
    val twoDaysAgo = nowDateTime.minusDays(days).toString.substring(0, 19)
    val yearMonth = nowDateTime.toString.substring(0, 7)
    val read_index: String = "gnews_raw_data_full"
    val query2days =
      s"""
         |{
         | "query":{
         |    "constant_score":{
         |      "filter":{
         |        "range":{
         |          "timestamp":{
         |            "gte":"$twoDaysAgo",
         |            "lt":"$now"
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
        """.stripMargin
    val tagsRdd = EsSpark.esRDD(sc, s"$read_index/article", query2days, options)
      .repartition(8).map { news =>
      var tags = news._2.get("tags") match {
        case Some(v) => if (v == None) null else v.asInstanceOf[String]
        case None => null
      }
      // 10000
      var tags_index: Int = TAGS_KEY_ARR_NUM
      val data_index = news._1
      if (null != tags && tags.trim.length > 1) {
        tags_index = getTagsIndex(tags)
        tags = s"$tags--$data_index"
      }
      (tags_index, tags)
    }.filter(_._2 != null)

    val redis_flag = tags_rds_flg_Broadcast.value
    tagsRdd.aggregateByKey(new AList[String]())(seqOp = (tagsList, tagsStr) => {
      tagsList.add(tagsStr)
      tagsList
    },
      combOp = (tagsList1, tagsList2) => {
        tagsList1.addAll(tagsList2)
        tagsList1
      }).foreachPartition { tags_datas =>
      val jedis = RedisClusterHelper.getConnection
      tags_datas.foreach { data =>
        val tags_index = data._1
        val redis_key = s"$RDS_SIMI_TAGS_PREFIX:$tags_index:$redis_flag"
        jedis.lpush(redis_key, data._2.asScala: _*)
        jedis.expire(redis_key, 15 * 60) //key的有效时间15分钟
      }
    }
  }*/

  /*def nvalidate_similary(messageNode: ObjectNode, extractTags: String, error_index: String, jedis: JedisCluster, tags_rds_flg_Broadcast: Broadcast[Int]): Unit = {
  val tags_rds_flag = tags_rds_flg_Broadcast.value
  val tags_index = getTagsIndex(extractTags)
  val redis_key = s"$RDS_SIMI_TAGS_PREFIX:$tags_index:$tags_rds_flag"
  if (jedis.exists(redis_key)) {
    val tags_list = jedis.lrange(redis_key, 0, -1)
    val tags_iter = tags_list.iterator()
    var need_iter: Boolean = true
    while (need_iter && tags_iter.hasNext) {
      val old_tag_and_index_str = tags_iter.next()
      val values_arr = old_tag_and_index_str.split("--")
      val old_tag = values_arr(0)
      val data_index = values_arr(1)
      val similarScore = GNewsUtil.similarity(old_tag, extractTags)
      val score_threshold = getScoreThreshold
      if (similarScore >= score_threshold) {
        addErrorInfo(messageNode, error_index, s"文章与 _id=$data_index 的数据相似度> $score_threshold")
        //找到过去一条相似的即可
        need_iter = false
      }
    }
  }
}*/

  /*private def getScoreThreshold: Float = {
    val score_str = MysqlUtils.getProperties().getProperty("similar_score")
    score_str.toFloat
  }*/

}
