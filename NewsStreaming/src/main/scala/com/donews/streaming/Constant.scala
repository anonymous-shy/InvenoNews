package com.donews.streaming

import scala.collection.immutable.HashMap

object Constant {
  val GNEWS_TYPE_OF_ES6: String = "_doc"
  val FIELD_TYPE_STR: String = "str"
  val FIELD_TYPE_INT: String = "int"
  val ES_INDEX_KEY: String = "index_of_es"
  val ES_ID_KEY: String = "record_id"
  val FIELD_ERROR_KEY: String = "error_message"
  final val LOCAL_TIME_ZONE = 8
  final val LONGTEXT_LENGTH: Double = Math.pow(2, 32) - 1
  final val MEDIUMTEXT_LENGTH: Double = Math.pow(2, 24) - 1
  final val TEXT_LENGTH: Double = Math.pow(2, 16) - 1
  final val TAGS_KEY_ARR_NUM = 10000
  final val RDS_SIMI_TAGS_PREFIX: String = "GNEWS_SIMI_TAGS"
  final val TAGS_CLASS_TITLE_CONTENT = "title_content"
  final val TAGS_CLASS_TITLE = "title"
  final val TAGS_CLASS_CONTENT = "content"

  final val NEWS_TAGS_URL: String = "http://101.200.185.42/tags/gettags"

  final val TEXT_CLASS_URL: String = "http://101.200.185.42/class/text/"
  final val IMAGE_CLASS_URL: String = "http://101.200.185.42/class/image/"
  final val VIDEO_CLASS_URL: String = "http://101.200.185.42/class/video/"
  final val NEWS_INFO_URL: String = "http://10.44.153.64/general/getResults"
  final val ARTICLE_GENRE = HashMap[String, Int]("article" -> 101, "gallery" -> 102, "video" -> 103)
}
