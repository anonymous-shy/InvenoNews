package com.donews.streaming

case class TagsInfo(title_tags: String, content_tags: String, title_content_tags: String)

case class NewsInfo(cat1: String, cat2: String, news_score: String, shieldkeywords: String, areaname: String)

object Schemas {

  val test_schema = "{\"type\": \"object\", \"$schema\": \"http://json-schema.org/draft-04/schema\", \"properties\":{\"nihao\":{\"type\":\"string\"}}}"

  val based_arraytype_fields: Array[String] = Array[String]("video_location", "img_location", "small_img_location", "tags")

  val base_schema = "  \"$schema\": \"http://json-schema.org/draft-04/schema\", \"type\":\"object\", \"properties\":{  \"data_source_type\": {  \"type\": \"string\" }, \"data_source_key\": {\"type\": \"string\" }, \"channel\": {\"type\": \"string\" }, \"sub_channel\": {\"type\": \"string\" }, \"data_source_sub_id\": {\"type\": \"string\" }, \"data_source_id\": {\"type\": \"string\" }, \"media\": {\"type\": \"string\" }, \"crawlid\": {\"type\": \"string\" }, \"appid\": {\"type\": \"string\" }, \"id\": {\"type\": \"string\" }, \"timestamp\": {\"type\": \"string\" }, \"url\": {\"type\": \"string\" },\"url_domain\": {\"type\": \"string\" },\"response_url\": {\"type\": \"string\" },\"status_code\": {\"type\": \"string\" },\"title\": {\"type\": \"string\" },\"desc\": {\"type\": \"string\" },\"publish_time\": {\"type\": \"string\" },\"author\": {\"type\": \"string\" },\"info_source\": {\"type\": \"string\" },  \"video_location_count\": {\"type\": \"integer\" },\"img_location_count\": {\"type\": \"integer\" },\"small_img_location_count\": {\"type\": \"integer\" }, \"parsed_content\": {\"type\": \"string\" }, \"parsed_content_main_body\": {\"type\": \"string\" }, \"parsed_content_char_count\": {\"type\": \"integer\" }, \"like_count\": {\"type\": \"integer\" }, \"click_count\": {\"type\": \"integer\" }, \"comment_count\": {\"type\": \"integer\" }, \"repost_count\": {\"type\": \"integer\" }, \"authorized\": {\"type\": \"string\" },\"article_genre\": {\"type\": \"string\" },\"info_source_url\": {\"type\": \"string\" },\"toutiao_out_url\": {\"type\": \"string\" },\"toutiao_refer_url\": {\"type\": \"string\" },\"toutiao_category_class_id\": {\"type\": \"string\" },\"toutiao_category_class\": {\"type\": \"string\" }  },  \"required\":[\"data_source_type\",\"data_source_key\",\"channel\",\"sub_channel\",\"data_source_sub_id\",\"data_source_id\", \"media\",\"crawlid\",\"appid\",\"id\",\"timestamp\",\"url\",\"url_domain\",\"response_url\",\"status_code\",\"title\",\"desc\",\"body\", \"publish_time\",\"author\",\"info_source\", \"img_location_count\", \"small_img_location_count\",\"parsed_content\", \"parsed_content_main_body\",\"parsed_content_char_count\", \"like_count\",\"click_count\",\"comment_count\",\"repost_count\", \"authorized\",\"article_genre\",\"info_source_url\",\"toutiao_out_url\", \"toutiao_refer_url\",\"toutiao_category_class_id\", \"toutiao_category_class\",\"video_location_count\"]}"

}
