package com.mxl.sparkmall.api.service

import io.searchbox.client.JestClient
import io.searchbox.core.{Search, SearchResult}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import scala.collection.immutable.HashMap

@Service
class DAUServiceImpl extends DAUService {
  @Autowired
  private var jestClient: JestClient = _

  val es_index_dau = "sparkmall_dau"

  override def dauCount(date: String): Long = {
    val query =
      s"""
         |{
         |  "query": {
         |    "bool": {
         |      "filter": {
         |        "term": {
         |          "logDate": "$date"
         |        }
         |      }
         |    }
         |  }
         |}
        """.stripMargin

    val search: Search = new Search.Builder(query)
      .addIndex(es_index_dau)
      .addType("_doc")
      .build()

    val searchResult: SearchResult = jestClient.execute(search)

    searchResult.getTotal.toLong
  }

  override def dauCountPerHour(date: String): Map[String, Long] = {
    val query =
      s"""
         |{
         |  "query": {
         |    "bool": {
         |      "filter": {
         |        "term": {
         |          "logDate": "$date"
         |        }
         |      }
         |    }
         |  }
         |  , "aggs": {
         |    "groupby_hour": {
         |      "terms": {
         |        "field": "logHour",
         |        "size": 24
         |      }
         |    }
         |  }
         |}
        """.stripMargin

    val search: Search = new Search.Builder(query)
      .addIndex(es_index_dau)
      .addType("_doc")
      .build()

    val searchResult: SearchResult = jestClient.execute(search)

    new HashMap[String,Long]
  }
}
