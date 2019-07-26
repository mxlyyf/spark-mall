package com.mxl.sparkmall.common.util

import com.mxl.sparkmall.common.bean.StartupLog
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.{ElasticsearchVersion, HttpClientConfig}
import io.searchbox.core.{Bulk, Count, CountResult, DocumentResult, Index}

object ESUtil {
  private var factory: JestClientFactory = _

  /**
    * 构建客户端工厂对象
    */
  def buildFactory(): Unit = {
    val config: HttpClientConfig = new HttpClientConfig.Builder("http://192.168.213.101:9200")
      .multiThreaded(true)
      .connTimeout(10000)
      .readTimeout(10000)
      .maxTotalConnection(30)
      .build()
    factory = new JestClientFactory()
    factory.setHttpClientConfig(config)
  }

  /**
    * 获取客户端对象
    *
    * @return
    */
  def getJest() = {
    if (factory == null) buildFactory()
    val jest = factory.getObject
    jest
  }

  /**
    * 关闭客户端
    *
    * @param
    */
  def closeJest(jestClient: JestClient) = {
    if (jestClient != null) {
      try {
        jestClient.shutdownClient()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  //批量插入
  def insertBulk(index: String, sources: Iterable[Any]) = {
    val builder = new Bulk.Builder()
      .defaultIndex(index)
      .defaultType("_doc")

    sources.foreach(any => {
      builder.addAction(new Index.Builder(any).build())
    })
    getJest().execute(builder.build())
  }

  // 插入单条数据
  def insertOne(indexName: String, source: Any) = {
    val index: Index = new Index.Builder(source)
      .`type`("_doc")
      .index(indexName)
      //.id()
      .build()
    getJest().execute(index)
  }

  //计数
  def count(index: String) = {
    val count: Count = new Count.Builder()
      .addIndex(index)
      .addType("_doc")
      .build
    getJest().execute(count).getCount
  }

}
