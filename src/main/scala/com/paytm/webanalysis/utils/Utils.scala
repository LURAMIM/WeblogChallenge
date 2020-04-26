package com.paytm.webanalysis.utils

import com.paytm.webanalysis.config.{Config, ExtractorConfig, LoaderConfig, TransformerConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.error.ConfigReaderFailures

import scala.collection.mutable

object Utils {

  private var spark: Option[SparkSession] = None
  private var dataFrameCache = new mutable.HashMap[String, DataFrame]()
  private val etlConfig: Either[ConfigReaderFailures, Config] = pureconfig.loadConfig[Config]

  def init(): Unit ={
    spark = Some(SparkSession.builder().appName("WeblogChallenger").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate())

  }

  def getSparkSession():SparkSession = {
    if(spark.isDefined){
      spark.get
    }else{
      throw new Exception("SparkSession should be initialized")
    }
  }

  def addDataFrame(dfName: String, dataFrame: DataFrame)= {
    dataFrameCache.put(dfName, dataFrame)
    dataFrame.createOrReplaceTempView(dfName)
  }

  def getDataFrame(dfName: String): DataFrame ={
    if(dataFrameCache.contains(dfName)){
      dataFrameCache.get(dfName).get
    } else {
      null
    }

  }

  def getExtractorConfig(): Map[String, ExtractorConfig] = {
    etlConfig.right.get.extractors
  }

  def getTransformerConfig(): Map[String, TransformerConfig] = {
    etlConfig.right.get.transformers
  }

  def getLoaderConfig(): Map[String, LoaderConfig] = {
    etlConfig.right.get.loaders
  }

}
