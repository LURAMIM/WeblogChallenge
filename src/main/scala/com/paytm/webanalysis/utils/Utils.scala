package com.paytm.webanalysis.utils

import com.paytm.webanalysis.config._
import com.paytm.webanalysis.exceptions.{DataFrameNotFoundException, SparkContextException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.error.ConfigReaderFailures

import scala.collection.mutable

/**
 * Utils holds all required utility methods/variables for the application
 * @author: Arulkumar Lingan
 */
object Utils extends Logging {

  private var spark: Option[SparkSession] = None
  private var dataFrameCache = new mutable.HashMap[String, DataFrame]()
  private val etlConfig: Either[ConfigReaderFailures, Config] = pureconfig.loadConfig[Config]

  /**
   * init method initializes the spark session to use throughout the application
   */
  def init(): Unit = {
    logInfo("Utils' Init method invoked")
    spark = Some(SparkSession.builder().appName("WeblogChallenger").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate())

  }


  /**
   * getSparkSession gets the sparksession if defined else throws an exception
   * @return SparkSession
   */
  def getSparkSession(): SparkSession = {
    logInfo("Utils' getSparkSession method invoked")
    if (spark.isDefined) {
      spark.get
    } else {
      throw SparkContextException("SparkSession should be initialized")
    }
  }

  /**
   * addDatFrame adds passed dataframe to the Cache
   * @param dfName
   * @param dataFrame
   * @return Unit
   */
  def addDataFrame(dfName: String, dataFrame: DataFrame) = {
    logInfo("Utils' addDataFrame method invoked..." + dfName)
    dataFrameCache.put(dfName, dataFrame)
    dataFrame.createOrReplaceTempView(dfName)
  }

  /**
   * getDataFrame returns a dataframe of the given key if found else null
   * @param dfName
   * @return DataFrame
   */
  def getDataFrame(dfName: String): DataFrame = {
    logInfo("Utils' getDataFrame method invoked")
    if (dataFrameCache.contains(dfName)) {
      dataFrameCache.get(dfName).get
    } else {
      throw DataFrameNotFoundException("DataFrame is not in Cache. CHeck your code, to load the cache with your DataFrame")
    }

  }

  /**
   * getETLConfig returns complete ETL configurations
   * @return ETLConfig
   */
  def getETLConfig(): EtlConfig = {
    logInfo("Utils' getETLConfig method invoked")
    etlConfig.right.get.etlConfig
  }

  /**
   * getExtractorConfig returns extractor's configuration
   * @return ExtractorConfig
   */
  def getExtractorConfig(): Map[String, ExtractorConfig] = {
    logInfo("Utils' getExtractorConfig method invoked")
    etlConfig.right.get.extractors
  }

  /**
   * getTransformerConfig returns transformer configurations
   * @return TransformerConfig
   */
  def getTransformerConfig(): Map[String, TransformerConfig] = {
    logInfo("Utils' getTransformerConfig method invoked")
    etlConfig.right.get.transformers
  }

  /**
   * getLoaderConfig returns Loader configurations
   * @return LoaderConfig
   */
  def getLoaderConfig(): Map[String, LoaderConfig] = {
    logInfo("Utils' getLoaderConfig method invoked")
    etlConfig.right.get.loaders
  }

}