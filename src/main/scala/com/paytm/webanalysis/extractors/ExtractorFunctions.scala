package com.paytm.webanalysis.extractors

import com.paytm.webanalysis.config.ExtractorConfig
import com.paytm.webanalysis.constants.Constants
import com.paytm.webanalysis.utils.Utils
import org.apache.spark.internal.Logging


/**
 * @author: Arulkumar Lingan
 *  ExtractorFunctions used to define functions needed to extract data from
 *  different form of sources
 */
trait ExtractorFunctions extends Logging {

  /**
   * csvExtractor variable holds the function that helps to read CSV file
   */
  val csvExtractor = (config: ExtractorConfig) => {
    logInfo("CSV Extractor running for " + config)
    val spark = Utils.getSparkSession()
    val properties = config.properties.get
    val keys = properties.keySet
    var dfReader = spark.read
    keys.map(key => dfReader = dfReader.option(key, properties.get(key).get))
    var rawDF = dfReader.csv(config.filePath.get)
    if (config.columnNames.isDefined) {
      rawDF = rawDF.toDF(config.columnNames.get: _*)
    }
    rawDF
  }

  /**
   * getExtractor gets the specific extractor based on the type of source data and executes
   * @param config
   * @return DataFrame
   */
  def getExtractor(config: ExtractorConfig) = {
    logInfo("ALA: Extractor type -> " + config.extractorType)
    config.extractorType match {
      case Constants.CSV => csvExtractor
    }
  }

}