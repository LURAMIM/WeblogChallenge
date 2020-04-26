package com.paytm.webanalysis.extractors

import com.paytm.webanalysis.config.ExtractorConfig
import com.paytm.webanalysis.constants.Constants
import com.paytm.webanalysis.utils.Utils

trait ExtractorFunctions {

  val csvExtractor = (config: ExtractorConfig) => {
      val spark = Utils.getSparkSession()
      val properties = config.properties.get
      val keys = properties.keySet
      var dfReader = spark.read
      keys.map(key => dfReader = dfReader.option(key, properties.get(key).get))
      var rawDF = dfReader.csv(config.filePath.get)
      if(config.columnNames.isDefined){
        rawDF = rawDF.toDF(config.columnNames.get:_*)
      }
    rawDF
  }

  def getExtractor(config: ExtractorConfig)={
    config.extractorType match {
      case Constants.CSV => csvExtractor
    }
  }

}
