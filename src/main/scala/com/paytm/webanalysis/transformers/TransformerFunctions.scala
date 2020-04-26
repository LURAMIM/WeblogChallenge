package com.paytm.webanalysis.transformers

import com.paytm.webanalysis.config.TransformerConfig
import com.paytm.webanalysis.constants.Constants
import com.paytm.webanalysis.utils.Utils
import org.apache.spark.sql.functions.split

trait TransformerFunctions {

  val sessionDFUpdater = (config: TransformerConfig) => {
    val dataFrame = Utils.getDataFrame(config.sourceDataFrame.get)
    dataFrame.withColumn("visitorIP", split(dataFrame("visitorIP_Port"), ":")(0))
      .withColumn("justURL", split(dataFrame("hitURL"), " ")(1))
  }

  val queryExecutor = (config: TransformerConfig) => {
     Utils.getSparkSession().sql(config.query.get)
  }

  def getTransformer(config: TransformerConfig)={
    config.transformerType match {
      case Constants.SESSION_TRANSFORMER => sessionDFUpdater
      case Constants.QUERY_EXECUTOR => queryExecutor
    }
  }

}
