package com.paytm.webanalysis.transformers

import com.paytm.webanalysis.config.TransformerConfig
import com.paytm.webanalysis.constants.Constants
import com.paytm.webanalysis.utils.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.split


/**
 * TransformerFunctions holds the different functions needed for the transformation
 * Any number of transformation can be added here,
 */
trait TransformerFunctions extends Logging {

  /**
   * sessionDFUpdater holds the function that updates the given session DataFrame as per the need.
   * To the specific, to adds 2 more columns visitorIP and justURL, from the existing data.
   */
  val sessionDFUpdater = (config: TransformerConfig) => {
    logInfo("sessionDFUpdater running for -> " + config)
    val dataFrame = Utils.getDataFrame(config.sourceDataFrame.get)
    dataFrame.withColumn("visitorIP", split(dataFrame("visitorIP_Port"), ":")(0))
      .withColumn("justURL", split(dataFrame("hitURL"), " ")(1))
  }

  /**
   * queryExecutor holds the function that executes the given query.
   */
  val queryExecutor = (config: TransformerConfig) => {
    logInfo("queryExecutor is running for -> " + config)
    Utils.getSparkSession().sql(config.query.get)
  }


  /**
   * getTransformer gets the transformer as per the type in the configuration and executes
   * @param config
   * @return
   */
  def getTransformer(config: TransformerConfig) = {
    logInfo("ALA: Transformer Type -> " + config.transformerType)
    config.transformerType match {
      case Constants.SESSION_TRANSFORMER => sessionDFUpdater
      case Constants.QUERY_EXECUTOR => queryExecutor
    }
  }

}