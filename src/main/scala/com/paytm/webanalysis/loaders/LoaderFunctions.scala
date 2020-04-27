package com.paytm.webanalysis.loaders

import com.paytm.webanalysis.config.LoaderConfig
import com.paytm.webanalysis.constants.Constants
import com.paytm.webanalysis.utils.Utils
import org.apache.spark.internal.Logging

/**
 * LoaderFunctions used to define different types of loader needed for ETL
 * @author: Arulkumar Lingan
 *
 */
trait LoaderFunctions extends Logging {

  /**
   * showFunction variable is used to carry the function that shows default(20) number of records for the
   * given loader config
   */
  val showFunction = (config: LoaderConfig) => {
    logInfo("SHOW Loader is running for -> " + config)
    Utils.getDataFrame(config.dataFrameName).show(false)
  }

  /**
   * csvLoader holds the function that helps to write the data of the given config/DataFrame, to csv files
   */
  val csvLoader = (config: LoaderConfig) => {
    logInfo("CSV Loader is running for -> " + config)
    Utils.getDataFrame(config.dataFrameName).write.csv(config.filePath.get)
  }


  /**
   * getLoader method is used to get the specific type of loader as per the configuration and execute it
   * @param config
   * @return Unit
   */
  def getLoader(config: LoaderConfig) = {
    logInfo("ALA: Loader Type " + config.loaderType)
    config.loaderType match {
      case Constants.CSV => csvLoader
      case Constants.SHOW => showFunction
    }
  }

}