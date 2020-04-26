package com.paytm.webanalysis.loaders

import com.paytm.webanalysis.config.LoaderConfig
import com.paytm.webanalysis.constants.Constants
import com.paytm.webanalysis.utils.Utils

trait LoaderFunctions {

  val showFunction = (config: LoaderConfig) => {Utils.getDataFrame(config.dataFrameName).show(false)}

  val csvLoader = (config: LoaderConfig) => {Utils.getDataFrame(config.dataFrameName).write.csv(config.filePath.get)}

  def getLoader(config: LoaderConfig) = {
    config.loaderType match {
      case Constants.CSV => csvLoader
      case Constants.SHOW => showFunction
    }
  }

}
