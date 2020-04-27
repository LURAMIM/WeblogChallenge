package com.paytm.webanalysis

import com.paytm.webanalysis.config.Config
import pureconfig.error.ConfigReaderFailures

object ConfigValidator extends App {

  try {
    val simpleConfig: Either[ConfigReaderFailures, Config] = pureconfig.loadConfig[Config]

    simpleConfig match {
      case Left(ex) => ex.toList.foreach(println)

      case Right(config) =>
        println("ETL Config => " + config.etlConfig)
        println("Extractors => " + config.extractors)
        println("Transformers => " + config.transformers)
        println("Loaders => " + config.loaders)
    }
  } catch {
    case exception: Exception => {
      exception.printStackTrace()
    }
    case error: Error => {
      error.printStackTrace()
    }
  }


}

