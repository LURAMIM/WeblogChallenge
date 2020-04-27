
package com.paytm.webanalysis

import com.paytm.webanalysis.extractors.DefaultExtractorService
import com.paytm.webanalysis.loaders.DefaultLoaderService
import com.paytm.webanalysis.transformers.DefaultTransformerService
import com.paytm.webanalysis.utils.Utils
import org.apache.spark.internal.Logging


/**
 * AnalysisDriver is a driver for Weblog Analysis Application
 * It reads data from the given log, transforms the data as per the business need using spark-sql
 *    and reports(loader) the results(4 aggregations) to the user
 * @author: Arulkumar Lingan
 */
object AnalysisDriver extends App with Logging {


  if (!(args.length == 1)) {
    logInfo("Invalid Syntax. File path expected[for now]. Syntax may change in future...")
    System.exit(100)
  }
  logInfo("WeblogChallenges starts here...")
  Utils.init()

  // Gets the list of ETL configurations, list of extractor/transformer/loader
  val etlConfig = Utils.getETLConfig()

  // Iterate through the extractors, execute them and load DataFrame cache
  val extractorConfigs = Utils.getExtractorConfig()
  logInfo("Extractor Config -> " + extractorConfigs + ", Size -> " + extractorConfigs.size)
  val extractorService = new DefaultExtractorService
  etlConfig.extractorList.get.map(key => {
    logInfo("Running Extractor -> " + extractorConfigs.get(key))
    extractorService.extractor(extractorConfigs.get(key).get)
  })

  // Iterate through the transformers, execute them and load DataFrame cache
  val transformerConfigs = Utils.getTransformerConfig()
  logInfo("Transformer Config -> " + transformerConfigs + ", Size -> " + transformerConfigs.size)
  val transformerService = new DefaultTransformerService
  etlConfig.transformerList.get.map(key => {
    logInfo("Running Transformer -> " + transformerConfigs.get(key))
    transformerService.transformer(transformerConfigs.get(key).get)
  })

  // Iterate through the loaders and execute them
  val loaderConfigs = Utils.getLoaderConfig()
  logInfo("Loader Config -> " + loaderConfigs + ", Size -> " + loaderConfigs.size)
  val loaderService = new DefaultLoaderService
  etlConfig.loaderList.get.map(key => {
    logInfo("Running Loader -> " + loaderConfigs.get(key))
    loaderService.loader(loaderConfigs.get(key).get)
  })

  logInfo("WeblogChallenges completed successfully")

}