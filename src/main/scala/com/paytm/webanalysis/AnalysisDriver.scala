package com.paytm.webanalysis

import com.paytm.webanalysis.config.ExtractorConfig
import com.paytm.webanalysis.extractors.{DefaultExtractorService, ExtractorService}
import com.paytm.webanalysis.loaders.DefaultLoaderService
import com.paytm.webanalysis.transformers.DefaultTransformerService
import com.paytm.webanalysis.utils.Utils

object AnalysisDriver extends App {
  if(!(args.length==1)){
    println("Invalid Syntax. File path expected[for now]. Syntax may change in future...")
    System.exit(100)
  }
  print("WeblogChallenges starts here...")
  Utils.init()

  val extractorConfigs = Utils.getExtractorConfig()
  val extractorService = new DefaultExtractorService
  extractorConfigs.mapValues(config => extractorService.getExtractor(config))

  val transformerConfigs = Utils.getTransformerConfig()
  val transformerService = new DefaultTransformerService
  transformerConfigs.mapValues(config => transformerService.getTransformer(config))

  val loaderConfigs = Utils.getLoaderConfig()
  val loaderService = new DefaultLoaderService
  loaderConfigs.mapValues(config => loaderService.getLoader(config))



}
