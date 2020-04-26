package com.paytm.webanalysis.extractors

import com.paytm.webanalysis.config.ExtractorConfig
import com.paytm.webanalysis.utils.Utils
import org.apache.spark.sql.DataFrame

trait ExtractorService {
  def extractor(config: ExtractorConfig)
}

final class DefaultExtractorService extends ExtractorService with ExtractorFunctions {
  override def extractor(config: ExtractorConfig) = {
    Utils.addDataFrame(config.dataFrameName, getExtractor(config)(config))
  }
}