package com.paytm.webanalysis.extractors

import com.paytm.webanalysis.config.ExtractorConfig
import com.paytm.webanalysis.utils.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

/**
 * @author: Arulkumar Lingan
 * ExtractorService is a contract for extractor
 */
trait ExtractorService {
  def extractor(config: ExtractorConfig): DataFrame
}

/**
 * @author: Arulkumar Lingan
 * DefaultExtractorService used as a default extractor service implementation
 */
final class DefaultExtractorService extends ExtractorService with ExtractorFunctions with Logging {
  override def extractor(config: ExtractorConfig) = {
    logInfo("Running extractor for -> " + config)
    val dataFrame = getExtractor(config)(config)
    Utils.addDataFrame(config.dataFrameName, dataFrame)
    dataFrame
  }
}