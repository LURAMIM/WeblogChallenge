package com.paytm.webanalysis.transformers

import com.paytm.webanalysis.config.TransformerConfig
import com.paytm.webanalysis.utils.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

/**
 * TransformerService trait is a contract for all Transformers
 * @author: Arulkumar Lingan
 */
trait TransformerService {
  def transformer(transformerConfig: TransformerConfig): DataFrame
}


/**
 * DefaultTransformerService is a default implementation of Transformer contract
 */
final class DefaultTransformerService extends TransformerService with TransformerFunctions with Logging {
  /**
   * transformer is a implementation of contract method. It gets the specific transformer(s) and executes
   * @param transformerConfig
   * @return DataFrame
   */
  override def transformer(transformerConfig: TransformerConfig): DataFrame = {
    logInfo("Transformer is triggered for -> " + transformerConfig)
    val dataFrame = getTransformer(transformerConfig)(transformerConfig)
    Utils.addDataFrame(transformerConfig.targetDataFrameName.get, dataFrame)
    dataFrame
  }
}
