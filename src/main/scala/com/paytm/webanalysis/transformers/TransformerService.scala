package com.paytm.webanalysis.transformers

import com.paytm.webanalysis.config.TransformerConfig
import com.paytm.webanalysis.utils.Utils
import org.apache.spark.sql.DataFrame

trait TransformerService {
    def transformer(transformerConfig: TransformerConfig)
}

final class DefaultTransformerService extends TransformerService with TransformerFunctions{
  override def transformer(transformerConfig: TransformerConfig) = {
    Utils.addDataFrame(transformerConfig.targetDataFrameName.get, getTransformer(transformerConfig)(transformerConfig))
  }
}
