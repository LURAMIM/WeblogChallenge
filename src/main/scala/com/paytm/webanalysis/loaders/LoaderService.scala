package com.paytm.webanalysis.loaders

import com.paytm.webanalysis.config.LoaderConfig
import org.apache.spark.internal.Logging

/**
 * LoaderService is a contract for loader. This can be extended as per the requirement
 * @author: Arulkumar Lingan
 */
trait LoaderService {
  def loader(config: LoaderConfig)
}

/**
 * DefaultLoaderService with default implementation of LoaderService
 */
final class DefaultLoaderService extends LoaderService with LoaderFunctions with Logging {
  /**
   * loader gets the specific loader as per the need and executes it
   * @param config
   * @return Unit
   */
  override def loader(config: LoaderConfig): Unit = {
    logInfo("Loader is initiated for " + config)
    getLoader(config)(config)
  }
}