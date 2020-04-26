package com.paytm.webanalysis.loaders

import com.paytm.webanalysis.config.LoaderConfig

trait LoaderService {
    def loader(config: LoaderConfig)
}

final class DefaultLoaderService extends LoaderService with LoaderFunctions {
  override def loader(config: LoaderConfig): Unit = {
    getLoader(config)(config)
  }
}
