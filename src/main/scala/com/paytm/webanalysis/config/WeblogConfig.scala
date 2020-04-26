package com.paytm.webanalysis.config
case class Config(extractors: Map[String, ExtractorConfig], transformers: Map[String, TransformerConfig], loaders: Map[String, LoaderConfig])
case class ExtractorConfig( extractorType: String, dataFrameName:String, filePath: Option[String], properties: Option[Map[String, String]], columnNames: Option[List[String]])
case class TransformerConfig(transformerType: String, sourceDataFrame: Option[String], query: Option[String], targetDataFrameName: Option[String])
case class LoaderConfig(loaderType: String, dataFrameName: String, filePath:Option[String])

