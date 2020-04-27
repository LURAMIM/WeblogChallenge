package com.paytm.webanalysis.config

/**
 * @author: Arulkumar Lingan
 * Config to hold complete ETL configurations
 */
case class Config(etlConfig: EtlConfig,
                  extractors: Map[String, ExtractorConfig],
                  transformers: Map[String, TransformerConfig],
                  loaders: Map[String, LoaderConfig])

/**
 * ETLConfig to hold list of extractors/transformers/loaders, for your run.
 * This can be wrapped up with another config and can use to run multiple/different ETLs with same code base
 */
case class EtlConfig(extractorList: Option[List[String]],
                     transformerList: Option[List[String]],
                     loaderList: Option[List[String]])

/**
 * ExtractorConfig holds configurations needed to read the data.
 * More configuration can be added when we grow with different types of extractors like Parquet, Tables and etc
 */
case class ExtractorConfig(extractorType: String,
                           dataFrameName: String,
                           filePath: Option[String],
                           properties: Option[Map[String, String]],
                           columnNames: Option[List[String]])

/**
 *  TransformerConfig holds configuration needed for the transformation as per the need.
 *  We can add more configuration as needed
 */
case class TransformerConfig(transformerType: String,
                             sourceDataFrame: Option[String],
                             query: Option[String],
                             targetDataFrameName: Option[String])

/**
 * LoaderConfig holds configuration needed for the loaders/writers.
 * We can added more configuration as needed.
 */
case class LoaderConfig(loaderType: String,
                        dataFrameName: String,
                        filePath: Option[String])