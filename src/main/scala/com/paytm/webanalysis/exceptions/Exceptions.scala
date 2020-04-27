package com.paytm.webanalysis.exceptions

case class SparkContextException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
case class DataFrameNotFoundException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
